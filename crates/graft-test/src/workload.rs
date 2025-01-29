use std::{thread::sleep, time::Duration, u64};

use crate::{PageHash, Ticker};

use super::{PageTracker, PageTrackerErr};
use config::ConfigError;
use crossbeam::channel::RecvTimeoutError;
use culprit::{Culprit, ResultExt};
use graft_client::{
    runtime::{
        fetcher::Fetcher,
        runtime::Runtime,
        storage::{
            volume_state::{SyncDirection, VolumeConfig, VolumeStatus},
            StorageErr,
        },
        sync::{ShutdownErr, StartupErr},
        volume::VolumeHandle,
    },
    ClientBuildErr, ClientErr,
};
use graft_core::{page::Page, page_offset::PageOffset, VolumeId};
use graft_server::supervisor;
use precept::{expect_always_or_unreachable, expect_reachable, expect_sometimes};
use rand::Rng;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::field;
use ureq::ErrorKind;

#[derive(Debug, Error)]
pub enum WorkloadErr {
    #[error("invalid workload configuration")]
    InvalidConfig,

    #[error("client error: {0}")]
    ClientErr(#[from] ClientErr),

    #[error("error building graft client: {0}")]
    ClientBuildErr(#[from] ClientBuildErr),

    #[error("sync task startup error: {0}")]
    SyncTaskStartupErr(#[from] StartupErr),

    #[error("sync task shutdown error: {0}")]
    SyncTaskShutdownErr(#[from] ShutdownErr),

    #[error("page tracker error: {0}")]
    PageTrackerErr(#[from] PageTrackerErr),

    #[error("storage error: {0}")]
    StorageErr(#[from] StorageErr),

    #[error("supervisor shutdown error: {0}")]
    SupervisorShutdownErr(#[from] supervisor::ShutdownErr),
}

impl From<ConfigError> for WorkloadErr {
    fn from(_: ConfigError) -> Self {
        WorkloadErr::InvalidConfig
    }
}

impl WorkloadErr {
    fn should_retry(&self) -> bool {
        match self {
            WorkloadErr::ClientErr(ClientErr::HttpErr(kind)) => match kind {
                ErrorKind::Dns => true,
                ErrorKind::ConnectionFailed => true,
                ErrorKind::TooManyRedirects => true,
                _ => false,
            },
            WorkloadErr::StorageErr(StorageErr::ConcurrentWrite) => true,
            _ => false,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type")]
pub enum Workload {
    Writer { vid: VolumeId, interval_ms: u64 },
    Reader { vid: VolumeId, interval_ms: u64 },
}

struct WorkloadEnv<F: Fetcher, R: Rng> {
    worker_id: &'static str,
    runtime: Runtime<F>,
    rng: R,
    ticker: Ticker,
}

impl Workload {
    pub fn run<F: Fetcher, R: Rng>(
        self,
        worker_id: &'static str,
        runtime: Runtime<F>,
        rng: R,
        ticker: Ticker,
    ) -> Result<(), Culprit<WorkloadErr>> {
        let mut env = WorkloadEnv { worker_id, runtime, rng, ticker };

        while env.ticker.tick() {
            if let Err(err) = match self {
                Workload::Writer { ref vid, interval_ms: interval } => {
                    workload_writer(&mut env, vid, Duration::from_millis(interval))
                }
                Workload::Reader { ref vid, interval_ms: interval } => {
                    workload_reader(&mut env, vid, Duration::from_millis(interval))
                }
            } {
                if err.ctx().should_retry() {
                    tracing::warn!("retrying workload after error: {:?}", err);
                    precept::expect_reachable!("retryable error occurred");
                    continue;
                } else {
                    return Err(err);
                }
            }
        }
        Ok(())
    }
}

fn load_tracker<F: Fetcher>(
    handle: &VolumeHandle<F>,
    worker_id: &str,
) -> Result<PageTracker, Culprit<WorkloadErr>> {
    // load the page tracker from the volume, if the volume is empty this will
    // initialize a new page tracker
    let reader = handle.reader().or_into_ctx()?;
    let first_page = reader.read(0.into()).or_into_ctx()?;
    let page_tracker = PageTracker::deserialize_from_page(&first_page).or_into_ctx()?;

    // ensure the page tracker is only empty when we expect it to be
    expect_always_or_unreachable!(
        page_tracker.is_empty() ^ reader.snapshot().is_some(),
        "page tracker should only be empty when the snapshot is missing",
        {
            "snapshot": reader.snapshot(),
            "tracker_len": page_tracker.len(),
            "worker": worker_id
        }
    );

    Ok(page_tracker)
}

/// This workload continuously writes and reads pages to a volume, verifying the
/// contents of pages are always correct
fn workload_writer<F: Fetcher, R: Rng>(
    env: &mut WorkloadEnv<F, R>,
    vid: &VolumeId,
    interval: Duration,
) -> Result<(), Culprit<WorkloadErr>> {
    let handle = env
        .runtime
        .open_volume(vid, VolumeConfig::new(SyncDirection::Both))
        .or_into_ctx()?;

    // pull the volume explicitly to ensure we are up to date
    tracing::info!("pulling volume {:?}", vid);
    handle.sync_with_remote(SyncDirection::Pull).or_into_ctx()?;

    let mut page_tracker = load_tracker(&handle, env.worker_id).or_into_ctx()?;

    while env.ticker.tick() {
        // check the volume status to see if we need to reset
        let status = handle.status().or_into_ctx()?;
        if status != VolumeStatus::Ok {
            let span =
                tracing::info_span!("volume_reset", ?status, result = field::Empty).entered();
            handle.reset_to_remote().or_into_ctx()?;
            span.record("result", format!("{:?}", handle.snapshot().or_into_ctx()?));
            drop(span);

            // reload the page tracker from the volume
            page_tracker = load_tracker(&handle, env.worker_id).or_into_ctx()?;
        }

        // check that the in-memory is in sync with storage
        let pt = load_tracker(&handle, env.worker_id).or_into_ctx()?;
        let diff = page_tracker.diff(&pt);
        expect_always_or_unreachable!(
            diff.is_empty(),
            "page tracker should be in sync with storage",
            {
                "worker": env.worker_id,
                "snapshot": handle.reader().or_into_ctx()?.snapshot(),
                "diff": diff,
            }
        );

        // randomly pick a page offset and a page value.
        // select the next offset to ensure we don't pick the 0th page
        let offset = PageOffset::test_random(&mut env.rng, 16).next();
        let new_page: Page = env.rng.gen();
        let new_hash = PageHash::new(&new_page);
        let expected_hash = page_tracker.upsert(offset, new_hash.clone());

        let reader = handle.reader().or_into_ctx()?;
        let span = tracing::info_span!(
            "write_page",
            offset=offset.to_string(),
            snapshot=?reader.snapshot(),
            ?new_hash
        )
        .entered();

        // verify the offset is missing or present as expected
        let page = reader.read(offset).or_into_ctx()?;
        let actual_hash = PageHash::new(&page);

        if let Some(expected_hash) = expected_hash {
            expect_always_or_unreachable!(
                expected_hash == actual_hash,
                "page should have expected contents",
                {
                    "offset": offset,
                    "worker": env.worker_id,
                    "snapshot": reader.snapshot(),
                    "expected": expected_hash,
                    "actual": actual_hash
                }
            );
        } else {
            expect_always_or_unreachable!(
                page.is_empty(),
                "page should be empty as it has never been written to",
                {
                    "offset": offset,
                    "worker": env.worker_id,
                    "snapshot": reader.snapshot(),
                    "actual": actual_hash
                }
            );
        }

        let mut writer = handle.writer().or_into_ctx()?;

        // write the page to the volume and update the page index
        writer.write(offset, new_page);
        writer.write(0.into(), page_tracker.serialize_into_page().or_into_ctx()?);

        let pre_commit_remote = writer.snapshot().and_then(|s| s.remote());
        let reader = writer.commit().or_into_ctx()?;
        let post_commit_remote = reader.snapshot().and_then(|s| s.remote());

        if pre_commit_remote != post_commit_remote {
            tracing::info!(
                "remote LSN changed concurrently with local commit; from {:?} to {:?}",
                pre_commit_remote,
                post_commit_remote
            );
        }

        drop(span);

        sleep(interval);
    }
    Ok(())
}

fn workload_reader<F: Fetcher, R: Rng>(
    env: &mut WorkloadEnv<F, R>,
    vid: &VolumeId,
    interval: Duration,
) -> Result<(), Culprit<WorkloadErr>> {
    let handle = env
        .runtime
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Pull))
        .or_into_ctx()?;

    // pull the volume explicitly to ensure we are up to date
    tracing::info!("pulling volume {:?}", vid);
    handle.sync_with_remote(SyncDirection::Pull).or_into_ctx()?;

    let subscription = handle.subscribe_to_remote_changes();

    let mut last_snapshot = None;
    let mut seen_nonempty = false;

    while env.ticker.tick() {
        // wait for the next commit
        match subscription.recv_timeout(interval) {
            Ok(()) => (),
            Err(RecvTimeoutError::Timeout) => {
                tracing::info!("timeout while waiting for next commit, looping");
                continue;
            }
            Err(RecvTimeoutError::Disconnected) => panic!("subscription closed"),
        }

        expect_reachable!(
            "reader workload received commit",
            { "worker": env.worker_id }
        );

        let reader = handle.reader().or_into_ctx()?;
        let snapshot = reader.snapshot().expect("snapshot missing");

        tracing::info!(snapshot=?snapshot, "received commit for volume {:?}", vid);

        expect_sometimes!(
            last_snapshot
                .replace(snapshot.clone())
                .is_none_or(|last| &last != snapshot),
            "the snapshot is different after receiving a commit notification",
            { "snapshot": snapshot, "worker": env.worker_id }
        );

        let page_count = snapshot.pages();
        if seen_nonempty {
            expect_always_or_unreachable!(
                page_count > 0,
                "the snapshot should never be empty after we have seen a non-empty snapshot",
                { "snapshot": snapshot, "worker": env.worker_id }
            );
        }

        if page_count == 0 {
            tracing::info!("volume is empty, waiting for the next commit");
            continue;
        } else {
            seen_nonempty = true;
        }

        // load the page index
        let first_page = reader.read(0.into()).or_into_ctx()?;
        let page_tracker = PageTracker::deserialize_from_page(&first_page).or_into_ctx()?;

        // ensure all pages are either empty or have the expected hash
        for offset in snapshot.pages().offsets() {
            if offset == 0 {
                // skip the page tracker
                continue;
            }

            let span = tracing::info_span!("read_page", offset=offset.to_string(), snapshot=?reader.snapshot(), hash=field::Empty).entered();
            let page = reader.read(offset).or_into_ctx()?;
            let actual_hash = PageHash::new(&page);

            if let Some(expected_hash) = page_tracker.get_hash(offset) {
                expect_always_or_unreachable!(
                    expected_hash == &actual_hash,
                    "page should have expected contents",
                    { "offset": offset, "worker": env.worker_id }
                );
            } else {
                expect_always_or_unreachable!(
                    page.is_empty(),
                    "page should be empty as it has never been written to",
                    { "offset": offset, "worker": env.worker_id }
                );
            }

            span.record("hash", actual_hash.to_string());
            drop(span);
        }
    }
    Ok(())
}
