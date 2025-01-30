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
        volume_reader::VolumeReader,
    },
    ClientBuildErr, ClientErr,
};
use graft_core::{gid::ClientId, page::Page, page_offset::PageOffset, VolumeId};
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

    #[error("supervisor shutdown error: {0}")]
    SupervisorShutdownErr(#[from] supervisor::ShutdownErr),
}

impl From<StorageErr> for WorkloadErr {
    fn from(err: StorageErr) -> Self {
        WorkloadErr::ClientErr(ClientErr::StorageErr(err))
    }
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
            WorkloadErr::ClientErr(ClientErr::StorageErr(StorageErr::ConcurrentWrite)) => true,
            WorkloadErr::ClientErr(ClientErr::StorageErr(StorageErr::RemoteConflict)) => true,
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
    cid: ClientId,
    runtime: Runtime<F>,
    rng: R,
    ticker: Ticker,
}

impl Workload {
    pub fn run<F: Fetcher, R: Rng>(
        self,
        cid: ClientId,
        runtime: Runtime<F>,
        rng: R,
        ticker: Ticker,
    ) -> Result<(), Culprit<WorkloadErr>> {
        let mut env = WorkloadEnv { cid, runtime, rng, ticker };

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

fn reset_volume_if_needed<F: Fetcher>(
    handle: &VolumeHandle<F>,
) -> Result<(), Culprit<WorkloadErr>> {
    let status = handle.status().or_into_ctx()?;
    if status != VolumeStatus::Ok {
        precept::expect_reachable!("volume needs reset", { "vid": handle.vid() });
        let span = tracing::info_span!("volume_reset", ?status, result = field::Empty).entered();
        handle.reset_to_remote().or_into_ctx()?;
        span.record("result", format!("{:?}", handle.snapshot().or_into_ctx()?));
    }
    Ok(())
}

fn load_tracker<F: Fetcher>(
    reader: &VolumeReader<F>,
    cid: &ClientId,
) -> Result<PageTracker, Culprit<WorkloadErr>> {
    // load the page tracker from the volume, if the volume is empty this will
    // initialize a new page tracker
    let first_page = reader.read(0.into()).or_into_ctx()?;
    let page_tracker = PageTracker::deserialize_from_page(&first_page).or_into_ctx()?;

    // ensure the page tracker is only empty when we expect it to be
    expect_always_or_unreachable!(
        page_tracker.is_empty() ^ reader.snapshot().is_some(),
        "page tracker should only be empty when the snapshot is missing",
        {
            "snapshot": reader.snapshot(),
            "tracker_len": page_tracker.len(),
            "cid": cid
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

    let status = handle.status().or_into_ctx()?;
    expect_sometimes!(status != VolumeStatus::Ok, "volume is not ok when workload starts", { "vid": vid });

    // check to see if the volume needs recovering from a previous crash
    reset_volume_if_needed(&handle).or_into_ctx()?;

    // pull the volume explicitly to ensure we are up to date
    tracing::info!("pulling volume {:?}", vid);
    handle.sync_with_remote(SyncDirection::Pull).or_into_ctx()?;

    while env.ticker.tick() {
        // check the volume status to see if we need to reset
        reset_volume_if_needed(&handle).or_into_ctx()?;

        // open a reader
        let reader = handle.reader().or_into_ctx()?;

        // randomly pick a page offset and a page value.
        // select the next offset to ensure we don't pick the 0th page
        let offset = PageOffset::test_random(&mut env.rng, 16).next();
        let new_page: Page = env.rng.gen();
        let new_hash = PageHash::new(&new_page);

        let span = tracing::info_span!(
            "write_page",
            offset=offset.to_string(),
            snapshot=?reader.snapshot(),
            ?new_hash
        )
        .entered();

        // load the tracker and the expected page hash
        let mut page_tracker = load_tracker(&reader, &env.cid).or_into_ctx()?;
        let expected_hash = page_tracker.upsert(offset, new_hash.clone());

        // verify the offset is missing or present as expected
        let page = reader.read(offset).or_into_ctx()?;
        let actual_hash = PageHash::new(&page);

        if let Some(expected_hash) = expected_hash {
            expect_always_or_unreachable!(
                expected_hash == actual_hash,
                "page should have expected contents",
                {
                    "offset": offset,
                    "cid": env.cid,
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
                    "cid": env.cid,
                    "snapshot": reader.snapshot(),
                    "actual": actual_hash
                }
            );
        }

        let mut writer = reader.upgrade();

        // write out the updated page tracker and the new page
        writer.write(0.into(), page_tracker.serialize_into_page().or_into_ctx()?);
        writer.write(offset, new_page);

        let pre_commit_remote = writer.snapshot().and_then(|s| s.remote());
        let reader = writer.commit().or_into_ctx()?;
        let post_commit_remote = reader.snapshot().and_then(|s| s.remote());

        expect_sometimes!(
            pre_commit_remote != post_commit_remote,
            "remote LSN changed concurrently with local commit",
            {
                "pre_commit": pre_commit_remote,
                "snapshot": reader.snapshot(),
                "cid": env.cid
            }
        );

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

    // check to see if the volume needs recovering from a previous crash
    // this can happen if this reader used to be a writer
    reset_volume_if_needed(&handle).or_into_ctx()?;

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
            { "cid": env.cid }
        );

        let reader = handle.reader().or_into_ctx()?;
        let snapshot = reader.snapshot().expect("snapshot missing");

        tracing::info!(snapshot=?snapshot, "received commit for volume {:?}", vid);

        expect_sometimes!(
            last_snapshot
                .replace(snapshot.clone())
                .is_none_or(|last| &last != snapshot),
            "the snapshot is different after receiving a commit notification",
            { "snapshot": snapshot, "cid": env.cid }
        );

        let page_count = snapshot.pages();
        if seen_nonempty {
            expect_always_or_unreachable!(
                page_count > 0,
                "the snapshot should never be empty after we have seen a non-empty snapshot",
                { "snapshot": snapshot, "cid": env.cid }
            );
        }

        if page_count == 0 {
            tracing::info!("volume is empty, waiting for the next commit");
            continue;
        } else {
            seen_nonempty = true;
        }

        // load the page index
        let page_tracker = load_tracker(&reader, &env.cid).or_into_ctx()?;

        // ensure all pages are either empty or have the expected hash
        for offset in snapshot.pages().offsets() {
            if offset == 0 {
                // skip the page tracker
                continue;
            }

            let span = tracing::info_span!(
                "read_page",
                offset = offset.to_string(),
                hash = field::Empty
            )
            .entered();

            let page = reader.read(offset).or_into_ctx()?;
            let actual_hash = PageHash::new(&page);
            span.record("hash", actual_hash.to_string());

            if let Some(expected_hash) = page_tracker.get_hash(offset) {
                expect_always_or_unreachable!(
                    expected_hash == &actual_hash,
                    "page should have expected contents",
                    {
                        "offset": offset,
                        "cid": env.cid,
                        "snapshot": snapshot,
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
                        "cid": env.cid,
                        "snapshot": snapshot,
                        "actual": actual_hash
                    }
                );
            }

            drop(span);
        }
    }
    Ok(())
}
