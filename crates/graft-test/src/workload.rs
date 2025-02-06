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
            snapshot::Snapshot,
            volume_state::{SyncDirection, VolumeConfig, VolumeStatus},
            StorageErr,
        },
        sync::{ShutdownErr, StartupErr},
        volume::VolumeHandle,
        volume_reader::{VolumeRead, VolumeReader},
        volume_writer::VolumeWrite,
    },
    ClientErr,
};
use graft_core::{gid::ClientId, page::Page, page_offset::PageOffset, VolumeId};
use graft_proto::GraftErrCode;
use graft_server::supervisor;
use precept::{expect_always_or_unreachable, expect_reachable, expect_sometimes};
use rand::Rng;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::field;

#[derive(Debug, Error)]
pub enum WorkloadErr {
    #[error("invalid workload configuration")]
    InvalidConfig,

    #[error("client error: {0}")]
    ClientErr(#[from] ClientErr),

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
        fn should_retry_io(err: std::io::ErrorKind) -> bool {
            match err {
                std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::NotConnected
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::NetworkDown
                | std::io::ErrorKind::NetworkUnreachable => true,
                _ => false,
            }
        }

        match self {
            WorkloadErr::ClientErr(ClientErr::GraftErr(err)) => match err.code() {
                GraftErrCode::CommitRejected => true,
                GraftErrCode::SnapshotMissing => true,
                GraftErrCode::ServiceUnavailable => true,
                _ => false,
            },
            WorkloadErr::ClientErr(ClientErr::HttpErr(err)) => match err {
                ureq::Error::ConnectionFailed
                | ureq::Error::HostNotFound
                | ureq::Error::Timeout(_) => true,
                ureq::Error::Decompress(_, ioerr) => should_retry_io(ioerr.kind()),
                ureq::Error::Io(ioerr) => should_retry_io(ioerr.kind()),
                _ => false,
            },
            WorkloadErr::ClientErr(ClientErr::IoErr(err)) => should_retry_io(*err),
            WorkloadErr::ClientErr(ClientErr::StorageErr(err)) => match err {
                StorageErr::ConcurrentWrite | StorageErr::RemoteConflict => true,
                _ => false,
            },
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

fn recover_and_sync_volume<F: Fetcher>(
    handle: &VolumeHandle<F>,
) -> Result<(), Culprit<WorkloadErr>> {
    let vid = handle.vid();
    let status = handle.status().or_into_ctx()?;
    let span = tracing::info_span!(
        "verify_and_pull_volume",
        ?status,
        ?vid,
        result = field::Empty
    )
    .entered();

    match status {
        VolumeStatus::Ok => {
            // retrieve the latest remote snapshot
            handle.sync_with_remote(SyncDirection::Pull).or_into_ctx()?;
        }
        VolumeStatus::RejectedCommit | VolumeStatus::Conflict => {
            precept::expect_reachable!("volume needs reset", {
                "vid": handle.vid(), "status": status
            });
            // reset the volume to the latest remote snapshot
            handle.reset_to_remote().or_into_ctx()?;
        }
        VolumeStatus::InterruptedPush => {
            precept::expect_reachable!("volume has an interrupted push", {
                "vid": handle.vid(), "status": status
            });
            // finish the sync to the remote and then update
            handle.sync_with_remote(SyncDirection::Both).or_into_ctx()?;
        }
    }

    span.record("result", format!("{:?}", handle.snapshot().or_into_ctx()?));

    Ok(())
}

fn load_tracker<F: Fetcher>(
    reader: &VolumeReader<F>,
    cid: &ClientId,
) -> Result<PageTracker, Culprit<WorkloadErr>> {
    let span = tracing::info_span!("load_tracker", snapshot=?reader.snapshot(), hash=field::Empty)
        .entered();

    // load the page tracker from the volume, if the volume is empty this will
    // initialize a new page tracker
    let first_page = reader.read(0).or_into_ctx()?;
    let page_tracker = PageTracker::deserialize_from_page(&first_page).or_into_ctx()?;

    // record the hash of the page tracker for debugging
    span.record("hash", PageHash::new(&first_page).to_string());

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

    // ensure the volume is recovered and synced with the server
    recover_and_sync_volume(&handle).or_into_ctx()?;

    while env.ticker.tick() {
        // check the volume status to see if we need to reset
        let status = handle.status().or_into_ctx()?;
        if status != VolumeStatus::Ok {
            let span = tracing::info_span!("reset_volume", ?status, vid=?handle.vid(), result=field::Empty).entered();
            precept::expect_always!(
                status != VolumeStatus::InterruptedPush,
                "volume needs reset after workload start",
                { "vid": handle.vid(), "status": status }
            );
            // reset the volume to the latest remote snapshot
            handle.reset_to_remote().or_into_ctx()?;
            span.record("result", format!("{:?}", handle.snapshot().or_into_ctx()?));
        }

        // open a reader
        let reader = handle.reader().or_into_ctx()?;

        // randomly pick a page offset and a page value.
        // select the next offset to ensure we don't pick the 0th page
        let offset = PageOffset::test_random(&mut env.rng, 16).next();
        let new_page: Page = env.rng.random();
        let new_hash = PageHash::new(&new_page);

        let span = tracing::info_span!(
            "write_page",
            offset=offset.to_string(),
            snapshot=?reader.snapshot(),
            ?new_hash,
            tracker_hash=field::Empty
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

        // serialize the page tracker with the updated page and output it's hash for debugging
        let tracker_page = page_tracker.serialize_into_page().or_into_ctx()?;
        span.record("tracker_hash", PageHash::new(&tracker_page).to_string());

        // write out the updated page tracker and the new page
        let mut writer = reader.upgrade();
        writer.write(0, tracker_page);
        writer.write(offset, new_page);

        // commit the changes
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

    // ensure the volume is recovered and synced with the server
    recover_and_sync_volume(&handle).or_into_ctx()?;

    let subscription = handle.subscribe_to_remote_changes();

    let mut last_snapshot: Option<Snapshot> = None;
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
