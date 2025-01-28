use std::{thread::sleep, time::Duration, u64};

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
use tracing_subscriber::filter::FromEnvError;

#[derive(Debug, Error)]
pub enum WorkloadErr {
    #[error("failed to initialize tracing subscriber")]
    TracingInit,

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

impl From<FromEnvError> for WorkloadErr {
    fn from(_: FromEnvError) -> Self {
        WorkloadErr::TracingInit
    }
}

impl From<tracing_subscriber::util::TryInitError> for WorkloadErr {
    fn from(_: tracing_subscriber::util::TryInitError) -> Self {
        WorkloadErr::TracingInit
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(tag = "type")]
pub enum Workload {
    Writer { vid: VolumeId, interval_ms: u64 },
    Reader { vid: VolumeId, interval_ms: u64 },
}

impl Workload {
    pub fn run<F: Fetcher>(
        self,
        worker_id: &str,
        handle: Runtime<F>,
        rng: impl Rng,
        ticks: usize,
    ) -> Result<(), Culprit<WorkloadErr>> {
        match self {
            Workload::Writer { vid, interval_ms: interval } => workload_writer(
                worker_id,
                handle,
                rng,
                vid,
                Duration::from_millis(interval),
                ticks,
            ),
            Workload::Reader { vid, interval_ms: interval } => workload_reader(
                worker_id,
                handle,
                rng,
                vid,
                ticks,
                Duration::from_millis(interval),
            ),
        }
    }
}

fn load_tracker<F: Fetcher>(
    handle: &VolumeHandle<F>,
    worker_id: &str,
) -> Result<PageTracker, Culprit<WorkloadErr>> {
    // load the page tracker from the volume, if the volume is empty this will
    // initialize a new page tracker
    let reader = handle.reader().or_into_ctx()?;
    tracing::info!("loading page tracker at snapshot {:?}", reader.snapshot());
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

    tracing::info!("loaded page tracker with {} pages", page_tracker.len());

    Ok(page_tracker)
}

/// This workload continuously writes and reads pages to a volume, verifying the
/// contents of pages are always correct
fn workload_writer<F: Fetcher>(
    worker_id: &str,
    runtime: Runtime<F>,
    mut rng: impl Rng,
    vid: VolumeId,
    interval: Duration,
    ticks: usize,
) -> Result<(), Culprit<WorkloadErr>> {
    // open the volume
    let handle = runtime
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Push))
        .or_into_ctx()?;

    // pull the volume explicitly before continuing
    tracing::info!("pulling volume {:?}", vid);
    handle.sync_with_remote(SyncDirection::Pull).or_into_ctx()?;

    let mut page_tracker = load_tracker(&handle, worker_id)?;

    for _ in 0..ticks {
        // check the volume status to see if we need to reset
        let status = handle.status().or_into_ctx()?;
        if status != VolumeStatus::Ok {
            tracing::info!("volume has status {status}, resetting");
            handle.reset_to_remote().or_into_ctx()?;
            tracing::info!("volume reset to {:?}", handle.snapshot().or_into_ctx()?);

            // reload the page tracker from the volume
            page_tracker = load_tracker(&handle, worker_id)?;
        }

        // randomly pick a page offset and a page value.
        // select the next offset to ensure we don't pick the 0th page
        let offset = PageOffset::test_random(&mut rng, 16).next();
        let new_page: Page = rng.gen();
        let existing_hash = page_tracker.upsert(offset, &new_page);

        let reader = handle.reader().or_into_ctx()?;
        tracing::info!(?offset, snapshot=?reader.snapshot(), "writing page");

        // verify the offset is missing or present as expected
        let page = reader.read(offset).or_into_ctx()?;
        if let Some(existing) = existing_hash {
            expect_always_or_unreachable!(
                existing == page,
                "page should have expected contents",
                { "offset": offset, "worker": worker_id, "snapshot": reader.snapshot() }
            );
        } else {
            expect_always_or_unreachable!(
                page.is_empty(),
                "page should be empty as it has never been written to",
                { "offset": offset, "worker": worker_id, "snapshot": reader.snapshot() }
            );
        }

        let mut writer = handle.writer().or_into_ctx()?;

        // write the page to the volume and update the page index
        writer.write(offset, new_page);
        writer.write(0.into(), page_tracker.serialize_into_page().or_into_ctx()?);

        writer.commit().or_into_ctx()?;

        sleep(interval);
    }
    Ok(())
}

fn workload_reader<F: Fetcher>(
    worker_id: &str,
    runtime: Runtime<F>,
    _rng: impl Rng,
    vid: VolumeId,
    ticks: usize,
    interval: Duration,
) -> Result<(), Culprit<WorkloadErr>> {
    let handle = runtime
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Pull))
        .or_into_ctx()?;

    let subscription = handle.subscribe_to_remote_changes();

    let mut last_snapshot = None;
    let mut seen_nonempty = false;

    for _ in 0..ticks {
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
            { "worker": worker_id }
        );

        let reader = handle.reader().or_into_ctx()?;
        let snapshot = reader.snapshot().expect("snapshot missing");

        tracing::info!(snapshot=?snapshot, "received commit for volume {:?}", vid);

        expect_sometimes!(
            last_snapshot
                .replace(snapshot.clone())
                .is_none_or(|last| &last != snapshot),
            "the snapshot is different after receiving a commit notification",
            { "snapshot": snapshot, "worker": worker_id }
        );

        let page_count = snapshot.pages();
        if seen_nonempty {
            expect_always_or_unreachable!(
                page_count > 0,
                "the snapshot should never be empty after we have seen a non-empty snapshot",
                { "snapshot": snapshot, "worker": worker_id }
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

            tracing::info!("reading page at offset {offset}");
            let page = reader.read(offset).or_into_ctx()?;

            if let Some(expected_hash) = page_tracker.get_hash(offset) {
                expect_always_or_unreachable!(
                    expected_hash == page,
                    "page should have expected contents",
                    { "offset": offset, "worker": worker_id }
                );
            } else {
                expect_always_or_unreachable!(
                    page.is_empty(),
                    "page should be empty as it has never been written to",
                    { "offset": offset, "worker": worker_id }
                );
            }
        }
    }
    Ok(())
}
