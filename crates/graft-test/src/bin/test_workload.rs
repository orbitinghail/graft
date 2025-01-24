use std::{thread::sleep, time::Duration, u64};

use antithesis_sdk::{antithesis_init, assert_always_or_unreachable};
use clap::Parser;
use config::{Config, ConfigError};
use culprit::{Culprit, ResultExt};
use graft_client::{
    runtime::{
        fetcher::NetFetcher,
        runtime::Runtime,
        storage::{
            volume_state::{SyncDirection, VolumeConfig, VolumeStatus},
            Storage,
        },
        sync::ShutdownErr,
        volume::VolumeHandle,
    },
    ClientBuildErr, ClientBuilder, ClientErr, ClientPair, MetastoreClient, PagestoreClient,
};
use graft_core::{page::Page, page_offset::PageOffset, VolumeId};
use graft_test::{
    running_in_antithesis, test_tracing::tracing_init, worker_id, PageTracker, PageTrackerErr,
};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use tracing_subscriber::filter::FromEnvError;
use url::Url;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Args {
    /// The workload file to run
    workload: String,
}

#[derive(Debug, Error)]
enum WorkloadErr {
    #[error("failed to initialize tracing subscriber")]
    TracingInit,

    #[error("invalid workload configuration")]
    InvalidConfig,

    #[error("client error: {0}")]
    ClientErr(#[from] ClientErr),

    #[error("error building graft client: {0}")]
    ClientBuildErr(#[from] ClientBuildErr),

    #[error("sync task shutdown error: {0}")]
    SyncTaskShutdownErr(#[from] ShutdownErr),

    #[error("page tracker error: {0}")]
    PageTrackerErr(#[from] PageTrackerErr),
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
enum Workload {
    Writer { vid: VolumeId, interval_ms: u64 },
    Reader { vid: VolumeId },
}

impl Workload {
    fn run(
        self,
        worker_id: &str,
        handle: Runtime<NetFetcher>,
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
            Workload::Reader { vid } => workload_reader(worker_id, handle, rng, vid, ticks),
        }
    }
}

fn load_volume(
    runtime: &Runtime<NetFetcher>,
    vid: &VolumeId,
    worker_id: &str,
) -> Result<(VolumeHandle<NetFetcher>, PageTracker), Culprit<WorkloadErr>> {
    // open the volume
    let handle = runtime
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Push))
        .or_into_ctx()?;

    // pull the volume explicitly before continuing
    tracing::info!("pulling volume {:?}", vid);
    handle.sync_with_remote(SyncDirection::Pull).or_into_ctx()?;

    // load the page tracker from the volume, if the volume is empty this will
    // initialize a new page tracker
    let reader = handle.reader().or_into_ctx()?;
    tracing::info!("loading page tracker at snapshot {:?}", reader.snapshot());
    let first_page = reader.read(0.into()).or_into_ctx()?;
    let page_tracker = PageTracker::deserialize_from_page(&first_page).or_into_ctx()?;

    // ensure the page tracker is only empty when we expect it to be
    assert_always_or_unreachable!(
        page_tracker.is_empty() ^ reader.snapshot().is_some(),
        "page tracker should only be empty when the snapshot is missing",
        &json!({
            "snapshot": reader.snapshot(),
            "tracker_len": page_tracker.len(),
            "worker": worker_id
        })
    );

    tracing::info!("loaded page tracker with {} pages", page_tracker.len());

    Ok((handle, page_tracker))
}

/// This workload continuously writes and reads pages to a volume, verifying the
/// contents of pages are always correct
fn workload_writer(
    worker_id: &str,
    runtime: Runtime<NetFetcher>,
    mut rng: impl Rng,
    vid: VolumeId,
    interval: Duration,
    ticks: usize,
) -> Result<(), Culprit<WorkloadErr>> {
    let (handle, mut page_tracker) = load_volume(&runtime, &vid, worker_id)?;

    for _ in 0..ticks {
        // check the volume status to see if we need to reset
        let status = handle.status().or_into_ctx()?;
        if status != VolumeStatus::Ok {
            tracing::warn!("volume has status {status}, resetting");
        }

        // randomly pick a page offset and a page value.
        // select the next offset to ensure we don't pick the 0th page
        let offset = PageOffset::test_random(&mut rng, 16).next();
        let new_page: Page = rng.gen();
        let existing_hash = page_tracker.upsert(offset, &new_page);

        tracing::info!(?offset, "writing page");

        // verify the offset is missing or present as expected
        let reader = handle.reader().or_into_ctx()?;
        let page = reader.read(offset).or_into_ctx()?;
        let details = json!({ "offset": offset, "worker": worker_id });
        if let Some(existing) = existing_hash {
            assert_always_or_unreachable!(
                existing == page,
                "page should have expected contents",
                &details
            );
        } else {
            assert_always_or_unreachable!(
                page.is_empty(),
                "page should be empty as it has never been written to",
                &details
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

fn workload_reader(
    worker_id: &str,
    runtime: Runtime<NetFetcher>,
    _rng: impl Rng,
    vid: VolumeId,
    ticks: usize,
) -> Result<(), Culprit<WorkloadErr>> {
    let handle = runtime
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Pull))
        .or_into_ctx()?;

    let subscription = handle.subscribe_to_remote_changes();

    let mut last_snapshot = None;
    let mut seen_nonempty = false;

    for _ in 0..ticks {
        // wait for the next commit
        subscription.recv().expect("change subscription closed");

        let reader = handle.reader().or_into_ctx()?;
        let snapshot = reader.snapshot().expect("snapshot missing");

        tracing::info!(snapshot=?snapshot, "received commit for volume {:?}", vid);

        antithesis_sdk::assert_always_or_unreachable!(
            last_snapshot
                .replace(snapshot.clone())
                .is_none_or(|last| &last != snapshot),
            "the snapshot should be different after receiving a commit notification",
            &json!({ "snapshot": snapshot, "worker": worker_id })
        );

        let page_count = snapshot.pages();
        if seen_nonempty {
            antithesis_sdk::assert_always_or_unreachable!(
                page_count > 0,
                "the snapshot should never be empty after we have seen a non-empty snapshot",
                &json!({ "snapshot": snapshot, "worker": worker_id })
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
                assert_always_or_unreachable!(
                    expected_hash == page,
                    "page should have expected contents",
                    &json!({ "offset": offset, "worker": worker_id })
                );
            } else {
                assert_always_or_unreachable!(
                    page.is_empty(),
                    "page should be empty as it has never been written to",
                    &json!({ "offset": offset, "worker": worker_id })
                );
            }
        }
    }
    Ok(())
}

fn main() -> Result<(), Culprit<WorkloadErr>> {
    antithesis_init();
    let mut rng = antithesis_sdk::random::AntithesisRng;
    let worker_id = worker_id(&mut rng);
    tracing_init(Some(worker_id.clone()));
    let args = Args::parse();

    let workload: Workload = Config::builder()
        .add_source(config::Environment::with_prefix("WORKLOAD").separator("_"))
        .add_source(config::File::with_name(&args.workload))
        .build()?
        .try_deserialize()?;

    tracing::info!(
        workload_file = args.workload,
        ?workload,
        "STARTING TEST WORKLOAD"
    );

    let metastore_client: MetastoreClient =
        ClientBuilder::new(Url::parse("http://metastore:3001").unwrap())
            .build()
            .or_into_ctx()?;
    let pagestore_client: PagestoreClient =
        ClientBuilder::new(Url::parse("http://pagestore:3000").unwrap())
            .build()
            .or_into_ctx()?;
    let clients = ClientPair::new(metastore_client, pagestore_client);

    let storage = Storage::open_temporary().unwrap();
    let mut runtime = Runtime::new(NetFetcher::new(clients.clone()), storage);
    let sync_task = runtime.start_sync_task(clients, Duration::from_secs(1), 8);

    antithesis_sdk::lifecycle::setup_complete(&json!({ "workload": workload }));

    let (ticks, shutdown_timeout) = if running_in_antithesis() {
        (rng.gen_range(100..5000), Duration::from_secs(3600))
    } else {
        (100, Duration::from_secs(60))
    };

    tracing::info!(?ticks, "running test workload");
    workload
        .run(&worker_id, runtime.clone(), rng, ticks)
        .or_into_ctx()?;

    tracing::info!("workload finished");
    tracing::info!("waiting for sync task to shutdown");
    sync_task.shutdown_timeout(shutdown_timeout).or_into_ctx()?;

    antithesis_sdk::assert_reachable!(
        "test workload finishes",
        &json!({
            "worker": worker_id
        })
    );

    tracing::info!("shutdown complete");

    Ok(())
}
