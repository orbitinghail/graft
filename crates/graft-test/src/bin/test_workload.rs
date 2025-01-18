use std::{thread::sleep, time::Duration, u64};

use antithesis_sdk::{antithesis_init, assert_always_or_unreachable};
use clap::Parser;
use config::{Config, ConfigError};
use culprit::{Culprit, ResultExt};
use futures::FutureExt;
use graft_client::{
    runtime::{
        fetcher::NetFetcher,
        runtime::Runtime,
        storage::{
            volume_config::{SyncDirection, VolumeConfig},
            Storage,
        },
        sync::ShutdownErr,
    },
    ClientBuildErr, ClientBuilder, ClientErr, ClientPair, MetastoreClient, PagestoreClient,
};
use graft_core::{page::Page, page_offset::PageOffset, VolumeId};
use graft_test::{PageTracker, PageTrackerErr};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::json;
use thiserror::Error;
use tokio::{select, signal::ctrl_c, sync::broadcast, task::spawn_blocking};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{
    filter::FromEnvError, fmt::format::FmtSpan, util::SubscriberInitExt, EnvFilter,
};
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

    #[error("failed to receive broadcast message")]
    BroadcastRecvErr,

    #[error("client error: {0}")]
    ClientErr(#[from] ClientErr),

    #[error("error building graft client: {0}")]
    ClientBuildErr(#[from] ClientBuildErr),

    #[error("sync task shutdown error: {0}")]
    SyncTaskShutdownErr(#[from] ShutdownErr),

    #[error("page tracker error: {0}")]
    PageTrackerErr(#[from] PageTrackerErr),
}

impl From<broadcast::error::RecvError> for WorkloadErr {
    fn from(_: broadcast::error::RecvError) -> Self {
        WorkloadErr::BroadcastRecvErr
    }
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
    fn start(self, handle: Runtime<NetFetcher>, rng: impl Rng) -> Result<(), Culprit<WorkloadErr>> {
        match self {
            Workload::Writer { vid, interval_ms: interval } => {
                workload_writer(handle, rng, vid, Duration::from_millis(interval))
            }
            Workload::Reader { vid } => workload_reader(handle, rng, vid),
        }
    }
}

/// This workload continuously writes and reads pages to a volume, verifying the
/// contents of pages are always correct
fn workload_writer(
    runtime: Runtime<NetFetcher>,
    mut rng: impl Rng,
    vid: VolumeId,
    interval: Duration,
) -> Result<(), Culprit<WorkloadErr>> {
    let mut page_tracker = PageTracker::default();

    let handle = runtime
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Push))
        .or_into_ctx()?;

    loop {
        // randomly pick a page offset and a page value.
        // select the next offset to ensure we don't pick the 0th page
        let offset = PageOffset::test_random(&mut rng, 16).next();
        let new_page: Page = rng.gen();
        let existing_hash = page_tracker.upsert(offset, &new_page);

        tracing::info!(?offset, "writing page");

        // verify the offset is missing or present as expected
        let reader = handle.reader().or_into_ctx()?;
        let page = reader.read(offset).or_into_ctx()?;
        let details = json!({ "offset": offset });
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
}

fn workload_reader(
    runtime: Runtime<NetFetcher>,
    _rng: impl Rng,
    vid: VolumeId,
) -> Result<(), Culprit<WorkloadErr>> {
    let handle = runtime
        .open_volume(&vid, VolumeConfig::new(SyncDirection::Pull))
        .or_into_ctx()?;

    let subscription = handle.subscribe_to_remote_changes();

    loop {
        let pre_change_snapshot = handle.snapshot().or_into_ctx()?;

        // wait for the next commit
        subscription.recv().expect("change subscription closed");

        let reader = handle.reader().or_into_ctx()?;
        tracing::info!(snapshot=?reader.snapshot(), "received commit for volume {:?}", vid);

        antithesis_sdk::assert_always_or_unreachable!(
            reader.snapshot() != &pre_change_snapshot,
            "the snapshot should be different after receiving a commit notification",
            &json!({ "snapshot": reader.snapshot() })
        );

        // load the page index
        let first_page = reader.read(0.into()).or_into_ctx()?;
        let page_tracker = PageTracker::deserialize_from_page(&first_page).or_into_ctx()?;

        // ensure all pages are either empty or have the expected hash
        for offset in reader.snapshot().local().page_count().offsets() {
            tracing::info!("reading page at offset {offset}");
            let page = reader.read(offset).or_into_ctx()?;

            if let Some(expected_hash) = page_tracker.get_hash(offset) {
                assert_always_or_unreachable!(
                    expected_hash == page,
                    "page should have expected contents",
                    &json!({ "offset": offset })
                );
            } else {
                assert_always_or_unreachable!(
                    page.is_empty(),
                    "page should be empty as it has never been written to",
                    &json!({ "offset": offset })
                );
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Culprit<WorkloadErr>> {
    antithesis_init();
    let running_in_antithesis = std::env::var("ANTITHESIS_OUTPUT_DIR").is_ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::TRACE.into())
                .from_env()?,
        )
        .with_span_events(FmtSpan::CLOSE)
        .with_ansi(!running_in_antithesis)
        .finish()
        .try_init()?;
    tracing::info!("starting test workload runner");

    let mut rng = antithesis_sdk::random::AntithesisRng;

    let args = Args::parse();

    tracing::info!("loading workload config from file {}", args.workload);

    let workload: Workload = Config::builder()
        .add_source(config::Environment::with_prefix("WORKLOAD").separator("_"))
        .add_source(config::File::with_name(&args.workload))
        .build()?
        .try_deserialize()?;

    tracing::info!(?workload, "loaded workload");

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
    let runtime = Runtime::new(NetFetcher::new(clients.clone()), storage);
    let sync_task = runtime.start_sync_task(clients, Duration::from_secs(1));

    antithesis_sdk::lifecycle::setup_complete(&json!({ "workload": workload }));

    // run the test for between 0 and 5 minutes
    let test_timeout = Duration::from_secs(rng.gen_range(0..300));
    tracing::info!(?test_timeout, "starting test workload");

    let workload_fut = spawn_blocking(move || workload.start(runtime.clone(), rng));

    select! {
        result = workload_fut => {
            result.expect("workload task panic")?;
            tracing::info!("workload finished");
        }
        _ = ctrl_c().fuse() => {
            tracing::info!("received SIGINT, shutting down");
        }
        _ = tokio::time::sleep(test_timeout).fuse() => {
            tracing::info!("test timeout reached");
        }
    }

    tracing::info!("waiting for sync task to shutdown");
    sync_task
        .shutdown_timeout(Duration::from_secs(u64::MAX))
        .or_into_ctx()?;

    antithesis_sdk::assert_reachable!("test workload finishes");

    tracing::info!("shutdown complete");

    Ok(())
}
