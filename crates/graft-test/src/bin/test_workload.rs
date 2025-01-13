use std::time::Duration;

use antithesis_sdk::{antithesis_init, assert_always_or_unreachable};
use clap::Parser;
use config::{Config, ConfigError};
use culprit::{Culprit, ResultExt};
use futures::FutureExt;
use graft_client::{
    runtime::{
        handle::RuntimeHandle,
        storage::{
            volume::{SyncDirection, VolumeConfig},
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
use tokio::{select, signal::ctrl_c, sync::broadcast, time::sleep};
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
    async fn start(
        self,
        handle: &RuntimeHandle,
        rng: impl Rng,
    ) -> Result<(), Culprit<WorkloadErr>> {
        match self {
            Workload::Writer { vid, interval_ms: interval } => {
                workload_writer(handle, rng, vid, Duration::from_millis(interval)).await
            }
            Workload::Reader { vid } => workload_reader(handle, rng, vid).await,
        }
    }
}

/// This workload continuously writes and reads pages to a volume, verifying the
/// contents of pages are always correct
async fn workload_writer(
    handle: &RuntimeHandle,
    mut rng: impl Rng,
    vid: VolumeId,
    interval: Duration,
) -> Result<(), Culprit<WorkloadErr>> {
    let mut page_tracker = PageTracker::default();

    handle
        .add_volume(&vid, VolumeConfig::new(SyncDirection::Both))
        .or_into_ctx()?;

    loop {
        // randomly pick a page offset and a page value.
        // select the next offset to ensure we don't pick the 0th page
        let offset = PageOffset::test_random(&mut rng, 16).next();
        let new_page: Page = rng.gen();
        let existing_hash = page_tracker.upsert(offset, &new_page);

        tracing::info!(?offset, "writing page");

        // verify the offset is missing or present as expected
        let txn = handle.read_txn(&vid).or_into_ctx()?;
        let page = txn.read(offset).or_into_ctx()?;
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

        let mut txn = handle.write_txn(&vid).or_into_ctx()?;

        // write the page to the volume and update the page index
        txn.write(offset, new_page);
        txn.write(0.into(), page_tracker.serialize_into_page().or_into_ctx()?);

        txn.commit().or_into_ctx()?;

        sleep(interval).await;
    }
}

async fn workload_reader(
    handle: &RuntimeHandle,
    _rng: impl Rng,
    vid: VolumeId,
) -> Result<(), Culprit<WorkloadErr>> {
    handle
        .add_volume(&vid, VolumeConfig::new(SyncDirection::Pull))
        .or_into_ctx()?;

    let mut commits_rx = handle.subscribe_to_remote_commits();

    loop {
        let commit_vid = commits_rx.recv().await.or_into_ctx()?;
        if commit_vid != vid {
            antithesis_sdk::assert_unreachable!("received commit for unexpected volume");
        }

        let Some(snapshot) = handle.snapshot(&vid).or_into_ctx()? else {
            antithesis_sdk::assert_unreachable!("volume has no snapshot");
            continue;
        };

        tracing::info!(?snapshot, "received commit for volume {:?}", vid);

        let txn = handle.read_txn(&vid).or_into_ctx()?;

        antithesis_sdk::assert_always_or_unreachable!(
            txn.snapshot() == Some(&snapshot),
            "read transaction should be using the expected snapshot",
            &json!({ "actual": txn.snapshot(), "expected": snapshot })
        );

        // load the page index
        let first_page = txn.read(0.into()).or_into_ctx()?;
        let page_tracker = PageTracker::deserialize_from_page(&first_page).or_into_ctx()?;

        // ensure all pages are either empty or have the expected hash
        for offset in snapshot.page_count().offsets() {
            tracing::info!("reading page at offset {offset}");
            let page = txn.read(offset).or_into_ctx()?;

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

    let storage = Storage::open_temporary().unwrap();
    let handle = RuntimeHandle::new(storage);

    let metastore_client: MetastoreClient =
        ClientBuilder::new(Url::parse("http://metastore:3001").unwrap())
            .build()
            .or_into_ctx()?;
    let pagestore_client: PagestoreClient =
        ClientBuilder::new(Url::parse("http://pagestore:3000").unwrap())
            .build()
            .or_into_ctx()?;
    let clients = ClientPair::new(metastore_client, pagestore_client);

    let sync_task = handle.spawn_sync_task(clients, Duration::from_secs(1));

    antithesis_sdk::lifecycle::setup_complete(&json!({ "workload": workload }));

    // run the test for between 0 and 5 minutes
    let test_timeout = Duration::from_secs(rng.gen_range(0..300));
    tracing::info!(?test_timeout, "starting test workload");

    select! {
        _ = workload.start(&handle, rng).fuse() => {
            tracing::info!("workload finished");
        }
        _ = ctrl_c().fuse() => {
            tracing::info!("received SIGINT, shutting down");
        }
        _ = sleep(test_timeout).fuse() => {
            tracing::info!("test timeout reached");
        }
    }

    tracing::info!("waiting for sync task to shutdown");
    sync_task.shutdown().await.or_into_ctx()?;

    antithesis_sdk::assert_reachable!("test workload finishes");

    tracing::info!("shutdown complete");

    Ok(())
}
