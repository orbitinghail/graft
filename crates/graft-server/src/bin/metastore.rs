use std::{sync::Arc, time::Duration};

use antithesis_sdk::antithesis_init;
use config::Config;
use futures::FutureExt;
use graft_server::{
    api::{
        metastore::{metastore_routes, MetastoreApiState},
        routes::build_router,
        task::ApiServerTask,
    },
    metrics::registry::Registry,
    object_store_util::ObjectStoreConfig,
    supervisor::Supervisor,
    volume::{
        catalog::{VolumeCatalog, VolumeCatalogConfig},
        store::VolumeStore,
        updater::VolumeCatalogUpdater,
    },
};
use serde::Deserialize;
use serde_json::json;
use tokio::{net::TcpListener, select, signal::ctrl_c};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{fmt::format::FmtSpan, util::SubscriberInitExt, EnvFilter};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(default)]
struct MetastoreConfig {
    catalog: VolumeCatalogConfig,
    objectstore: ObjectStoreConfig,
    port: u16,
    catalog_update_concurrency: usize,
}

impl Default for MetastoreConfig {
    fn default() -> Self {
        Self {
            catalog: Default::default(),
            objectstore: Default::default(),
            port: 3001,
            catalog_update_concurrency: 16,
        }
    }
}

#[tokio::main]
async fn main() {
    antithesis_init();
    let running_in_antithesis = std::env::var("ANTITHESIS_OUTPUT_DIR").is_ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env()
                .expect("failed to initialize env filter"),
        )
        .with_span_events(FmtSpan::CLOSE)
        .with_ansi(!running_in_antithesis)
        .finish()
        .try_init()
        .expect("failed to initialize tracing subscriber");
    tracing::info!("starting metastore");

    antithesis_sdk::lifecycle::setup_complete(&json!({}));

    rlimit::increase_nofile_limit(rlimit::INFINITY).expect("failed to increase nofile limit");

    let config = Config::builder()
        .add_source(config::File::with_name("metastore").required(false))
        .add_source(config::Environment::with_prefix("METASTORE").separator("_"))
        .build()
        .expect("failed to load config");
    let config: MetastoreConfig = config
        .try_deserialize()
        .expect("failed to deserialize config");

    tracing::info!(?config, "loaded configuration");

    let store = config
        .objectstore
        .build()
        .expect("failed to build object store");
    let store = Arc::new(VolumeStore::new(store));
    let catalog =
        VolumeCatalog::open_config(config.catalog).expect("failed to open volume catalog");
    let updater = VolumeCatalogUpdater::new(config.catalog_update_concurrency);

    let state = Arc::new(MetastoreApiState::new(store, catalog, updater));
    let router = build_router(Registry::default(), state, metastore_routes());

    let addr = format!("0.0.0.0:{}", config.port);
    tracing::info!("listening on {}", addr);

    let mut supervisor = Supervisor::default();
    supervisor.spawn(ApiServerTask::new(
        "metastore-api-server",
        TcpListener::bind(addr).await.unwrap(),
        router,
    ));

    select! {
        result = supervisor.supervise().fuse() => result.unwrap(),
        _ = ctrl_c().fuse() => {
            tracing::info!("received SIGINT, shutting down");
            supervisor.shutdown(Duration::from_secs(5)).await.unwrap()
        }
    };
}
