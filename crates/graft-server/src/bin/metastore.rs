use std::{sync::Arc, time::Duration};

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
use graft_tracing::{tracing_init, TracingConsumer};
use precept::dispatch::antithesis::AntithesisDispatch;
use serde::Deserialize;
use tokio::{net::TcpListener, select, signal::ctrl_c};

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
    let dispatcher =
        Box::new(AntithesisDispatch::try_load().expect("failed to setup antithesis dispatch"));
    precept::init(Box::leak(dispatcher)).expect("failed to setup precept");

    tracing_init(TracingConsumer::Server, None);
    tracing::info!("starting metastore");

    precept::setup_complete!();

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
