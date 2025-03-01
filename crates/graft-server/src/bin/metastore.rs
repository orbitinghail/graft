use std::{sync::Arc, time::Duration};

use config::{Config, FileFormat};
use futures::FutureExt;
use graft_server::{
    api::{
        metastore::{MetastoreApiState, metastore_routes},
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
use graft_tracing::{TracingConsumer, init_tracing};
use precept::dispatch::{antithesis::AntithesisDispatch, noop::NoopDispatch};
use serde::{Deserialize, Serialize};
use tokio::{net::TcpListener, select, signal::ctrl_c};

#[derive(Debug, Deserialize, Serialize)]
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
        AntithesisDispatch::try_load_boxed().unwrap_or_else(|| NoopDispatch::new_boxed());
    precept::init_boxed(dispatcher).expect("failed to setup precept");

    init_tracing(TracingConsumer::Server, None);
    tracing::info!("starting metastore");

    precept::setup_complete!();

    rlimit::increase_nofile_limit(rlimit::INFINITY).expect("failed to increase nofile limit");

    let config = Config::builder()
        .add_source(config::File::new("metastore.toml", FileFormat::Toml).required(false))
        .add_source(config::Environment::with_prefix("METASTORE").separator("_"))
        .build()
        .expect("failed to load config");
    let config: MetastoreConfig = config
        .try_deserialize()
        .expect("failed to deserialize config");

    let toml_config = toml::to_string_pretty(&config).expect("failed to serialize config");
    tracing::info!("loaded configuration:\n{toml_config}");

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
