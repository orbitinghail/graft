use std::{sync::Arc, time::Duration};

use config::{Config, FileFormat};
use futures::FutureExt;
use graft_server::{
    api::{
        auth::AuthState,
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
    auth: Option<AuthState>,

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
            auth: None,
        }
    }
}

#[tokio::main]
async fn main() {
    let dispatcher =
        AntithesisDispatch::try_load_boxed().unwrap_or_else(|| NoopDispatch::new_boxed());
    precept::init_boxed(dispatcher).expect("failed to setup precept");

    // sanity check that we don't enable precept in production
    let is_production = std::env::var("GRAFT_PRODUCTION").is_ok();
    assert!(
        !(is_production && precept::ENABLED),
        "precept is enabled in production"
    );

    init_tracing(TracingConsumer::Server, None);
    tracing::info!("starting Graft metastore");

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

    assert!(
        !is_production || config.auth.is_some(),
        "auth must be configured in production"
    );

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

    let auth = config.auth.map(|c| c.into());
    let state = Arc::new(MetastoreApiState::new(store, catalog, updater));
    let router = build_router(Registry::default(), auth, state, metastore_routes());

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
