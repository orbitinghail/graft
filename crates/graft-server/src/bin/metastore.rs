use std::{fs::exists, sync::Arc, time::Duration};

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
use tokio::{net::TcpListener, select, signal::ctrl_c};
use tracing_subscriber::{fmt::format::FmtSpan, util::SubscriberInitExt, EnvFilter};
use twelf::{config, Layer};

#[config]
#[derive(Debug)]
#[serde(deny_unknown_fields)]
struct Config {
    catalog: VolumeCatalogConfig,
    objectstore: ObjectStoreConfig,
    port: u16,
    catalog_update_concurrency: usize,
}

impl Default for Config {
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
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_span_events(FmtSpan::CLOSE)
        .finish()
        .try_init()
        .expect("failed to initialize tracing subscriber");
    tracing::info!("starting metastore");

    rlimit::increase_nofile_limit(rlimit::INFINITY).expect("failed to increase nofile limit");

    let mut layers = vec![
        Layer::DefaultTrait,
        Layer::Env(Some("METASTORE_".to_string())),
    ];

    if exists("metastore.toml").is_ok_and(|p| p) {
        // insert the toml layer at the second position, after the default trait
        // and before loading env vars
        layers.insert(1, Layer::Toml("metastore.toml".into()));
    }

    let config = Config::with_layers(&layers).expect("failed to load configuration");

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
            supervisor.shutdown(Duration::from_secs(5)).await
        }
    };
}
