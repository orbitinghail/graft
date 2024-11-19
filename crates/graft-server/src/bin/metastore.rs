use std::{sync::Arc, time::Duration};

use futures::FutureExt;
use graft_server::{
    api::{
        metastore::{metastore_router, MetastoreApiState},
        task::ApiServerTask,
    },
    object_store_util::ObjectStoreConfig,
    supervisor::Supervisor,
    volume::{
        catalog::{VolumeCatalog, VolumeCatalogConfig},
        store::VolumeStore,
        updater::VolumeCatalogUpdater,
    },
};
use tokio::{net::TcpListener, select, signal::ctrl_c};
use twelf::{config, Layer};

#[config]
#[derive(Debug, Default)]
struct Config {
    catalog: VolumeCatalogConfig,
    object_store: ObjectStoreConfig,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!("starting metastore");

    rlimit::increase_nofile_limit(rlimit::INFINITY).expect("failed to increase nofile limit");

    let config = Config::with_layers(&[
        Layer::DefaultTrait,
        Layer::Toml("metastore.toml".into()),
        Layer::Env(Some("METASTORE_".to_string())),
    ])
    .expect("failed to load configuration");

    tracing::info!(?config, "loaded configuration");

    let store = config
        .object_store
        .build()
        .expect("failed to build object store");
    let store = Arc::new(VolumeStore::new(store));
    let catalog =
        VolumeCatalog::open_config(config.catalog).expect("failed to open volume catalog");
    let updater = VolumeCatalogUpdater::new(8);

    let state = Arc::new(MetastoreApiState::new(store, catalog, updater));
    let router = metastore_router().with_state(state);

    let mut supervisor = Supervisor::default();
    supervisor.spawn(ApiServerTask::new(
        TcpListener::bind("0.0.0.0:3001").await.unwrap(),
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
