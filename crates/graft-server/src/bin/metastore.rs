use std::{sync::Arc, time::Duration};

use futures::FutureExt;
use graft_server::{
    api::{
        metastore::{metastore_router, MetastoreApiState},
        task::ApiServerTask,
    },
    supervisor::Supervisor,
    volume::{catalog::VolumeCatalog, store::VolumeStore},
};
use object_store::memory::InMemory;
use tokio::{net::TcpListener, select, signal::ctrl_c};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    rlimit::increase_nofile_limit(rlimit::INFINITY).expect("failed to increase nofile limit");

    let mut supervisor = Supervisor::default();

    let store = Arc::new(InMemory::default());
    let store = Arc::new(VolumeStore::new(store));
    let catalog = VolumeCatalog::open_temporary().unwrap();

    let state = Arc::new(MetastoreApiState::new(store, catalog, 8));
    let router = metastore_router().with_state(state);

    supervisor.spawn(ApiServerTask::new(
        TcpListener::bind("0.0.0.0:3000").await.unwrap(),
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
