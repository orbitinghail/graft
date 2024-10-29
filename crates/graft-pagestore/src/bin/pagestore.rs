use std::{sync::Arc, time::Duration};

use futures::{select, FutureExt};
use graft_core::supervisor::Supervisor;
use object_store::memory::InMemory;
use pagestore::{
    api::task::ApiServerTask,
    segment::{bus::Bus, loader::Loader, uploader::SegmentUploaderTask, writer::SegmentWriterTask},
    storage::mem::MemCache,
    volume::catalog::VolumeCatalog,
};
use tokio::{net::TcpListener, signal::ctrl_c, sync::mpsc};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    rlimit::increase_nofile_limit(rlimit::INFINITY).expect("failed to increase nofile limit");

    let mut supervisor = Supervisor::default();

    let store = Arc::new(InMemory::default());
    let cache = Arc::new(MemCache::default());
    let catalog = VolumeCatalog::open_temporary().unwrap();
    let loader = Loader::new(store.clone(), cache.clone(), 8);

    let (page_tx, page_rx) = mpsc::channel(128);
    let (store_tx, store_rx) = mpsc::channel(8);
    let commit_bus = Bus::new(128);

    supervisor.spawn(SegmentWriterTask::new(
        page_rx,
        store_tx,
        Duration::from_secs(1),
    ));

    supervisor.spawn(SegmentUploaderTask::new(
        store_rx,
        commit_bus.clone(),
        store.clone(),
        cache.clone(),
    ));

    supervisor.spawn(ApiServerTask::new(
        TcpListener::bind("0.0.0.0:3000").await.unwrap(),
        page_tx,
        commit_bus,
        catalog,
        loader,
    ));

    select! {
        result = supervisor.supervise().fuse() => result.unwrap(),
        _ = ctrl_c().fuse() => {
            tracing::info!("received SIGINT, shutting down");
            supervisor.shutdown(Duration::from_secs(5)).await
        }
    };
}
