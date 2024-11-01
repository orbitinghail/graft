use std::{sync::Arc, time::Duration};

use futures::{select, FutureExt};
use graft_core::byte_unit::ByteUnit;
use graft_server::{
    api::{
        pagestore::{pagestore_router, PagestoreApiState},
        task::ApiServerTask,
    },
    segment::{
        bus::Bus, cache::disk::DiskCache, loader::Loader, uploader::SegmentUploaderTask,
        writer::SegmentWriterTask,
    },
    supervisor::Supervisor,
    volume::catalog::VolumeCatalog,
};
use object_store::memory::InMemory;
use rlimit::Resource;
use tokio::{net::TcpListener, signal::ctrl_c, sync::mpsc};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    rlimit::increase_nofile_limit(rlimit::INFINITY).expect("failed to increase nofile limit");

    // eventually make these configurable and persistent
    let cache_dir = tempfile::tempdir().expect("failed to create temporary directory");
    let cache_size = ByteUnit::from_gb(1);
    let cache_open_limit = rlimit::getrlimit(Resource::NOFILE)
        .expect("failed to get nofile limit")
        .0 as usize
        / 2;

    assert!(cache_open_limit > 128, "cache_open_limit is too low");

    let mut supervisor = Supervisor::default();

    let store = Arc::new(InMemory::default());
    let cache = Arc::new(DiskCache::new(cache_dir, cache_size, cache_open_limit));
    let catalog = VolumeCatalog::open_temporary().unwrap();
    let loader = Loader::new(store.clone(), cache.clone(), 8);

    let (page_tx, page_rx) = mpsc::channel(128);
    let (store_tx, store_rx) = mpsc::channel(8);
    let commit_bus = Bus::new(128);

    let api_state = Arc::new(PagestoreApiState::new(
        page_tx,
        commit_bus.clone(),
        catalog.clone(),
        loader,
    ));
    let router = pagestore_router().with_state(api_state);

    supervisor.spawn(SegmentWriterTask::new(
        page_rx,
        store_tx,
        Duration::from_secs(1),
    ));

    supervisor.spawn(SegmentUploaderTask::new(store_rx, commit_bus, store, cache));

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
