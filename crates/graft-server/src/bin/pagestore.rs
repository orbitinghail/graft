use std::{sync::Arc, time::Duration};

use futures::{select, FutureExt};
use graft_client::MetaStoreClient;
use graft_core::byte_unit::ByteUnit;
use graft_server::{
    api::{
        pagestore::{pagestore_router, PagestoreApiState},
        task::ApiServerTask,
    },
    segment::{
        bus::Bus,
        cache::disk::{DiskCache, DiskCacheConfig},
        loader::SegmentLoader,
        uploader::SegmentUploaderTask,
        writer::SegmentWriterTask,
    },
    supervisor::Supervisor,
    volume::{
        catalog::{VolumeCatalog, VolumeCatalogConfig},
        updater::VolumeCatalogUpdater,
    },
};
use object_store::memory::InMemory;
use rlimit::Resource;
use tokio::{net::TcpListener, signal::ctrl_c, sync::mpsc};
use twelf::config;

#[config]
#[derive(Debug)]
struct Config {
    catalog: VolumeCatalogConfig,
    cache: DiskCacheConfig,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!("starting pagestore");

    rlimit::increase_nofile_limit(rlimit::INFINITY).expect("failed to increase nofile limit");

    let config = Config {
        catalog: VolumeCatalogConfig {
            path: tempfile::tempdir()
                .expect("failed to create temporary directory")
                .into_path(),
            temporary: true,
        },
        cache: DiskCacheConfig {
            path: tempfile::tempdir()
                .expect("failed to create temporary directory")
                .into_path(),
            space_limit: ByteUnit::from_gb(1),
            open_limit: rlimit::getrlimit(Resource::NOFILE)
                .expect("failed to get nofile limit")
                .0 as usize
                / 2,
        },
    };

    assert!(config.cache.open_limit > 128, "cache_open_limit is too low");

    let mut supervisor = Supervisor::default();

    let store = Arc::new(InMemory::default());
    let cache = Arc::new(DiskCache::new(config.cache));
    let catalog = VolumeCatalog::open_temporary().unwrap();
    let loader = SegmentLoader::new(store.clone(), cache.clone(), 8);
    let updater = VolumeCatalogUpdater::new(8);

    let (page_tx, page_rx) = mpsc::channel(128);
    let (store_tx, store_rx) = mpsc::channel(8);
    let commit_bus = Bus::new(128);

    let metastore = MetaStoreClient::default();

    let api_state = Arc::new(PagestoreApiState::new(
        page_tx,
        commit_bus.clone(),
        catalog.clone(),
        loader,
        metastore,
        updater,
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
