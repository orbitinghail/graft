use std::{
    sync::{Arc, Once},
    time::Duration,
};

use graft_client::{ClientBuilder, ClientPair, MetastoreClient, PagestoreClient};
use graft_server::{
    api::{
        metastore::{metastore_routes, MetastoreApiState},
        pagestore::{pagestore_routes, PagestoreApiState},
        routes::build_router,
        task::ApiServerTask,
    },
    metrics::registry::Registry,
    object_store_util::ObjectStoreConfig,
    segment::{
        bus::Bus, cache::mem::MemCache, loader::SegmentLoader, uploader::SegmentUploaderTask,
        writer::SegmentWriterTask,
    },
    supervisor::Supervisor,
    volume::{catalog::VolumeCatalog, store::VolumeStore, updater::VolumeCatalogUpdater},
};
use tokio::{net::TcpListener, sync::mpsc};
use url::Url;

pub fn setup_logger() {
    // setup logger only once
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        env_logger::Builder::from_default_env()
            .filter_module("graft_core", log::LevelFilter::Trace)
            .filter_module("graft_test", log::LevelFilter::Trace)
            .filter_module("graft_server", log::LevelFilter::Trace)
            .filter_module("graft_client", log::LevelFilter::Trace)
            .filter_level(log::LevelFilter::Info)
            .init();
    });
}

pub async fn run_graft_services() -> (Supervisor, ClientPair) {
    let mut supervisor = Supervisor::default();

    let metastore = run_metastore(&mut supervisor).await;
    let pagestore = run_pagestore(metastore.clone(), &mut supervisor).await;

    (supervisor, ClientPair::new(metastore, pagestore))
}

pub async fn run_metastore(supervisor: &mut Supervisor) -> MetastoreClient {
    let obj_store = ObjectStoreConfig::Memory.build().unwrap();
    let vol_store = Arc::new(VolumeStore::new(obj_store));
    let catalog = VolumeCatalog::open_temporary().unwrap();
    let updater = VolumeCatalogUpdater::new(8);
    let state = Arc::new(MetastoreApiState::new(vol_store, catalog, updater));
    let router = build_router(Registry::default(), state, metastore_routes());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let endpoint = Url::parse(&format!("http://localhost:{port}")).unwrap();
    supervisor.spawn(ApiServerTask::new("metastore-api", listener, router));
    ClientBuilder::new(endpoint).build().unwrap()
}

pub async fn run_pagestore(
    metastore: MetastoreClient,
    supervisor: &mut Supervisor,
) -> PagestoreClient {
    let mut registry = Registry::default();
    let obj_store = ObjectStoreConfig::Memory.build().unwrap();
    let cache = Arc::new(MemCache::default());
    let catalog = VolumeCatalog::open_temporary().unwrap();
    let loader = SegmentLoader::new(obj_store.clone(), cache.clone(), 8);
    let updater = VolumeCatalogUpdater::new(10);

    let (page_tx, page_rx) = mpsc::channel(128);
    let (store_tx, store_rx) = mpsc::channel(8);
    let commit_bus = Bus::new(128);

    supervisor.spawn(SegmentWriterTask::new(
        registry.segment_writer(),
        page_rx,
        store_tx,
        Duration::from_secs(1),
    ));

    supervisor.spawn(SegmentUploaderTask::new(
        registry.segment_uploader(),
        store_rx,
        commit_bus.clone(),
        obj_store,
        cache,
    ));

    let state = Arc::new(PagestoreApiState::new(
        page_tx,
        commit_bus,
        catalog.clone(),
        loader,
        metastore,
        updater,
        10,
    ));
    let router = build_router(registry, state, pagestore_routes());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let endpoint = Url::parse(&format!("http://localhost:{port}")).unwrap();
    supervisor.spawn(ApiServerTask::new("pagestore-api", listener, router));

    ClientBuilder::new(endpoint).build().unwrap()
}
