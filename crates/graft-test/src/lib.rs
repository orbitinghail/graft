use std::{
    collections::HashMap,
    sync::{Arc, Once},
    thread::JoinHandle,
    time::Duration,
};

use bytes::{Buf, BufMut, BytesMut};
use culprit::{Culprit, ResultExt};
use graft_client::{ClientBuilder, ClientPair, MetastoreClient, PagestoreClient};
use graft_core::{
    page::{Page, PAGESIZE},
    page_offset::PageOffset,
};
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
    supervisor::{ShutdownErr, Supervisor},
    volume::{catalog::VolumeCatalog, store::VolumeStore, updater::VolumeCatalogUpdater},
};
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::{
        mpsc,
        oneshot::{self},
    },
};
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

pub struct GraftBackend {
    shutdown_tx: oneshot::Sender<Duration>,
    result_rx: oneshot::Receiver<Result<(), Culprit<ShutdownErr>>>,
    handle: JoinHandle<()>,
}

pub fn start_graft_backend() -> (GraftBackend, ClientPair) {
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .expect("failed to construct tokio runtime");

    let mut supervisor = Supervisor::default();
    let metastore = runtime.block_on(run_metastore(&mut supervisor));
    let pagestore = runtime.block_on(run_pagestore(metastore.clone(), &mut supervisor));

    let handle = std::thread::spawn(move || {
        runtime.block_on(async {
            let timeout = shutdown_rx.await.expect("shutdown channel closed");
            let result = supervisor.shutdown(timeout).await;
            result_tx.send(result).expect("result channel closed");
        })
    });

    (
        GraftBackend { shutdown_tx, result_rx, handle },
        ClientPair::new(metastore, pagestore),
    )
}

impl GraftBackend {
    pub fn shutdown(self, timeout: Duration) -> Result<(), Culprit<ShutdownErr>> {
        self.shutdown_tx
            .send(timeout)
            .expect("shutdown channel closed");

        self.handle.join().expect("backend thread panic");

        self.result_rx
            .blocking_recv()
            .expect("result channel closed")
    }
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

#[derive(Debug, Error)]
pub enum PageTrackerErr {
    #[error("failed to serialize page tracker")]
    Serialize,

    #[error("failed to deserialize page tracker")]
    Deserialize,
}

#[derive(Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct PageTracker {
    pages: HashMap<PageOffset, PageHash>,
}

impl PageTracker {
    pub fn upsert(&mut self, offset: PageOffset, page: &Page) -> Option<PageHash> {
        self.pages.insert(offset, PageHash::new(page))
    }

    pub fn get_hash(&self, offset: PageOffset) -> Option<&PageHash> {
        self.pages.get(&offset)
    }

    pub fn serialize_into_page(&self) -> Result<Page, Culprit<PageTrackerErr>> {
        let mut bytes = BytesMut::zeroed(PAGESIZE.as_usize());
        let json = serde_json::to_vec(self).unwrap();
        if json.len() > (PAGESIZE.as_usize() - 8) {
            return Err(Culprit::new_with_note(
                PageTrackerErr::Serialize,
                "page size exceeded",
            ));
        }
        bytes.put_u64_le(json.len() as u64);
        bytes.put_slice(&json);
        Ok(Page::try_from(bytes.freeze()).or_ctx(|_| PageTrackerErr::Serialize)?)
    }

    pub fn deserialize_from_page(page: &Page) -> Result<Self, Culprit<PageTrackerErr>> {
        let mut bytes = page.as_ref();
        let len = bytes.get_u64_le() as usize;
        let (json, _) = bytes.split_at(len);
        serde_json::from_slice(&json).or_ctx(|_| PageTrackerErr::Deserialize)
    }
}

#[derive(Debug, Default, serde::Deserialize, serde:: Serialize, PartialEq, Eq)]
pub struct PageHash([u8; 32]);

impl PageHash {
    pub fn new(page: &Page) -> Self {
        Self(blake3::hash(page.as_ref()).into())
    }
}

impl PartialEq<Page> for PageHash {
    fn eq(&self, other: &Page) -> bool {
        self.0 == <[u8; 32]>::from(blake3::hash(other.as_ref()))
    }
}

impl PartialEq<Page> for &PageHash {
    fn eq(&self, other: &Page) -> bool {
        self.0 == <[u8; 32]>::from(blake3::hash(other.as_ref()))
    }
}
