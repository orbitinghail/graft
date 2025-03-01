use std::{
    fmt::{Debug, Display},
    sync::{Arc, Once},
    thread::JoinHandle,
    time::Duration,
};

use culprit::Culprit;
use graft_client::{ClientPair, MetastoreClient, NetClient, PagestoreClient};
use graft_core::{
    PageCount, PageIdx,
    page::{PAGESIZE, Page},
    pageidx,
};
use graft_server::{
    api::{
        metastore::{MetastoreApiState, metastore_routes},
        pagestore::{PagestoreApiState, pagestore_routes},
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
use graft_tracing::{TracingConsumer, init_tracing};
use precept::dispatch::test::TestDispatch;
use thiserror::Error;
use tokio::{
    net::TcpListener,
    sync::{
        mpsc,
        oneshot::{self},
    },
};
use url::Url;

pub use graft_test_macro::datatest;
pub use graft_test_macro::test;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

pub mod workload;

// this function is automatically run before each test by the macro graft_test_macro::test
pub fn setup_test() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        init_tracing(TracingConsumer::Test, None);
        precept::init(&TestDispatch, |_| true).expect("failed to setup precept");
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
        .thread_name("graft-backend")
        .enable_all()
        .start_paused(true)
        .build()
        .expect("failed to construct tokio runtime");

    let net_client = NetClient::new();

    let mut supervisor = Supervisor::default();
    let metastore = runtime.block_on(run_metastore(net_client.clone(), &mut supervisor));
    let pagestore = runtime.block_on(run_pagestore(
        net_client.clone(),
        metastore.clone(),
        &mut supervisor,
    ));

    let builder = std::thread::Builder::new().name("graft-backend".to_string());

    let handle = builder
        .spawn(move || {
            runtime.block_on(async {
                // if the shutdown channel closes, try to shutdown the superviser with a default timeout
                let timeout = shutdown_rx.await.unwrap_or(Duration::from_secs(5));
                let result = supervisor.shutdown(timeout).await;
                let _ = result_tx.send(result);
            })
        })
        .expect("failed to spawn backend thread");

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

pub async fn run_metastore(net_client: NetClient, supervisor: &mut Supervisor) -> MetastoreClient {
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
    MetastoreClient::new(endpoint, net_client)
}

pub async fn run_pagestore(
    net_client: NetClient,
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

    PagestoreClient::new(endpoint, net_client)
}

#[derive(Debug, Clone, Copy)]
pub struct Ticker {
    remaining: usize,
}

impl Ticker {
    pub fn new(remaining: usize) -> Self {
        Self { remaining }
    }

    pub fn tick(&mut self) -> bool {
        self.remaining = self.remaining.saturating_sub(1);
        self.remaining != 0
    }
}

#[derive(Debug, Error)]
pub enum PageTrackerErr {
    #[error("failed to serialize page tracker")]
    Serialize,

    #[error("failed to deserialize page tracker")]
    Deserialize,
}

#[derive(Debug, PartialEq, Eq, IntoBytes, FromBytes, Immutable, Unaligned, KnownLayout)]
#[repr(transparent)]
pub struct PageTracker {
    // 128 pages, indexed by page index
    pages: [PageHash; 128],
}

// ensure that the size of PageTracker is equal to the size of a page
static_assertions::const_assert!(std::mem::size_of::<PageTracker>() == PAGESIZE.as_usize());

impl Default for PageTracker {
    fn default() -> Self {
        Self { pages: [PageHash::default(); 128] }
    }
}

impl PageTracker {
    // the page tracker is stored after the data pages
    pub const PAGEIDX: PageIdx = pageidx!(129);
    pub const MAX_PAGES: PageCount = PageCount::new(128);

    pub fn insert(&mut self, pageidx: PageIdx, hash: PageHash) -> Option<PageHash> {
        let index = (pageidx.to_u32() - 1) as usize;
        if index >= self.pages.len() {
            panic!("page index out of bounds: {index}");
        }

        let out = std::mem::replace(&mut self.pages[index], hash);
        (!out.is_empty()).then_some(out)
    }

    pub fn get_hash(&self, pageidx: PageIdx) -> Option<&PageHash> {
        let index = (pageidx.to_u32() - 1) as usize;
        if index >= self.pages.len() {
            panic!("page index out of bounds: {index}");
        }
        if self.pages[index].is_empty() {
            None
        } else {
            Some(&self.pages[index])
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pages.iter().all(|hash| hash.is_empty())
    }
}

#[derive(
    Default, PartialEq, Eq, Clone, Copy, IntoBytes, FromBytes, Immutable, Unaligned, KnownLayout,
)]
#[repr(transparent)]
pub struct PageHash([u8; 32]);

impl PageHash {
    pub fn new(page: &Page) -> Self {
        if page.is_empty() {
            // bs58 encodes to `11111111111111111111111111111111`
            Self([0; 32])
        } else {
            Self(blake3::hash(page.as_ref()).into())
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|&b| b == 0)
    }
}

impl Debug for PageHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", bs58::encode(&self.0).into_string())
    }
}

impl Display for PageHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", bs58::encode(&self.0).into_string())
    }
}

#[cfg(test)]
mod test {
    use graft_core::{
        PageIdx,
        page::{PAGESIZE, Page},
    };
    use zerocopy::{FromBytes, IntoBytes};

    use crate::{PageHash, PageTracker};

    #[test]
    fn exercise_page_tracker() {
        let pages = (0..PageTracker::MAX_PAGES.to_u32())
            .map(|_| rand::random::<Page>())
            .collect::<Vec<_>>();

        let mut tracker = PageTracker::default();

        for (i, page) in pages.into_iter().enumerate() {
            let hash = PageHash::new(&page);
            let pageidx = PageIdx::new(i as u32 + 1);
            assert!(tracker.insert(pageidx, hash).is_none());
            assert_eq!(tracker.get_hash(pageidx), Some(&hash));
        }

        // round trip tracker
        let bytes = tracker.as_bytes();
        assert_eq!(bytes.len(), PAGESIZE.as_usize());
        let tracker2 = PageTracker::read_from_bytes(tracker.as_bytes()).unwrap();
        assert_eq!(tracker, tracker2);
    }
}
