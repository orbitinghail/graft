use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::{Arc, Once},
    thread::JoinHandle,
    time::Duration,
};

use bytes::{Buf, BufMut, BytesMut};
use culprit::{Culprit, ResultExt};
use graft_client::{ClientPair, MetastoreClient, NetClient, PagestoreClient};
use graft_core::{
    page::{Page, PAGESIZE},
    PageIdx,
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
use graft_tracing::{init_tracing, TracingConsumer};
use precept::dispatch::test::TestDispatch;
use serde::{Deserialize, Serialize};
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

pub mod workload;

// this function is automatically run before each test by the macro graft_test_macro::test
pub fn setup_test() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        init_tracing(TracingConsumer::Test, None);
        precept::init(&TestDispatch).expect("failed to setup precept");
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
                let timeout = shutdown_rx.await.expect("shutdown channel closed");
                let result = supervisor.shutdown(timeout).await;
                result_tx.send(result).expect("result channel closed");
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

#[derive(Debug, Default, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub struct PageTracker {
    pages: HashMap<PageIdx, PageHash>,
}

impl PageTracker {
    pub fn upsert(&mut self, pageidx: PageIdx, hash: PageHash) -> Option<PageHash> {
        self.pages.insert(pageidx, hash)
    }

    pub fn get_hash(&self, pageidx: PageIdx) -> Option<&PageHash> {
        self.pages.get(&pageidx)
    }

    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    pub fn serialize_into_page(&self) -> Result<Page, Culprit<PageTrackerErr>> {
        let mut bytes = BytesMut::with_capacity(PAGESIZE.as_usize());
        let json = serde_json::to_vec(self).unwrap();
        if json.len() > (PAGESIZE.as_usize() - 8) {
            return Err(Culprit::new_with_note(
                PageTrackerErr::Serialize,
                "page size exceeded",
            ));
        }
        bytes.put_u64_le(json.len() as u64);
        bytes.put_slice(&json);
        bytes.resize(PAGESIZE.as_usize(), 0);
        Ok(Page::try_from(bytes.freeze()).or_ctx(|_| PageTrackerErr::Serialize)?)
    }

    pub fn deserialize_from_page(page: &Page) -> Result<Self, Culprit<PageTrackerErr>> {
        if page.is_empty() {
            tracing::warn!("empty page, initializing new page tracker");
            return Ok(Self::default());
        }

        let mut bytes = page.as_ref();
        let len = bytes.get_u64_le() as usize;
        let (json, _) = bytes.split_at(len);
        serde_json::from_slice(&json).or_ctx(|_| PageTrackerErr::Deserialize)
    }
}

#[derive(Default, PartialEq, Eq, Clone)]
pub struct PageHash([u8; 32]);

impl PageHash {
    pub fn new(page: &Page) -> Self {
        if page.is_empty() {
            Self([0; 32])
        } else {
            Self(blake3::hash(page.as_ref()).into())
        }
    }
}

impl Serialize for PageHash {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&bs58::encode(&self.0).into_string())
        } else {
            serializer.serialize_bytes(&self.0)
        }
    }
}

impl<'de> Deserialize<'de> for PageHash {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let s = <&'de str>::deserialize(deserializer)?;
            let bytes = bs58::decode(s)
                .into_vec()
                .map_err(serde::de::Error::custom)?;
            if bytes.len() != 32 {
                return Err(serde::de::Error::custom("invalid hash length"));
            }
            let mut hash = [0; 32];
            hash.copy_from_slice(&bytes);
            Ok(Self(hash))
        } else {
            Ok(Self(<[u8; 32]>::deserialize(deserializer)?))
        }
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
    use graft_core::page::{Page, EMPTY_PAGE};

    use crate::PageHash;

    #[test]
    fn page_hash_serializes() {
        let mut pages = (0..100).map(|_| rand::random::<Page>()).collect::<Vec<_>>();
        pages.push(EMPTY_PAGE);

        // round trip each one through serialize/deserialize
        for page in pages {
            let hash = PageHash::new(&page);
            let json = serde_json::to_string(&hash).unwrap();
            let hash2: PageHash = serde_json::from_str(&json).unwrap();
            assert_eq!(hash, hash2);
        }
    }
}
