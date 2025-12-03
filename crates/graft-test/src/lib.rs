use std::{
    ffi::CString,
    fmt::{Debug, Display},
    ops::{Deref, DerefMut},
    sync::{Arc, Once},
    thread::JoinHandle,
};

use graft::{
    local::fjall_storage::FjallStorage,
    remote::{Remote, RemoteConfig},
    rt::runtime::Runtime,
};
use graft_core::{
    LogId, PageCount, PageIdx,
    page::{PAGESIZE, Page},
    pageidx,
};
use graft_sqlite::vfs::GraftVfs;
use graft_tracing::{SubscriberInitExt, TracingConsumer, setup_tracing_with_writer};
use precept::dispatch::test::TestDispatch;
use rusqlite::{Connection, OpenFlags, ToSql};
use sqlite_plugin::vfs::{RegisterOpts, register_static};
use thiserror::Error;
use tokio::sync::Notify;
use tracing_subscriber::fmt::TestWriter;

pub use graft_test_macro::datatest;
pub use graft_test_macro::test;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

// pub mod workload;

// this function is automatically run before each test by the macro graft_test_macro::test
pub fn setup_test() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        setup_tracing_with_writer(TracingConsumer::Test, TestWriter::default()).init();
        precept::init(&TestDispatch).expect("failed to setup precept");
        precept::disable_faults();
    });
}

pub struct GraftTestRuntime {
    thread: JoinHandle<()>,
    runtime: Runtime,
    remote: Arc<Remote>,
    shutdown_tx: Arc<tokio::sync::Notify>,

    // this is set the first time a vfs is created for this test runtime
    vfs_id: Option<CString>,
}

impl Deref for GraftTestRuntime {
    type Target = Runtime;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

impl DerefMut for GraftTestRuntime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.runtime
    }
}

impl GraftTestRuntime {
    pub fn with_memory_remote() -> GraftTestRuntime {
        let remote = Arc::new(RemoteConfig::Memory.build().unwrap());
        Self::with_remote(remote)
    }

    pub fn with_remote(remote: Arc<Remote>) -> GraftTestRuntime {
        let thread_builder = std::thread::Builder::new().name("graft-runtime".to_string());

        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .start_paused(true)
            .enable_all()
            .build()
            .unwrap();

        let storage = Arc::new(FjallStorage::open_temporary().unwrap());
        let runtime = Runtime::new(tokio_rt.handle().clone(), remote.clone(), storage, None);

        let shutdown_tx = Arc::new(Notify::const_new());
        let shutdown_rx = shutdown_tx.clone();

        let thread = thread_builder
            .spawn(move || tokio_rt.block_on(async { shutdown_rx.notified().await }))
            .expect("failed to spawn backend thread");

        GraftTestRuntime {
            thread,
            runtime,
            remote,
            shutdown_tx,
            vfs_id: None,
        }
    }

    /// Spawn a new runtime connected to the same remote as this runtime
    pub fn spawn_peer(&self) -> GraftTestRuntime {
        Self::with_remote(self.remote.clone())
    }

    pub fn open_sqlite(&mut self, dbname: &str, remote: Option<LogId>) -> GraftSqliteConn {
        let vfs_id = self.vfs_id.get_or_insert_with(|| {
            // generate a 16 byte random ascii CString
            let vfs_id = {
                let mut bytes = [0u8; 16];
                for byte in bytes.iter_mut() {
                    *byte = rand::random::<u8>() % 26 + b'a';
                }
                CString::new(bytes.to_vec()).unwrap()
            };

            register_static(
                vfs_id.clone(),
                GraftVfs::new(self.runtime.clone()),
                RegisterOpts { make_default: false },
            )
            .expect("failed to register vfs");
            vfs_id
        });
        // setup vfs if needed
        let conn = Connection::open_with_flags_and_vfs(
            dbname,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
            vfs_id.as_c_str(),
        )
        .unwrap();
        let conn = GraftSqliteConn { conn };
        if let Some(remote) = remote {
            conn.graft_pragma_arg("clone", remote.serialize());
        }
        conn
    }

    pub fn shutdown(self) -> std::thread::Result<()> {
        self.shutdown_tx.notify_one();
        self.thread.join()
    }
}

pub struct GraftSqliteConn {
    conn: rusqlite::Connection,
}

impl Deref for GraftSqliteConn {
    type Target = rusqlite::Connection;

    fn deref(&self) -> &Self::Target {
        &self.conn
    }
}

impl DerefMut for GraftSqliteConn {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.conn
    }
}

impl GraftSqliteConn {
    pub fn graft_pragma(&self, suffix: &str) {
        let pragma = format!("graft_{suffix}");
        self.pragma_query(None, &pragma, |row| {
            let output: String = row.get(0).unwrap();
            tracing::debug!("{pragma} output: {output}");
            Ok(())
        })
        .unwrap();
    }

    pub fn graft_pragma_arg<T: ToSql>(&self, suffix: &str, arg: T) {
        let pragma = format!("graft_{suffix}");
        self.pragma(None, &pragma, arg, |row| {
            let output: String = row.get(0).unwrap();
            tracing::debug!("{pragma} output: {output}");
            Ok(())
        })
        .unwrap();
    }
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

    pub fn finish(&mut self) {
        self.remaining = 0
    }

    pub fn is_done(&self) -> bool {
        self.remaining == 0
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
            let pageidx = PageIdx::must_new(i as u32 + 1);
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
