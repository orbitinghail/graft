pub mod workload;

use std::{
    ffi::CString,
    ops::{Deref, DerefMut},
    sync::{Arc, Once},
    thread::JoinHandle,
};

use graft::core::LogId;
use graft::{
    local::fjall_storage::FjallStorage,
    remote::{Remote, RemoteConfig},
    rt::runtime::Runtime,
};
use graft_sqlite::vfs::GraftVfs;
use graft_tracing::{SubscriberInitExt, TracingConsumer, setup_tracing_with_writer};
use precept::dispatch::test::TestDispatch;
use rusqlite::{Connection, OpenFlags, ToSql};
use sqlite_plugin::vfs::{RegisterOpts, register_static};
use tokio::sync::Notify;
use tracing_subscriber::fmt::TestWriter;

pub use graft_test_macro::datatest;
pub use graft_test_macro::test;

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

impl Into<rusqlite::Connection> for GraftSqliteConn {
    fn into(self) -> rusqlite::Connection {
        self.conn
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
