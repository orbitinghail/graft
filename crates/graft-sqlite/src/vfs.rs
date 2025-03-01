// TODO: remove this once the vfs is implemented
#![allow(unused)]

use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use culprit::{Culprit, ResultExt};
use graft_client::{
    ClientErr,
    runtime::{
        runtime::Runtime,
        storage::volume_state::{SyncDirection, VolumeConfig},
    },
};
use graft_core::{VolumeId, gid::GidParseErr};
use graft_tracing::TracingConsumer;
use parking_lot::Mutex;
use sqlite_plugin::{
    flags::{AccessFlags, LockLevel, OpenKind, OpenOpts},
    logger::{SqliteLogLevel, SqliteLogger},
    vars::{
        self, SQLITE_BUSY, SQLITE_BUSY_SNAPSHOT, SQLITE_CANTOPEN, SQLITE_INTERNAL, SQLITE_NOTFOUND,
        SQLITE_READONLY,
    },
    vfs::{Pragma, PragmaErr, Vfs, VfsHandle, VfsResult},
};
use thiserror::Error;
use tryiter::TryIteratorExt;

use crate::{
    file::{FileHandle, VfsFile, mem_file::MemFile, vol_file::VolFile},
    pragma::GraftPragma,
};

#[derive(Debug, Error)]
pub enum ErrCtx {
    #[error("Graft client error: {0}")]
    Client(#[from] ClientErr),

    #[error("Failed to parse VolumeId: {0}")]
    GidParseErr(#[from] GidParseErr),

    #[error("Unknown Pragma")]
    UnknownPragma,

    #[error("Cant open Volume")]
    CantOpen,

    #[error("Transaction is busy")]
    Busy,

    #[error("The transaction snapshot is no longer current")]
    BusySnapshot,

    #[error("Invalid lock transition")]
    InvalidLockTransition,

    #[error("Invalid volume state")]
    InvalidVolumeState,
}

impl ErrCtx {
    #[inline]
    fn wrap<T>(mut cb: impl FnOnce() -> culprit::Result<T, ErrCtx>) -> VfsResult<T> {
        match cb() {
            Ok(t) => Ok(t),
            Err(err) => {
                let code = match err.ctx() {
                    ErrCtx::UnknownPragma => SQLITE_NOTFOUND,
                    ErrCtx::CantOpen => SQLITE_CANTOPEN,
                    ErrCtx::Busy => SQLITE_BUSY,
                    ErrCtx::BusySnapshot => SQLITE_BUSY_SNAPSHOT,
                    _ => SQLITE_INTERNAL,
                };
                if code == SQLITE_INTERNAL {
                    tracing::error!("{}", err);
                }
                Err(code)
            }
        }
    }
}

impl<T> From<ErrCtx> for culprit::Result<T, ErrCtx> {
    fn from(err: ErrCtx) -> culprit::Result<T, ErrCtx> {
        Err(Culprit::new(err))
    }
}

pub struct GraftVfs {
    runtime: Runtime,
    locks: Mutex<HashMap<VolumeId, Arc<Mutex<()>>>>,
}

impl GraftVfs {
    pub fn new(runtime: Runtime) -> Self {
        Self { runtime, locks: Default::default() }
    }
}

impl Vfs for GraftVfs {
    type Handle = FileHandle;

    fn register_logger(&self, logger: SqliteLogger) {
        #[derive(Clone)]
        struct Writer(Arc<Mutex<SqliteLogger>>);

        impl std::io::Write for Writer {
            #[inline]
            fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
                self.0.lock().log(SqliteLogLevel::Notice, data);
                Ok(data.len())
            }

            #[inline]
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let writer = Writer(Arc::new(Mutex::new(logger)));
        let make_writer = move || writer.clone();
        graft_tracing::init_tracing_with_writer(TracingConsumer::Tool, None, make_writer);
    }

    fn pragma(
        &self,
        handle: &mut Self::Handle,
        pragma: Pragma<'_>,
    ) -> Result<Option<String>, PragmaErr> {
        tracing::trace!("pragma: file={handle:?}, pragma={pragma:?}");
        if let FileHandle::VolFile(file) = handle {
            GraftPragma::try_from(&pragma)?.eval(&self.runtime, file)
        } else {
            Err(PragmaErr::NotFound)
        }
    }

    fn access(&self, path: &str, flags: AccessFlags) -> VfsResult<bool> {
        tracing::trace!("access: path={path:?}; flags={flags:?}");
        ErrCtx::wrap(move || {
            if let Ok(vid) = path.parse::<VolumeId>() {
                Ok(self
                    .runtime
                    .iter_volumes()
                    .try_filter(|v| Ok(v.vid() == &vid))
                    .try_next()
                    .or_into_ctx()?
                    .is_some())
            } else {
                Ok(false)
            }
        })
    }

    fn open(&self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Self::Handle> {
        tracing::trace!("open: path={path:?}, opts={opts:?}");
        ErrCtx::wrap(move || {
            // we only open a Volume for main database files named after a Volume ID
            if opts.kind() == OpenKind::MainDb {
                if let Some(path) = path {
                    let vid: VolumeId = path.parse()?;

                    // get or create a reserved lock for this Volume
                    let reserved_lock = self.locks.lock().entry(vid.clone()).or_default().clone();

                    let handle = self
                        .runtime
                        .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
                        .or_into_ctx()?;
                    return Ok(VolFile::new(handle, opts, reserved_lock).into());
                }
            }

            // all other files use in-memory storage
            Ok(MemFile::default().into())
        })
    }

    fn close(&self, handle: Self::Handle) -> VfsResult<()> {
        tracing::trace!("close: file={handle:?}");
        ErrCtx::wrap(move || {
            match handle {
                FileHandle::MemFile(_) => Ok(()),
                FileHandle::VolFile(vol_file) => {
                    if vol_file.opts().delete_on_close() {
                        // TODO: do we want to actually delete volumes? or mark them for deletion?
                        self.runtime
                            .update_volume_config(vol_file.handle().vid(), |conf| {
                                conf.with_sync(SyncDirection::Disabled)
                            })
                            .or_into_ctx()?;
                    }

                    // close and drop the vol_file
                    let handle = vol_file.close();

                    let mut locks = self.locks.lock();
                    let reserved_lock = locks
                        .get(handle.vid())
                        .expect("reserved lock missing from lock manager");

                    // clean up the lock if this was the last reference
                    // SAFETY: we are holding a lock on the lock manager,
                    // preventing any concurrent opens from incrementing the
                    // reference count
                    if Arc::strong_count(reserved_lock) == 1 {
                        locks.remove(handle.vid());
                    }

                    Ok(())
                }
            }
        })
    }

    fn delete(&self, path: &str) -> VfsResult<()> {
        tracing::trace!("delete: path={path:?}");
        ErrCtx::wrap(|| {
            if let Ok(vid) = path.parse() {
                // TODO: do we want to actually delete volumes? or mark them for deletion?
                self.runtime
                    .update_volume_config(&vid, |conf| conf.with_sync(SyncDirection::Disabled))
                    .or_into_ctx()?;
            }
            Ok(())
        })
    }

    fn lock(&self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        tracing::trace!("lock: file={handle:?}, level={level:?}");
        ErrCtx::wrap(move || handle.lock(level))
    }

    fn unlock(&self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        tracing::trace!("unlock: file={handle:?}, level={level:?}");
        ErrCtx::wrap(move || handle.unlock(level))
    }

    fn file_size(&self, handle: &mut Self::Handle) -> VfsResult<usize> {
        tracing::trace!("file_size: handle={handle:?}");
        ErrCtx::wrap(move || handle.file_size())
    }

    fn truncate(&self, handle: &mut Self::Handle, size: usize) -> VfsResult<()> {
        tracing::trace!("truncate: handle={handle:?}, size={size}");
        ErrCtx::wrap(move || handle.truncate(size))
    }

    fn write(&self, handle: &mut Self::Handle, offset: usize, data: &[u8]) -> VfsResult<usize> {
        tracing::trace!(
            "write: handle={handle:?}, offset={offset}, len={}",
            data.len()
        );
        ErrCtx::wrap(move || handle.write(offset, data))
    }

    fn read(&self, handle: &mut Self::Handle, offset: usize, data: &mut [u8]) -> VfsResult<usize> {
        tracing::trace!(
            "read: handle={handle:?}, offset={offset}, len={}",
            data.len()
        );
        ErrCtx::wrap(move || handle.read(offset, data))
    }
}
