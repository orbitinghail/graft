use std::{collections::HashMap, fmt::Debug, sync::Arc};

use culprit::{Culprit, ResultExt};
use graft_kernel::{
    GraftErr, VolumeErr,
    rt::runtime_handle::RuntimeHandle,
    volume_name::{VolumeName, VolumeNameErr},
};
use graft_tracing::TracingConsumer;
use parking_lot::Mutex;
use sqlite_plugin::{
    flags::{AccessFlags, LockLevel, OpenKind, OpenOpts},
    logger::{SqliteLogLevel, SqliteLogger},
    vars::{
        self, SQLITE_BUSY, SQLITE_BUSY_SNAPSHOT, SQLITE_CANTOPEN, SQLITE_INTERNAL, SQLITE_IOERR,
        SQLITE_NOTFOUND,
    },
    vfs::{Pragma, PragmaErr, SqliteErr, Vfs, VfsResult},
};
use thiserror::Error;

use crate::{
    file::{FileHandle, VfsFile, mem_file::MemFile, vol_file::VolFile},
    pragma::GraftPragma,
};

#[derive(Debug, Error)]
pub enum ErrCtx {
    #[error("Graft error: {0}")]
    Graft(#[from] GraftErr),

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

    #[error(transparent)]
    FmtErr(#[from] std::fmt::Error),
}

impl From<VolumeNameErr> for ErrCtx {
    #[inline]
    fn from(value: VolumeNameErr) -> Self {
        ErrCtx::Graft(value.into())
    }
}

impl ErrCtx {
    #[inline]
    fn wrap<T>(cb: impl FnOnce() -> culprit::Result<T, ErrCtx>) -> VfsResult<T> {
        match cb() {
            Ok(t) => Ok(t),
            Err(err) => {
                let code = err.ctx().sqlite_err();
                if code == SQLITE_INTERNAL {
                    tracing::error!("{}", err);
                }
                Err(code)
            }
        }
    }

    fn sqlite_err(&self) -> SqliteErr {
        match self {
            ErrCtx::UnknownPragma => SQLITE_NOTFOUND,
            ErrCtx::CantOpen => SQLITE_CANTOPEN,
            ErrCtx::Busy => SQLITE_BUSY,
            ErrCtx::BusySnapshot => SQLITE_BUSY_SNAPSHOT,
            ErrCtx::Graft(err) => Self::map_graft_err(err),
            _ => SQLITE_INTERNAL,
        }
    }

    fn map_graft_err(err: &GraftErr) -> SqliteErr {
        match err {
            GraftErr::Storage(_) => SQLITE_IOERR,
            GraftErr::Remote(_) => SQLITE_IOERR,
            GraftErr::Volume(err) => match err {
                VolumeErr::VolumeNotFound(_) | VolumeErr::NamedVolumeNotFound(_) => SQLITE_IOERR,
                VolumeErr::ConcurrentWrite(_) => SQLITE_BUSY_SNAPSHOT,
                VolumeErr::NamedVolumeNeedsRecovery(_)
                | VolumeErr::NamedVolumeDiverged(_)
                | VolumeErr::NamedVolumeRemoteMismatch { .. }
                | VolumeErr::InvalidVolumeName(_) => SQLITE_INTERNAL,
            },
        }
    }
}

impl<T> From<ErrCtx> for culprit::Result<T, ErrCtx> {
    fn from(err: ErrCtx) -> culprit::Result<T, ErrCtx> {
        Err(Culprit::new(err))
    }
}

pub struct GraftVfs {
    runtime: RuntimeHandle,
    locks: Mutex<HashMap<VolumeName, Arc<Mutex<()>>>>,
}

impl GraftVfs {
    pub fn new(runtime: RuntimeHandle) -> Self {
        Self { runtime, locks: Default::default() }
    }
}

impl Vfs for GraftVfs {
    type Handle = FileHandle;

    fn device_characteristics(&self) -> i32 {
        // writes up to a single page are atomic
        vars::SQLITE_IOCAP_ATOMIC512 |
        vars::SQLITE_IOCAP_ATOMIC1K |
        vars::SQLITE_IOCAP_ATOMIC2K |
        vars::SQLITE_IOCAP_ATOMIC4K |
        // after reboot following a crash or power loss, the only bytes in a file that were written
        // at the application level might have changed and that adjacent bytes, even bytes within
        // the same sector are guaranteed to be unchanged
        vars::SQLITE_IOCAP_POWERSAFE_OVERWRITE |
        // when data is appended to a file, the data is appended first then the size of the file is
        // extended, never the other way around
        vars::SQLITE_IOCAP_SAFE_APPEND |
        // information is written to disk in the same order as calls to xWrite()
        vars::SQLITE_IOCAP_SEQUENTIAL
    }

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
        graft_tracing::init_tracing_with_writer(TracingConsumer::Tool, make_writer);
    }

    fn pragma(
        &self,
        handle: &mut Self::Handle,
        pragma: Pragma<'_>,
    ) -> Result<Option<String>, PragmaErr> {
        tracing::trace!("pragma: file={handle:?}, pragma={pragma:?}");
        if let FileHandle::VolFile(file) = handle {
            match GraftPragma::try_from(&pragma)?.eval(&self.runtime, file) {
                Ok(val) => Ok(val),
                Err(err) => Err(PragmaErr::Fail(
                    err.ctx().sqlite_err(),
                    Some(format!("{err:?}")),
                )),
            }
        } else {
            Err(PragmaErr::NotFound)
        }
    }

    fn access(&self, path: &str, flags: AccessFlags) -> VfsResult<bool> {
        tracing::trace!("access: path={path:?}; flags={flags:?}");
        ErrCtx::wrap(move || self.runtime.volume_exists(path).or_into_ctx())
    }

    fn open(&self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Self::Handle> {
        tracing::trace!("open: path={path:?}, opts={opts:?}");
        ErrCtx::wrap(move || {
            // we only open a Volume for main database files
            if opts.kind() == OpenKind::MainDb
                && let Some(path) = path
            {
                // TODO: parse query string to see if a remote VID is requested
                let remote_vid = None;

                // try to open a volume handle
                let handle = self.runtime.open_volume(path, remote_vid).or_into_ctx()?;

                // get or create a reserved lock for this Volume
                let reserved_lock = self
                    .locks
                    .lock()
                    .entry(handle.name().clone())
                    .or_default()
                    .clone();

                return Ok(VolFile::new(handle, opts, reserved_lock).into());
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
                        // TODO: delete volume on close if requested
                        // TODO: do we want to actually delete volumes? or mark them for deletion?
                    }

                    // close and drop the vol_file
                    let handle = vol_file.close();

                    // retrieve a reference to the reserved lock for the volume
                    let mut locks = self.locks.lock();
                    let reserved_lock = locks
                        .get(handle.name())
                        .expect("reserved lock missing from lock manager");

                    // clean up the lock if this was the last reference
                    // SAFETY: we are holding a lock on the lock manager,
                    // preventing any concurrent opens from incrementing the
                    // reference count
                    if Arc::strong_count(reserved_lock) == 1 {
                        locks.remove(handle.name());
                    }

                    Ok(())
                }
            }
        })
    }

    fn delete(&self, path: &str) -> VfsResult<()> {
        tracing::trace!("delete: path={path:?}");
        ErrCtx::wrap(|| {
            // TODO: delete volume
            // TODO: do we want to actually delete volumes? or mark them for deletion?
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
