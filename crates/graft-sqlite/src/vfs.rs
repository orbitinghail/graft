use std::{borrow::Cow, collections::HashMap, fmt::Debug, sync::Arc};

use graft::{GraftErr, LogicalErr, rt::runtime::Runtime};
use parking_lot::Mutex;
use sqlite_plugin::{
    flags::{AccessFlags, CreateMode, LockLevel, OpenKind, OpenMode, OpenOpts},
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

    #[error("Pragma error: {0}")]
    PragmaErr(Cow<'static, str>),

    #[error("Tag not found")]
    TagNotFound,

    #[error("Transaction is busy")]
    Busy,

    #[error("The transaction snapshot is no longer current")]
    BusySnapshot,

    #[error("Invalid lock transition")]
    InvalidLockTransition,

    #[error("Invalid volume state")]
    InvalidVolumeState,

    #[error(transparent)]
    IoErr(#[from] std::io::Error),

    #[error(transparent)]
    FmtErr(#[from] std::fmt::Error),
}

impl ErrCtx {
    #[inline]
    fn wrap<T>(cb: impl FnOnce() -> Result<T, ErrCtx>) -> VfsResult<T> {
        match cb() {
            Ok(t) => Ok(t),
            Err(err) => Err(err.sqlite_err()),
        }
    }

    fn sqlite_err(&self) -> SqliteErr {
        match self {
            ErrCtx::UnknownPragma => SQLITE_NOTFOUND,
            ErrCtx::TagNotFound => SQLITE_CANTOPEN,
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
            GraftErr::Logical(err) => match err {
                LogicalErr::VolumeNotFound(_) => SQLITE_IOERR,
                LogicalErr::VolumeConcurrentWrite(_) => SQLITE_BUSY_SNAPSHOT,
                LogicalErr::VolumeNeedsRecovery(_)
                | LogicalErr::VolumeDiverged(_)
                | LogicalErr::VolumeRemoteMismatch { .. } => SQLITE_INTERNAL,
            },
        }
    }
}

pub struct GraftVfs {
    runtime: Runtime,
    // VolFile locks keyed by tag
    locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl GraftVfs {
    pub fn new(runtime: Runtime) -> Self {
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

    fn access(&self, path: &str, flags: AccessFlags) -> VfsResult<bool> {
        tracing::trace!("access: path={path:?}; flags={flags:?}");
        ErrCtx::wrap(move || Ok(self.runtime.tag_exists(path)?))
    }

    fn open(&self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Self::Handle> {
        tracing::trace!("open: path={path:?}, opts={opts:?}");
        ErrCtx::wrap(move || {
            // we only open a Volume for main database files
            if opts.kind() == OpenKind::MainDb
                && let Some(tag) = path
            {
                let can_create = matches!(
                    opts.mode(),
                    OpenMode::ReadWrite {
                        create: CreateMode::Create | CreateMode::MustCreate
                    }
                );

                let vid = if can_create {
                    // create the volume if needed
                    if let Some(vid) = self.runtime.tag_get(tag)? {
                        vid
                    } else {
                        let volume = self.runtime.volume_open(None, None, None)?;
                        self.runtime.tag_replace(tag, volume.vid.clone())?;
                        volume.vid
                    }
                } else {
                    // just get the existing volume
                    self.runtime.tag_get(tag)?.ok_or(ErrCtx::TagNotFound)?
                };

                // get or create a reserved lock for this Volume
                let reserved_lock = self.locks.lock().entry(tag.to_owned()).or_default().clone();

                return Ok(VolFile::new(
                    self.runtime.clone(),
                    tag.to_owned(),
                    vid,
                    opts,
                    reserved_lock,
                )
                .into());
            }

            // all other files use in-memory storage
            Ok(MemFile::default().into())
        })
    }

    fn delete(&self, path: &str) -> VfsResult<()> {
        // nothing to do, SQLite only calls xDelete on secondary
        // files, which in this VFS are in-memory and delete on close
        tracing::trace!("delete: path={path:?}");
        Ok(())
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

                    // retrieve a reference to the reserved lock for the volume
                    let mut locks = self.locks.lock();
                    let reserved_lock = locks
                        .get(&vol_file.tag)
                        .expect("reserved lock missing from lock manager");

                    // clean up the lock if this was the last reference
                    // SAFETY: we are holding a lock on the lock manager,
                    // preventing any concurrent opens from incrementing the
                    // reference count
                    if Arc::strong_count(reserved_lock) == 1 {
                        locks.remove(&vol_file.tag);
                    }

                    Ok(())
                }
            }
        })
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
                Err(err) => Err(PragmaErr::Fail(err.sqlite_err(), Some(format!("{err}")))),
            }
        } else {
            Err(PragmaErr::NotFound)
        }
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
