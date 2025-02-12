// TODO: remove this once the vfs is implemented
#![allow(unused)]

use std::collections::{HashMap, HashSet};

use culprit::{Culprit, ResultExt};
use graft_client::{
    runtime::{
        fetcher::NetFetcher,
        runtime::Runtime,
        storage::volume_state::{SyncDirection, VolumeConfig},
    },
    ClientErr,
};
use graft_core::{gid::GidParseErr, VolumeId};
use graft_tracing::TracingConsumer;
use sqlite_plugin::{
    flags::{AccessFlags, LockLevel, OpenKind, OpenOpts},
    logger::{SqliteLogLevel, SqliteLogger},
    vars::{self, SQLITE_CANTOPEN, SQLITE_INTERNAL, SQLITE_NOTFOUND},
    vfs::{Pragma, Vfs, VfsHandle, VfsResult},
};
use thiserror::Error;
use tryiter::TryIteratorExt;

use crate::{
    file::{mem_file::MemFile, vol_file::VolFile, FileHandle, VfsFile},
    pragma::{GraftPragma, PragmaParseErr},
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
    runtime: Runtime<NetFetcher>,
}

impl GraftVfs {
    pub fn new(runtime: Runtime<NetFetcher>) -> Self {
        Self { runtime }
    }
}

impl Vfs for GraftVfs {
    type Handle = FileHandle;

    fn register_logger(&mut self, logger: SqliteLogger) {
        struct Writer(SqliteLogger);
        impl std::io::Write for Writer {
            fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
                self.0.log(SqliteLogLevel::Notice, data);
                Ok(data.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let make_writer = move || Writer(logger);
        graft_tracing::init_tracing_with_writer(TracingConsumer::Tool, None, make_writer);
    }

    fn pragma(
        &mut self,
        handle: &mut Self::Handle,
        pragma: Pragma<'_>,
    ) -> VfsResult<Option<String>> {
        ErrCtx::wrap(move || {
            tracing::trace!("pragma: file={handle:?}, pragma={pragma:?}");

            if let FileHandle::VolFile(file) = handle {
                return match GraftPragma::try_from(pragma) {
                    Ok(pragma) => pragma.eval(&self.runtime, file),
                    Err(PragmaParseErr::Invalid(pragma)) => {
                        Ok(Some(format!("invalid pragma: {}", pragma.name)))
                    }
                    Err(PragmaParseErr::Unknown(pragma)) => {
                        Err(Culprit::new(ErrCtx::UnknownPragma))
                    }
                };
            }

            Err(Culprit::new(ErrCtx::UnknownPragma))
        })
    }

    fn access(&mut self, path: &str, flags: AccessFlags) -> VfsResult<bool> {
        ErrCtx::wrap(move || {
            tracing::trace!("access: path={path:?}; flags={flags:?}");
            if let Some(vid) = path.parse::<VolumeId>().ok() {
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

    fn open(&mut self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Self::Handle> {
        ErrCtx::wrap(move || {
            tracing::trace!("open: path={path:?}, opts={opts:?}");
            // we only open a Volume for main database files named after a Volume ID
            if opts.kind() == OpenKind::MainDb {
                if let Some(path) = path {
                    let vid: VolumeId = path.parse()?;

                    let handle = self
                        .runtime
                        .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
                        .or_into_ctx()?;
                    return Ok(VolFile::new(handle, opts).into());
                }
            }

            // all other files use in-memory storage
            Ok(MemFile::default().into())
        })
    }

    fn close(&mut self, handle: Self::Handle) -> VfsResult<()> {
        ErrCtx::wrap(move || {
            tracing::trace!("close: file={handle:?}");
            match &handle {
                FileHandle::MemFile(_) => Ok(()),
                FileHandle::VolFile(vol_file) => {
                    if vol_file.opts().delete_on_close() {
                        self.runtime
                            .update_volume_config(vol_file.handle().vid(), |conf| {
                                conf.with_sync(SyncDirection::Disabled)
                            })
                            .or_into_ctx()?;
                    }
                    Ok(())
                }
            }
        })
    }

    fn delete(&mut self, path: &str) -> VfsResult<()> {
        ErrCtx::wrap(|| {
            tracing::trace!("delete: path={path:?}");
            if let Some(vid) = path.parse().ok() {
                self.runtime
                    .update_volume_config(&vid, |conf| conf.with_sync(SyncDirection::Disabled))
                    .or_into_ctx()?;
            }
            Ok(())
        })
    }

    fn lock(&mut self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        ErrCtx::wrap(move || handle.lock(level))
    }

    fn unlock(&mut self, handle: &mut Self::Handle, level: LockLevel) -> VfsResult<()> {
        ErrCtx::wrap(move || handle.unlock(level))
    }

    fn file_size(&mut self, handle: &mut Self::Handle) -> VfsResult<usize> {
        ErrCtx::wrap(move || handle.file_size())
    }

    fn truncate(&mut self, handle: &mut Self::Handle, size: usize) -> VfsResult<()> {
        ErrCtx::wrap(move || handle.truncate(size))
    }

    fn write(&mut self, handle: &mut Self::Handle, offset: usize, data: &[u8]) -> VfsResult<usize> {
        ErrCtx::wrap(move || handle.write(offset, data))
    }

    fn read(
        &mut self,
        handle: &mut Self::Handle,
        offset: usize,
        data: &mut [u8],
    ) -> VfsResult<usize> {
        ErrCtx::wrap(move || handle.read(offset, data))
    }
}
