// TODO: remove this once the vfs is implemented
#![allow(unused)]

use std::collections::HashMap;

use culprit::ResultExt;
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
    vars::{self, SQLITE_INTERNAL, SQLITE_NOTFOUND},
    vfs::{Pragma, Vfs, VfsHandle, VfsResult},
};
use thiserror::Error;

use crate::file::{mem::MemFile, volume::VolFile, FileHandle, VfsFile};

#[derive(Debug, Error)]
pub enum ErrCtx {
    #[error("Graft client error: {0}")]
    Client(#[from] ClientErr),

    #[error("Failed to parse VolumeId: {0}")]
    GidParseErr(#[from] GidParseErr),
}

impl ErrCtx {
    fn wrap<T>(mut cb: impl FnMut() -> culprit::Result<T, ErrCtx>) -> VfsResult<T> {
        match cb() {
            Ok(t) => Ok(t),
            Err(err) => {
                tracing::error!("{}", err);
                let code = match err {
                    _ => SQLITE_INTERNAL,
                };
                Err(code)
            }
        }
    }
}

pub struct GraftVfs {
    runtime: Runtime<NetFetcher>,
    files: HashMap<VolumeId, VolFile>,
}

impl GraftVfs {
    pub fn new(runtime: Runtime<NetFetcher>) -> Self {
        Self { runtime, files: HashMap::new() }
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
        Err(SQLITE_NOTFOUND)
    }

    fn access(&mut self, path: &str, flags: AccessFlags) -> VfsResult<bool> {
        ErrCtx::wrap(|| {
            tracing::trace!("access: path={path:?}; flags={flags:?}");
            if let Some(vid) = path.parse().ok() {
                Ok(self.files.contains_key(&vid))
            } else {
                Ok(false)
            }
        })
    }

    fn open(&mut self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Self::Handle> {
        ErrCtx::wrap(|| {
            // we only open a Graft for main database files that have a valid path
            // if opts.kind() == OpenKind::MainDb {
            //     if let Some(path) = path {
            //         let vid: VolumeId = path.parse()?;
            //         let handle = self
            //             .runtime
            //             .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
            //             .or_into_ctx()?;
            //         return Ok(VolFile::new(handle, opts).into());
            //     }
            // }

            // all other files use in-memory storage
            Ok(MemFile::default().into())
        })
    }

    fn close(&mut self, handle: &mut Self::Handle) -> VfsResult<()> {
        ErrCtx::wrap(move || {
            tracing::trace!("close: file={handle:?}");
            match handle {
                FileHandle::MemFile(_) => Ok(()),
                FileHandle::VolFile(vol_file) => {
                    self.files.remove(vol_file.handle().vid());
                    if vol_file.opts().delete_on_close() {
                        // TODO: what happens when we delete a volume?
                    }
                    Ok(())
                }
            }
        })
    }

    fn delete(&mut self, path: &str) -> VfsResult<()> {
        ErrCtx::wrap(|| {
            if let Some(vid) = path.parse().ok() {
                self.files.remove(&vid);
                // TODO: what happens when we delete a volume?
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
