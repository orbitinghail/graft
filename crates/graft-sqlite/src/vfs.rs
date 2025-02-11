// TODO: remove this once the vfs is implemented
#![allow(unused)]

use graft_tracing::TracingConsumer;
use sqlite_plugin::{
    flags::{AccessFlags, OpenOpts},
    logger::{SqliteLogLevel, SqliteLogger},
    vfs::{Vfs, VfsHandle, VfsResult},
};

pub struct FileHandle {}

impl VfsHandle for FileHandle {
    fn readonly(&self) -> bool {
        false
    }

    fn in_memory(&self) -> bool {
        true
    }
}

pub struct GraftVfs {}

impl GraftVfs {
    pub fn new() -> Self {
        Self {}
    }
}

impl Vfs for GraftVfs {
    type Handle = FileHandle;

    fn register_logger(&mut self, logger: SqliteLogger) {
        struct Writer(SqliteLogger);
        impl std::io::Write for Writer {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.log(SqliteLogLevel::Notice, buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let make_writer = move || Writer(logger);
        graft_tracing::init_tracing_with_writer(TracingConsumer::Tool, None, make_writer);
    }

    fn open(&mut self, path: Option<&str>, opts: OpenOpts) -> VfsResult<Self::Handle> {
        tracing::trace!("open received path {:?}", path);
        todo!()
    }

    fn delete(&mut self, path: &str) -> VfsResult<()> {
        todo!()
    }

    fn access(&mut self, path: &str, flags: AccessFlags) -> VfsResult<bool> {
        todo!()
    }

    fn file_size(&mut self, handle: &mut Self::Handle) -> VfsResult<usize> {
        todo!()
    }

    fn truncate(&mut self, handle: &mut Self::Handle, size: usize) -> VfsResult<()> {
        todo!()
    }

    fn write(&mut self, handle: &mut Self::Handle, offset: usize, buf: &[u8]) -> VfsResult<usize> {
        todo!()
    }

    fn read(
        &mut self,
        handle: &mut Self::Handle,
        offset: usize,
        buf: &mut [u8],
    ) -> VfsResult<usize> {
        todo!()
    }

    fn close(&mut self, handle: &mut Self::Handle) -> VfsResult<()> {
        todo!()
    }
}
