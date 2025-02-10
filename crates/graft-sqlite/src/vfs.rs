// TODO: remove this once the vfs is implemented
#![allow(unused)]

use sqlite_plugin::{
    flags::{AccessFlags, OpenOpts},
    logger::SqliteLogger,
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
        todo!()
    }

    fn open(&mut self, path: Option<String>, opts: OpenOpts) -> VfsResult<Self::Handle> {
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
