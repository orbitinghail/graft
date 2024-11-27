use sqlite_plugin::vfs::{Vfs, VfsHandle};

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

    fn open(
        &mut self,
        path: Option<String>,
        opts: sqlite_plugin::flags::OpenOpts,
    ) -> sqlite_plugin::vfs::VfsResult<Self::Handle> {
        todo!()
    }

    fn delete(&mut self, path: &str) -> sqlite_plugin::vfs::VfsResult<()> {
        todo!()
    }

    fn access(
        &mut self,
        path: &str,
        flags: sqlite_plugin::flags::AccessFlags,
    ) -> sqlite_plugin::vfs::VfsResult<bool> {
        todo!()
    }

    fn file_size(&mut self, handle: &mut Self::Handle) -> sqlite_plugin::vfs::VfsResult<usize> {
        todo!()
    }

    fn truncate(
        &mut self,
        handle: &mut Self::Handle,
        size: usize,
    ) -> sqlite_plugin::vfs::VfsResult<()> {
        todo!()
    }

    fn write(
        &mut self,
        handle: &mut Self::Handle,
        offset: usize,
        buf: &[u8],
    ) -> sqlite_plugin::vfs::VfsResult<usize> {
        todo!()
    }

    fn read(
        &mut self,
        handle: &mut Self::Handle,
        offset: usize,
        buf: &mut [u8],
    ) -> sqlite_plugin::vfs::VfsResult<usize> {
        todo!()
    }

    fn close(&mut self, handle: &mut Self::Handle) -> sqlite_plugin::vfs::VfsResult<()> {
        todo!()
    }
}
