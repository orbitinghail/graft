use culprit::Result;
use graft_client::runtime::{fetcher::NetFetcher, volume::VolumeHandle};
use sqlite_plugin::flags::{LockLevel, OpenOpts};

use crate::vfs::ErrCtx;

use super::VfsFile;

#[derive(Debug)]
pub struct VolFile {
    handle: VolumeHandle<NetFetcher>,
    opts: OpenOpts,
}

impl VolFile {
    pub fn new(handle: VolumeHandle<NetFetcher>, opts: OpenOpts) -> Self {
        Self { handle, opts }
    }

    pub fn handle(&self) -> &VolumeHandle<NetFetcher> {
        &self.handle
    }

    pub fn opts(&self) -> OpenOpts {
        self.opts
    }
}

impl VfsFile for VolFile {
    fn readonly(&self) -> bool {
        false
    }

    fn in_memory(&self) -> bool {
        false
    }

    fn lock(&mut self, level: LockLevel) -> Result<(), ErrCtx> {
        todo!()
    }

    fn unlock(&mut self, level: LockLevel) -> Result<(), ErrCtx> {
        todo!()
    }

    fn file_size(&mut self) -> Result<usize, ErrCtx> {
        todo!()
    }

    fn truncate(&mut self, size: usize) -> Result<(), ErrCtx> {
        todo!()
    }

    fn write(&mut self, offset: usize, data: &[u8]) -> Result<usize, ErrCtx> {
        todo!()
    }

    fn read(&mut self, offset: usize, data: &mut [u8]) -> Result<usize, ErrCtx> {
        todo!()
    }
}
