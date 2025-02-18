use std::fmt::Debug;

use culprit::Result;
use enum_dispatch::enum_dispatch;
use mem_file::MemFile;
use sqlite_plugin::{flags::LockLevel, vfs::VfsHandle};
use vol_file::VolFile;

use crate::vfs::ErrCtx;

pub mod mem_file;
pub mod vol_file;

#[enum_dispatch]
pub trait VfsFile: Debug {
    fn readonly(&self) -> bool;
    fn in_memory(&self) -> bool;

    fn lock(&mut self, level: LockLevel) -> Result<(), ErrCtx>;
    fn unlock(&mut self, level: LockLevel) -> Result<(), ErrCtx>;

    fn file_size(&mut self) -> Result<usize, ErrCtx>;
    fn truncate(&mut self, size: usize) -> Result<(), ErrCtx>;

    fn write(&mut self, offset: usize, data: &[u8]) -> Result<usize, ErrCtx>;
    fn read(&mut self, offset: usize, data: &mut [u8]) -> Result<usize, ErrCtx>;
}

#[enum_dispatch(VfsFile)]
#[derive(Debug)]
pub enum FileHandle {
    MemFile,
    VolFile,
}

impl VfsHandle for FileHandle {
    fn readonly(&self) -> bool {
        VfsFile::readonly(self)
    }

    fn in_memory(&self) -> bool {
        VfsFile::in_memory(self)
    }
}
