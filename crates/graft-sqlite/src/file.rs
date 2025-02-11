use std::fmt::Debug;

use culprit::Result;
use enum_dispatch::enum_dispatch;
use mem::MemFile;
use sqlite_plugin::{flags::LockLevel, vfs::VfsHandle};
use volume::VolFile;

use crate::vfs::ErrCtx;

pub mod mem;
pub mod volume;

#[enum_dispatch]
pub trait VfsFile: Debug + Clone {
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
#[derive(Debug, Clone)]
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
