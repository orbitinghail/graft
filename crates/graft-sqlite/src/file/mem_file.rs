use std::fmt::Debug;

use culprit::Result;
use sqlite_plugin::flags::LockLevel;

use crate::vfs::ErrCtx;

use super::VfsFile;

#[derive(Default)]
pub struct MemFile {
    data: Vec<u8>,
}

impl Debug for MemFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemFile")
            .field("len", &self.data.len())
            .finish()
    }
}

impl VfsFile for MemFile {
    fn readonly(&self) -> bool {
        false
    }

    fn in_memory(&self) -> bool {
        true
    }

    fn lock(&mut self, _level: LockLevel) -> Result<(), ErrCtx> {
        Ok(())
    }

    fn unlock(&mut self, _level: LockLevel) -> Result<(), ErrCtx> {
        Ok(())
    }

    fn file_size(&mut self) -> Result<usize, ErrCtx> {
        Ok(self.data.len())
    }

    fn truncate(&mut self, size: usize) -> Result<(), ErrCtx> {
        self.data.truncate(size);
        Ok(())
    }

    fn write(&mut self, offset: usize, data: &[u8]) -> Result<usize, ErrCtx> {
        if offset + data.len() > self.data.len() {
            self.data.resize(offset + data.len(), 0);
        }
        self.data[offset..offset + data.len()].copy_from_slice(data);
        Ok(data.len())
    }

    fn read(&mut self, offset: usize, data: &mut [u8]) -> Result<usize, ErrCtx> {
        let start = offset.min(self.data.len());
        let end = (offset + data.len()).min(self.data.len());
        let len = end - start;
        data[0..len].copy_from_slice(&self.data[start..end]);
        Ok(end - start)
    }
}
