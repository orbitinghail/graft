use culprit::{Result, ResultExt};
use graft_core::{page::Page, page_offset::PageOffset};

use crate::ClientErr;

use super::{snapshot::VolumeSnapshot, storage::memtable::Memtable, volume_reader::VolumeReader};

#[derive(Debug)]
pub struct VolumeWriter {
    reader: VolumeReader,
    memtable: Memtable,
}

impl From<VolumeReader> for VolumeWriter {
    fn from(reader: VolumeReader) -> Self {
        Self { reader, memtable: Default::default() }
    }
}

impl VolumeWriter {
    /// Access this writer's snapshot
    #[inline]
    pub fn snapshot(&self) -> &VolumeSnapshot {
        self.reader.snapshot()
    }

    /// Read a page; supports read your own writes (RYOW)
    pub fn read(&self, offset: PageOffset) -> Result<Page, ClientErr> {
        if let Some(page) = self.memtable.get(offset) {
            return Ok(page.clone());
        }
        self.reader.read(offset)
    }

    /// Write a page
    pub fn write(&mut self, offset: PageOffset, page: Page) {
        self.memtable.insert(offset, page);
    }

    /// Commit the transaction
    pub fn commit(self) -> Result<VolumeReader, ClientErr> {
        let (snapshot, storage) = self.reader.into_parts();
        let local = storage
            .commit(
                snapshot.vid(),
                Some(snapshot.local().clone()),
                self.memtable,
            )
            .or_into_ctx()?;
        Ok(VolumeReader::new(snapshot.with_local(local), storage))
    }
}
