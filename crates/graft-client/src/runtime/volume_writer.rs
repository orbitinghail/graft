use culprit::{Result, ResultExt};
use graft_core::{page::Page, page_offset::PageOffset, VolumeId};

use crate::ClientErr;

use super::{
    fetcher::Fetcher,
    storage::{memtable::Memtable, snapshot::Snapshot},
    volume_reader::VolumeReader,
};

#[derive(Debug)]
pub struct VolumeWriter<F> {
    reader: VolumeReader<F>,
    memtable: Memtable,
}

impl<F> From<VolumeReader<F>> for VolumeWriter<F> {
    fn from(reader: VolumeReader<F>) -> Self {
        Self { reader, memtable: Default::default() }
    }
}

impl<F: Fetcher> VolumeWriter<F> {
    #[inline]
    pub fn vid(&self) -> &VolumeId {
        self.reader.vid()
    }

    /// Access this writer's snapshot
    #[inline]
    pub fn snapshot(&self) -> Option<&Snapshot> {
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
    pub fn commit(self) -> Result<VolumeReader<F>, ClientErr> {
        let (vid, snapshot, shared) = self.reader.into_parts();
        let snapshot = shared
            .storage()
            .commit(&vid, snapshot, self.memtable)
            .or_into_ctx()?;
        Ok(VolumeReader::new(vid, Some(snapshot), shared))
    }
}
