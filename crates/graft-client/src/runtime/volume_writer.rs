use culprit::{Result, ResultExt};
use graft_core::{page::Page, page_count::PageCount, PageIdx, VolumeId};

use crate::ClientErr;

use super::{
    storage::{memtable::Memtable, snapshot::Snapshot},
    volume_reader::{VolumeRead, VolumeReader},
};

pub trait VolumeWrite {
    type CommitOutput;

    /// Write a page
    fn write(&mut self, offset: PageIdx, page: Page);

    /// Truncate the volume to a new page count.
    /// This can be used to increase or decrease the Volume's size.
    fn truncate(&mut self, pages: PageCount);

    /// Commit the transaction
    fn commit(self) -> Result<Self::CommitOutput, ClientErr>;
}

#[derive(Debug)]
pub struct VolumeWriter {
    pages: PageCount,
    reader: VolumeReader,
    memtable: Memtable,
}

impl VolumeWriter {
    pub fn pages(&self) -> PageCount {
        self.pages
    }
}

impl From<VolumeReader> for VolumeWriter {
    fn from(reader: VolumeReader) -> Self {
        let pages = reader.snapshot().map_or(PageCount::ZERO, |s| s.pages());
        Self {
            pages,
            reader,
            memtable: Default::default(),
        }
    }
}

impl VolumeRead for VolumeWriter {
    #[inline]
    fn vid(&self) -> &VolumeId {
        self.reader.vid()
    }

    /// Access this writer's snapshot
    #[inline]
    fn snapshot(&self) -> Option<&Snapshot> {
        self.reader.snapshot()
    }

    /// Read a page; supports read your own writes (RYOW)
    fn read(&self, offset: PageIdx) -> Result<Page, ClientErr> {
        let offset = offset.into();
        if let Some(page) = self.memtable.get(offset) {
            return Ok(page.clone());
        }
        self.reader.read(offset)
    }
}

impl VolumeWrite for VolumeWriter {
    type CommitOutput = VolumeReader;

    fn write(&mut self, offset: PageIdx, page: Page) {
        self.pages = self.pages.max(offset.pages());
        self.memtable.insert(offset, page);
    }

    fn truncate(&mut self, pages: PageCount) {
        self.pages = pages;
        self.memtable.truncate(self.pages.last_index())
    }

    fn commit(self) -> Result<VolumeReader, ClientErr> {
        let (vid, snapshot, shared) = self.reader.into_parts();

        // we have nothing to commit if the page count is equal to the snapshot
        // pagecount *and* the memtable is empty
        let snapshot_pagecount = snapshot.as_ref().map_or(PageCount::ZERO, |s| s.pages());
        let memtable_empty = self.memtable.is_empty();
        if self.pages == snapshot_pagecount && memtable_empty {
            return Ok(VolumeReader::new(vid, snapshot, shared));
        }

        let snapshot = shared
            .storage()
            .commit(&vid, snapshot, self.pages, self.memtable)
            .or_into_ctx()?;
        Ok(VolumeReader::new(vid, Some(snapshot), shared))
    }
}
