use culprit::{Result, ResultExt};
use graft_core::{page::Page, page_count::PageCount, page_offset::PageOffset, VolumeId};

use crate::ClientErr;

use super::{
    fetcher::Fetcher,
    storage::{memtable::Memtable, snapshot::Snapshot},
    volume_reader::{VolumeRead, VolumeReader},
};

pub trait VolumeWrite {
    type CommitOutput;

    /// Write a page
    fn write(&mut self, offset: impl Into<PageOffset>, page: Page);

    /// Truncate the volume to a new page count.
    /// This can be used to increase or decrease the Volume's size.
    fn truncate(&mut self, pages: impl Into<PageCount>);

    /// Commit the transaction
    fn commit(self) -> Result<Self::CommitOutput, ClientErr>;
}

#[derive(Debug)]
pub struct VolumeWriter<F> {
    pages: PageCount,
    reader: VolumeReader<F>,
    memtable: Memtable,
}

impl<F: Fetcher> VolumeWriter<F> {
    pub fn pages(&self) -> PageCount {
        self.pages
    }
}

impl<F: Fetcher> From<VolumeReader<F>> for VolumeWriter<F> {
    fn from(reader: VolumeReader<F>) -> Self {
        let pages = reader.snapshot().map_or(PageCount::ZERO, |s| s.pages());
        Self {
            pages,
            reader,
            memtable: Default::default(),
        }
    }
}

impl<F: Fetcher> VolumeRead for VolumeWriter<F> {
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
    fn read(&self, offset: impl Into<PageOffset>) -> Result<Page, ClientErr> {
        let offset = offset.into();
        if let Some(page) = self.memtable.get(offset) {
            return Ok(page.clone());
        }
        self.reader.read(offset)
    }
}

impl<F: Fetcher> VolumeWrite for VolumeWriter<F> {
    type CommitOutput = VolumeReader<F>;

    fn write(&mut self, offset: impl Into<PageOffset>, page: Page) {
        let offset = offset.into();
        self.pages = self.pages.max(offset.pages());
        self.memtable.insert(offset, page);
    }

    fn truncate(&mut self, pages: impl Into<PageCount>) {
        self.pages = pages.into();
        self.memtable.truncate(self.pages.last_offset())
    }

    fn commit(self) -> Result<VolumeReader<F>, ClientErr> {
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
