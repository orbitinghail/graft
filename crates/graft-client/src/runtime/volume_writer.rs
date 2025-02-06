use culprit::{Result, ResultExt};
use graft_core::{page::Page, page_offset::PageOffset, VolumeId};

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

    /// Commit the transaction
    fn commit(self) -> Result<Self::CommitOutput, ClientErr>;
}

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
        self.memtable.insert(offset.into(), page);
    }

    fn commit(self) -> Result<VolumeReader<F>, ClientErr> {
        let (vid, snapshot, shared) = self.reader.into_parts();
        let snapshot = shared
            .storage()
            .commit(&vid, snapshot, self.memtable)
            .or_into_ctx()?;
        Ok(VolumeReader::new(vid, Some(snapshot), shared))
    }
}
