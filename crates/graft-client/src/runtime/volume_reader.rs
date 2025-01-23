use culprit::{Result, ResultExt};

use graft_core::{
    page::{Page, EMPTY_PAGE},
    page_offset::PageOffset,
    VolumeId,
};

use crate::ClientErr;

use super::{
    fetcher::Fetcher,
    shared::Shared,
    storage::{page::PageValue, snapshot::Snapshot},
    volume_writer::VolumeWriter,
};

#[derive(Clone, Debug)]
pub struct VolumeReader<F> {
    vid: VolumeId,
    snapshot: Snapshot,
    shared: Shared<F>,
}

impl<F: Fetcher> VolumeReader<F> {
    pub(crate) fn new(vid: VolumeId, snapshot: Snapshot, shared: Shared<F>) -> Self {
        Self { vid, snapshot, shared }
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    /// Access this reader's snapshot
    #[inline]
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Read a page from the snapshot
    pub fn read(&self, offset: PageOffset) -> Result<Page, ClientErr> {
        match self
            .shared
            .storage()
            .read(self.vid(), self.snapshot.local(), offset)
            .or_into_ctx()?
        {
            (_, PageValue::Available(page)) => Ok(page),
            (_, PageValue::Empty) => Ok(EMPTY_PAGE),
            (_, PageValue::Pending) => self.shared.fetcher().fetch_page(
                self.shared.storage(),
                self.vid(),
                self.snapshot(),
                offset,
            ),
        }
    }

    /// Upgrade this reader into a writer
    pub fn upgrade(self) -> VolumeWriter<F> {
        self.into()
    }

    /// decompose this reader into snapshot and storage
    pub(crate) fn into_parts(self) -> (VolumeId, Snapshot, Shared<F>) {
        (self.vid, self.snapshot, self.shared)
    }
}
