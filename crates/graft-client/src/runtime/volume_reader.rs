use culprit::{Result, ResultExt};

use graft_core::{
    page::{Page, EMPTY_PAGE},
    page_offset::PageOffset,
};

use crate::ClientErr;

use super::{
    shared::Shared, snapshot::VolumeSnapshot, storage::page::PageValue, volume_writer::VolumeWriter,
};

#[derive(Clone, Debug)]
pub struct VolumeReader<F> {
    snapshot: VolumeSnapshot,
    shared: Shared<F>,
}

impl<F> VolumeReader<F> {
    pub(crate) fn new(snapshot: VolumeSnapshot, shared: Shared<F>) -> Self {
        Self { snapshot, shared }
    }

    /// Access this reader's snapshot
    #[inline]
    pub fn snapshot(&self) -> &VolumeSnapshot {
        &self.snapshot
    }

    /// Read a page from the snapshot
    pub fn read(&self, offset: PageOffset) -> Result<Page, ClientErr> {
        // TODO:
        // return None if offset is out of range OR we don't have a snapshot

        match self
            .shared
            .storage()
            .read(&self.snapshot.vid(), self.snapshot.local().lsn(), offset)
            .or_into_ctx()?
        {
            (_, PageValue::Available(page)) => Ok(page),
            (_, PageValue::Empty) => Ok(EMPTY_PAGE),
            (_, PageValue::Pending) => {
                if let Some(_remote) = self.snapshot().remote() {
                    // When this is fixed, update the test:
                    // graft-test/tests/sync.rs
                    todo!("download page from remote")
                } else {
                    Ok(EMPTY_PAGE)
                }
            }
        }
    }

    /// Upgrade this reader into a writer
    pub fn upgrade(self) -> VolumeWriter<F> {
        self.into()
    }

    /// decompose this reader into snapshot and storage
    pub(crate) fn into_parts(self) -> (VolumeSnapshot, Shared<F>) {
        (self.snapshot, self.shared)
    }
}

impl<F> Into<VolumeSnapshot> for VolumeReader<F> {
    fn into(self) -> VolumeSnapshot {
        self.snapshot
    }
}
