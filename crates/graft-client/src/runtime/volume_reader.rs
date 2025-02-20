use culprit::{Result, ResultExt};

use graft_core::{
    page::{Page, EMPTY_PAGE},
    PageIdx, VolumeId,
};

use crate::ClientErr;

use super::{
    shared::Shared,
    storage::{page::PageValue, snapshot::Snapshot},
    volume_writer::VolumeWriter,
};

pub trait VolumeRead {
    fn vid(&self) -> &VolumeId;

    /// Retrieve the Volume snapshot backing this reader
    fn snapshot(&self) -> Option<&Snapshot>;

    /// Read a page from the snapshot
    fn read(&self, offset: PageIdx) -> Result<Page, ClientErr>;
}

#[derive(Debug, Clone)]
pub struct VolumeReader {
    vid: VolumeId,
    snapshot: Option<Snapshot>,
    shared: Shared,
}

impl VolumeReader {
    pub(crate) fn new(vid: VolumeId, snapshot: Option<Snapshot>, shared: Shared) -> Self {
        Self { vid, snapshot, shared }
    }

    /// Upgrade this reader into a writer
    pub fn upgrade(self) -> VolumeWriter {
        self.into()
    }

    /// decompose this reader into snapshot and storage
    pub(crate) fn into_parts(self) -> (VolumeId, Option<Snapshot>, Shared) {
        (self.vid, self.snapshot, self.shared)
    }
}

impl VolumeRead for VolumeReader {
    #[inline]
    fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    fn read(&self, offset: PageIdx) -> Result<Page, ClientErr> {
        let offset = offset.into();
        if let Some(snapshot) = self.snapshot() {
            match self
                .shared
                .storage()
                .read(self.vid(), snapshot.local(), offset)
                .or_into_ctx()?
            {
                (_, PageValue::Available(page)) => Ok(page),
                (_, PageValue::Empty) => Ok(EMPTY_PAGE),
                (local_lsn, PageValue::Pending) => {
                    if let Some(remote_lsn) = snapshot.remote() {
                        self.shared
                            .fetcher()
                            .fetch_page(
                                self.shared.storage(),
                                self.vid(),
                                remote_lsn,
                                local_lsn,
                                offset,
                            )
                            .or_into_ctx()
                    } else {
                        Ok(EMPTY_PAGE)
                    }
                }
            }
        } else {
            Ok(EMPTY_PAGE)
        }
    }
}
