use culprit::{Culprit, ResultExt};
use graft_core::{PageCount, PageIdx, VolumeId, page::Page};

use crate::{
    KernelErr, rt::runtime_handle::RuntimeHandle, snapshot::Snapshot, volume_writer::VolumeWriter,
};

/// A type which can read from a Volume
pub trait VolumeRead {
    fn snapshot(&self) -> &Snapshot;
    fn page_count(&self) -> culprit::Result<PageCount, KernelErr>;
    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, KernelErr>;
}

#[derive(Debug, Clone)]
pub struct VolumeReader {
    runtime: RuntimeHandle,
    graft: VolumeId,
    snapshot: Snapshot,
}

impl VolumeReader {
    pub(crate) fn new(runtime: RuntimeHandle, graft: VolumeId, snapshot: Snapshot) -> Self {
        Self { runtime, graft, snapshot }
    }
}

impl TryFrom<VolumeReader> for VolumeWriter {
    type Error = Culprit<KernelErr>;

    fn try_from(reader: VolumeReader) -> Result<Self, Self::Error> {
        let page_count = reader.page_count()?;
        Ok(Self::new(
            reader.runtime,
            reader.graft,
            reader.snapshot,
            page_count,
        ))
    }
}

impl VolumeRead for VolumeReader {
    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    fn page_count(&self) -> culprit::Result<PageCount, KernelErr> {
        if let Some((vid, lsn)) = self.snapshot.head() {
            Ok(self
                .runtime
                .storage()
                .read()
                .page_count(vid, lsn)
                .or_into_ctx()?
                .expect("BUG: missing page count for snapshot"))
        } else {
            Ok(PageCount::ZERO)
        }
    }

    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, KernelErr> {
        self.runtime.read_page(&self.snapshot, pageidx)
    }
}
