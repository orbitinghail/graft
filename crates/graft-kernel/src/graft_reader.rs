use culprit::Culprit;
use graft_core::{PageCount, PageIdx, VolumeId, page::Page};

use crate::{KernelErr, graft_writer::GraftWriter, rt::runtime::Runtime, snapshot::Snapshot};

/// A type which can read from a Graft
pub trait GraftRead {
    fn snapshot(&self) -> &Snapshot;
    fn page_count(&self) -> culprit::Result<PageCount, KernelErr>;
    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, KernelErr>;
}

#[derive(Debug, Clone)]
pub struct GraftReader {
    runtime: Runtime,
    graft: VolumeId,
    snapshot: Snapshot,
}

impl GraftReader {
    pub(crate) fn new(runtime: Runtime, graft: VolumeId, snapshot: Snapshot) -> Self {
        Self { runtime, graft, snapshot }
    }
}

impl TryFrom<GraftReader> for GraftWriter {
    type Error = Culprit<KernelErr>;

    fn try_from(reader: GraftReader) -> Result<Self, Self::Error> {
        let page_count = reader.page_count()?;
        Ok(Self::new(
            reader.runtime,
            reader.graft,
            reader.snapshot,
            page_count,
        ))
    }
}

impl GraftRead for GraftReader {
    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    fn page_count(&self) -> culprit::Result<PageCount, KernelErr> {
        self.runtime.snapshot_pages(&self.snapshot)
    }

    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, KernelErr> {
        self.runtime.read_page(&self.snapshot, pageidx)
    }
}
