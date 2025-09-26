use culprit::Culprit;
use graft_core::{PageCount, PageIdx, page::Page};

use crate::{
    local::fjall_storage::FjallStorageErr, rt::runtime_handle::RuntimeHandle, snapshot::Snapshot,
    volume_writer::VolumeWriter,
};

/// A type which can read from a Volume
pub trait VolumeRead {
    fn page_count(&self) -> culprit::Result<PageCount, FjallStorageErr>;
    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, FjallStorageErr>;
}

#[derive(Debug, Clone)]
pub struct VolumeReader {
    runtime: RuntimeHandle,
    snapshot: Snapshot,
}

impl VolumeReader {
    pub(crate) fn new(runtime: RuntimeHandle, snapshot: Snapshot) -> Self {
        Self { runtime, snapshot }
    }
}

impl TryFrom<VolumeReader> for VolumeWriter {
    type Error = Culprit<FjallStorageErr>;

    fn try_from(reader: VolumeReader) -> Result<Self, Self::Error> {
        let page_count = reader.page_count()?;
        Ok(Self::new(reader.runtime, reader.snapshot, page_count))
    }
}

impl VolumeRead for VolumeReader {
    fn page_count(&self) -> culprit::Result<PageCount, FjallStorageErr> {
        self.runtime.storage().read().page_count(&self.snapshot)
    }

    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, FjallStorageErr> {
        self.runtime.read_page(&self.snapshot, pageidx)
    }
}
