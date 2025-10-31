use culprit::{Culprit, ResultExt};
use graft_core::{PageCount, PageIdx, page::Page};

use crate::{
    GraftErr, rt::runtime_handle::RuntimeHandle, snapshot::Snapshot, volume_name::VolumeName,
    volume_writer::VolumeWriter,
};

/// A type which can read from a Volume
pub trait VolumeRead {
    fn page_count(&self) -> culprit::Result<PageCount, GraftErr>;
    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, GraftErr>;
}

#[derive(Debug, Clone)]
pub struct VolumeReader {
    name: VolumeName,
    runtime: RuntimeHandle,
    snapshot: Snapshot,
}

impl VolumeReader {
    pub(crate) fn new(name: VolumeName, runtime: RuntimeHandle, snapshot: Snapshot) -> Self {
        Self { name, runtime, snapshot }
    }
}

impl TryFrom<VolumeReader> for VolumeWriter {
    type Error = Culprit<GraftErr>;

    fn try_from(reader: VolumeReader) -> Result<Self, Self::Error> {
        let page_count = reader.page_count()?;
        Ok(Self::new(
            reader.name,
            reader.runtime,
            reader.snapshot,
            page_count,
        ))
    }
}

impl VolumeRead for VolumeReader {
    fn page_count(&self) -> culprit::Result<PageCount, GraftErr> {
        self.runtime
            .storage()
            .read()
            .page_count(&self.snapshot)
            .or_into_ctx()
    }

    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, GraftErr> {
        self.runtime.read_page(&self.snapshot, pageidx)
    }
}
