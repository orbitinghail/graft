use graft_core::{PageCount, SegmentId, page::Page};
use splinter_rs::{PartitionRead, Splinter};

use crate::{
    local::fjall_storage::FjallStorageErr,
    volume_reader::{VolumeRead, VolumeReader},
};

pub struct VolumeWriter {
    reader: VolumeReader,
    page_count: PageCount,
    sid: SegmentId,
    graft: Splinter,
}

impl VolumeWriter {
    pub fn from_reader(reader: VolumeReader) -> culprit::Result<Self, FjallStorageErr> {
        let page_count = reader.page_count()?;
        Ok(Self {
            reader,
            page_count,
            sid: SegmentId::random(),
            graft: Splinter::default(),
        })
    }
}

impl VolumeRead for VolumeWriter {
    fn page_count(&self) -> culprit::Result<PageCount, FjallStorageErr> {
        Ok(self.page_count)
    }

    fn read_page(
        &self,
        pageidx: graft_core::PageIdx,
    ) -> culprit::Result<graft_core::page::Page, FjallStorageErr> {
        if !self.page_count.contains(pageidx) {
            Ok(Page::EMPTY)
        } else if self.graft.contains(pageidx.to_u32()) {
            let page = self.reader.storage().read_page(self.sid.clone(), pageidx)?;
            Ok(page.unwrap_or(Page::EMPTY))
        } else {
            self.reader.read_page(pageidx)
        }
    }
}
