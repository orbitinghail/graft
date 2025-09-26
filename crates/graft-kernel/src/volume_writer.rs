use culprit::Result;
use graft_core::{PageCount, PageIdx, commit::SegmentIdx, page::Page};

use crate::{
    local::fjall_storage::FjallStorageErr, rt::runtime_handle::RuntimeHandle, snapshot::Snapshot,
    volume_reader::VolumeRead,
};

/// A type which can write to a Volume
pub trait VolumeWrite {
    fn write_page(&mut self, pageidx: PageIdx, page: Page) -> Result<(), FjallStorageErr>;
    fn truncate(&mut self, page_count: PageCount) -> Result<(), FjallStorageErr>;
    fn commit(self) -> Result<(), FjallStorageErr>;
}

pub struct VolumeWriter {
    runtime: RuntimeHandle,
    snapshot: Snapshot,
    page_count: PageCount,
    segment: SegmentIdx,
}

impl VolumeWriter {
    pub(crate) fn new(runtime: RuntimeHandle, snapshot: Snapshot, page_count: PageCount) -> Self {
        let segment = runtime.create_staged_segment();
        Self { runtime, snapshot, page_count, segment }
    }
}

impl VolumeRead for VolumeWriter {
    fn page_count(&self) -> Result<PageCount, FjallStorageErr> {
        Ok(self.page_count)
    }

    fn read_page(&self, pageidx: PageIdx) -> Result<Page, FjallStorageErr> {
        if !self.page_count.contains(pageidx) {
            Ok(Page::EMPTY)
        } else if self.segment.contains(pageidx) {
            self.runtime
                .storage()
                .read()
                .read_page(self.segment.sid().clone(), pageidx)
                .transpose()
                .expect("BUG: Staged segment out of sync with storage")
        } else {
            self.runtime.read_page(&self.snapshot, pageidx)
        }
    }
}

impl VolumeWrite for VolumeWriter {
    fn write_page(&mut self, pageidx: PageIdx, page: Page) -> Result<(), FjallStorageErr> {
        self.page_count = self.page_count.max(pageidx.pages());
        self.segment.insert(pageidx);
        self.runtime
            .storage()
            .write_page(self.segment.sid().clone(), pageidx, page)
    }

    fn truncate(&mut self, page_count: PageCount) -> Result<(), FjallStorageErr> {
        let start = page_count
            .last_index()
            .unwrap_or_default()
            .saturating_next();
        self.page_count = page_count;
        self.segment.remove_page_range(start..=PageIdx::LAST);
        self.runtime
            .storage()
            .remove_page_range(self.segment.sid(), start..=PageIdx::LAST)
    }

    fn commit(self) -> Result<(), FjallStorageErr> {
        self.runtime
            .storage()
            .commit(self.snapshot, self.page_count, self.segment)
    }
}
