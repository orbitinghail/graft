use std::sync::Arc;

use culprit::Result;
use graft_core::{PageCount, PageIdx, SegmentId, page::Page};
use splinter_rs::{PartitionRead, PartitionWrite, Splinter};

use crate::local::fjall_storage::{FjallStorage, FjallStorageErr};

pub struct StagedSegment {
    storage: Arc<FjallStorage>,
    sid: SegmentId,
    graft: Splinter,
}

impl StagedSegment {
    pub fn new(storage: Arc<FjallStorage>) -> Self {
        Self {
            storage,
            sid: SegmentId::random(),
            graft: Splinter::default(),
        }
    }

    pub fn read_page(&self, pageidx: PageIdx) -> Result<Option<Page>, FjallStorageErr> {
        if self.graft.contains(pageidx.to_u32()) {
            self.storage.read_page(self.sid.clone(), pageidx)
        } else {
            Ok(None)
        }
    }

    pub fn write_page(&mut self, pageidx: PageIdx, page: Page) -> Result<(), FjallStorageErr> {
        self.graft.insert(pageidx.to_u32());
        self.storage.write_page(self.sid.clone(), pageidx, page)
    }

    pub fn truncate(&mut self, page_count: PageCount) -> Result<(), FjallStorageErr> {
        let start = page_count
            .last_index()
            .unwrap_or_default()
            .saturating_next();

        // remove all pages from the graft which are no longer valid
        self.graft.remove_range(start.to_u32()..);

        // remove all pages from the writer segment which are no longer valid
        self.storage
            .remove_page_range(&self.sid, start..=PageIdx::LAST)
    }
}

impl Drop for StagedSegment {
    fn drop(&mut self) {
        todo!("figure out how to gc a uncommitted staged segment")
    }
}
