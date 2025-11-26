use culprit::{Result, ResultExt};
use graft_core::{PageCount, PageIdx, VolumeId, commit::SegmentIdx, page::Page};

use crate::{
    KernelErr,
    graft_reader::{GraftRead, GraftReader},
    rt::runtime::Runtime,
    snapshot::Snapshot,
};

/// A type which can write to a Graft
pub trait GraftWrite {
    fn write_page(&mut self, pageidx: PageIdx, page: Page) -> Result<(), KernelErr>;
    fn truncate(&mut self, page_count: PageCount) -> Result<(), KernelErr>;
    fn commit(self) -> Result<GraftReader, KernelErr>;
}

#[derive(Debug)]
pub struct GraftWriter {
    runtime: Runtime,
    graft: VolumeId,
    snapshot: Snapshot,
    page_count: PageCount,
    segment: SegmentIdx,
}

impl GraftWriter {
    pub(crate) fn new(
        runtime: Runtime,
        graft: VolumeId,
        snapshot: Snapshot,
        page_count: PageCount,
    ) -> Self {
        let segment = runtime.create_staged_segment();
        Self {
            runtime,
            graft,
            snapshot,
            page_count,
            segment,
        }
    }
}

impl GraftRead for GraftWriter {
    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    fn page_count(&self) -> Result<PageCount, KernelErr> {
        Ok(self.page_count)
    }

    fn read_page(&self, pageidx: PageIdx) -> Result<Page, KernelErr> {
        if !self.page_count.contains(pageidx) {
            Ok(Page::EMPTY)
        } else if self.segment.contains(pageidx) {
            self.runtime
                .storage()
                .read()
                .read_page(self.segment.sid().clone(), pageidx)
                .transpose()
                .expect("BUG: Staged segment out of sync with storage")
                .or_into_ctx()
        } else {
            self.runtime.read_page(&self.snapshot, pageidx)
        }
    }
}

impl GraftWrite for GraftWriter {
    fn write_page(&mut self, pageidx: PageIdx, page: Page) -> Result<(), KernelErr> {
        self.page_count = self.page_count.max(pageidx.pages());
        self.segment.insert(pageidx);
        self.runtime
            .storage()
            .write_page(self.segment.sid().clone(), pageidx, page)
            .or_into_ctx()
    }

    fn truncate(&mut self, page_count: PageCount) -> Result<(), KernelErr> {
        let start = page_count
            .last_pageidx()
            .unwrap_or_default()
            .saturating_next();
        self.page_count = page_count;
        self.segment.remove_page_range(start..=PageIdx::LAST);
        self.runtime
            .storage()
            .remove_page_range(self.segment.sid(), start..=PageIdx::LAST)
            .or_into_ctx()
    }

    fn commit(self) -> Result<GraftReader, KernelErr> {
        let snapshot = self
            .runtime
            .storage()
            .read_write()
            .commit(&self.graft, self.snapshot, self.page_count, self.segment)
            .or_into_ctx()?;
        Ok(GraftReader::new(self.runtime, self.graft, snapshot))
    }
}
