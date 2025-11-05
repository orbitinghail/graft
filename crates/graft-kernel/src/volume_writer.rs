use culprit::{Result, ResultExt};
use graft_core::{PageCount, PageIdx, commit::SegmentIdx, page::Page, volume_ref::VolumeRef};

use crate::{
    GraftErr, rt::runtime_handle::RuntimeHandle, snapshot::Snapshot, volume_name::VolumeName,
    volume_reader::VolumeRead,
};

/// A type which can write to a Volume
pub trait VolumeWrite {
    fn write_page(&mut self, pageidx: PageIdx, page: Page) -> Result<(), GraftErr>;
    fn truncate(&mut self, page_count: PageCount) -> Result<(), GraftErr>;
    fn commit(self) -> Result<VolumeRef, GraftErr>;
}

#[derive(Debug)]
pub struct VolumeWriter {
    name: VolumeName,
    runtime: RuntimeHandle,
    snapshot: Snapshot,
    page_count: PageCount,
    segment: SegmentIdx,
}

impl VolumeWriter {
    pub(crate) fn new(
        name: VolumeName,
        runtime: RuntimeHandle,
        snapshot: Snapshot,
        page_count: PageCount,
    ) -> Self {
        let segment = runtime.create_staged_segment();
        Self {
            name,
            runtime,
            snapshot,
            page_count,
            segment,
        }
    }
}

impl VolumeRead for VolumeWriter {
    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    fn page_count(&self) -> Result<PageCount, GraftErr> {
        Ok(self.page_count)
    }

    fn read_page(&self, pageidx: PageIdx) -> Result<Page, GraftErr> {
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

impl VolumeWrite for VolumeWriter {
    fn write_page(&mut self, pageidx: PageIdx, page: Page) -> Result<(), GraftErr> {
        self.page_count = self.page_count.max(pageidx.pages());
        self.segment.insert(pageidx);
        self.runtime
            .storage()
            .write_page(self.segment.sid().clone(), pageidx, page)
            .or_into_ctx()
    }

    fn truncate(&mut self, page_count: PageCount) -> Result<(), GraftErr> {
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

    fn commit(self) -> Result<VolumeRef, GraftErr> {
        self.runtime
            .storage()
            .commit(self.name, self.snapshot, self.page_count, self.segment)
            .or_into_ctx()
    }
}
