use crate::core::{PageCount, PageIdx, VolumeId, commit::SegmentIdx, page::Page};

use crate::{
    GraftErr,
    rt::runtime::Runtime,
    snapshot::Snapshot,
    volume_reader::{VolumeRead, VolumeReader},
};

/// A type which can write to a Volume
pub trait VolumeWrite {
    fn write_page(&mut self, pageidx: PageIdx, page: Page) -> Result<(), GraftErr>;
    fn truncate(&mut self, page_count: PageCount) -> Result<(), GraftErr>;
    fn commit(self) -> Result<VolumeReader, GraftErr>;
}

#[derive(Debug)]
pub struct VolumeWriter {
    runtime: Runtime,
    vid: VolumeId,
    snapshot: Snapshot,
    page_count: PageCount,
    segment: SegmentIdx,
}

impl VolumeWriter {
    pub(crate) fn new(
        runtime: Runtime,
        vid: VolumeId,
        snapshot: Snapshot,
        page_count: PageCount,
    ) -> Self {
        let segment = runtime.create_staged_segment();
        Self {
            runtime,
            vid,
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
            Ok(self
                .runtime
                .storage()
                .read()
                .read_page(self.segment.sid().clone(), pageidx)
                .transpose()
                .expect("BUG: Staged segment out of sync with storage")?)
        } else {
            self.runtime.read_page(&self.snapshot, pageidx)
        }
    }
}

impl VolumeWrite for VolumeWriter {
    fn write_page(&mut self, pageidx: PageIdx, page: Page) -> Result<(), GraftErr> {
        self.page_count = self.page_count.max(pageidx.pages());
        self.segment.insert(pageidx);
        Ok(self
            .runtime
            .storage()
            .write_page(self.segment.sid().clone(), pageidx, page)?)
    }

    fn truncate(&mut self, page_count: PageCount) -> Result<(), GraftErr> {
        let start = page_count
            .last_pageidx()
            .unwrap_or_default()
            .saturating_next();
        self.page_count = page_count;
        self.segment.remove_page_range(start..=PageIdx::LAST);
        Ok(self
            .runtime
            .storage()
            .remove_page_range(self.segment.sid(), start..=PageIdx::LAST)?)
    }

    fn commit(self) -> Result<VolumeReader, GraftErr> {
        let snapshot = self.runtime.storage().read_write().commit(
            &self.vid,
            self.snapshot,
            self.page_count,
            self.segment,
        )?;
        Ok(VolumeReader::new(self.runtime, self.vid, snapshot))
    }
}
