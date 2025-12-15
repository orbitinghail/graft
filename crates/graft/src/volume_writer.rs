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

    /// Soft truncates the Volume to the given `PageCount`.
    ///
    /// It's important to understand that this operation does not actually write
    /// any pages to the Volume. It simply updates the page count.
    ///
    /// This means that truncation is soft. If you truncate to a smaller size
    /// and then truncate to a larger size later, pages that were previously
    /// hidden by the smaller page count will be visible again.
    fn soft_truncate(&mut self, page_count: PageCount) -> Result<(), GraftErr>;

    fn commit(self) -> Result<VolumeReader, GraftErr>;
}

#[derive(Debug)]
pub struct VolumeWriter {
    runtime: Runtime,
    vid: VolumeId,
    snapshot: Snapshot,
    segment: SegmentIdx,
}

impl VolumeWriter {
    pub(crate) fn new(runtime: Runtime, vid: VolumeId, snapshot: Snapshot) -> Self {
        let segment = runtime.create_staged_segment();
        Self { runtime, vid, snapshot, segment }
    }
}

impl VolumeRead for VolumeWriter {
    fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    fn page_count(&self) -> PageCount {
        self.snapshot.page_count
    }

    fn read_page(&self, pageidx: PageIdx) -> Result<Page, GraftErr> {
        if !self.page_count().contains(pageidx) {
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
        self.snapshot.page_count = self.snapshot.page_count.max(pageidx.pages());
        self.segment.insert(pageidx);
        Ok(self
            .runtime
            .storage()
            .write_page(self.segment.sid().clone(), pageidx, page)?)
    }

    fn soft_truncate(&mut self, page_count: PageCount) -> Result<(), GraftErr> {
        if page_count < self.page_count() {
            let start = page_count
                .last_pageidx()
                .unwrap_or_default()
                .saturating_next();
            self.segment.remove_page_range(start..=PageIdx::LAST);
            self.runtime
                .storage()
                .remove_page_range(self.segment.sid(), start..=PageIdx::LAST)?;
        }
        self.snapshot.page_count = page_count;
        Ok(())
    }

    fn commit(self) -> Result<VolumeReader, GraftErr> {
        let page_count = self.snapshot.page_count;
        let snapshot = self.runtime.storage().read_write().commit(
            &self.vid,
            self.snapshot,
            page_count,
            self.segment,
        )?;
        Ok(VolumeReader::new(self.runtime, self.vid, snapshot))
    }
}
