use culprit::Result;
use graft_core::{PageCount, PageIdx, page::Page};

use crate::{
    local::{fjall_storage::FjallStorageErr, staged_segment::StagedSegment},
    volume_reader::{VolumeRead, VolumeReader},
};

/// A type which can write to a Volume
pub trait VolumeWrite {
    fn write_page(&mut self, pageidx: PageIdx, page: Page) -> Result<(), FjallStorageErr>;
    fn truncate(&mut self, page_count: PageCount) -> Result<(), FjallStorageErr>;
    fn commit(self) -> Result<(), FjallStorageErr>;
}

pub struct VolumeWriter {
    reader: VolumeReader,
    page_count: PageCount,
    segment: StagedSegment,
}

impl VolumeWriter {
    pub fn from_reader(reader: VolumeReader) -> Result<Self, FjallStorageErr> {
        let page_count = reader.page_count()?;
        let storage = reader.storage().clone();
        Ok(Self {
            reader,
            page_count,
            segment: StagedSegment::new(storage),
        })
    }
}

impl VolumeRead for VolumeWriter {
    fn page_count(&self) -> Result<PageCount, FjallStorageErr> {
        Ok(self.page_count)
    }

    fn read_page(&self, pageidx: PageIdx) -> Result<Page, FjallStorageErr> {
        if !self.page_count.contains(pageidx) {
            Ok(Page::EMPTY)
        } else if let Some(page) = self.segment.read_page(pageidx)? {
            Ok(page)
        } else {
            self.reader.read_page(pageidx)
        }
    }
}

impl VolumeWrite for VolumeWriter {
    fn write_page(&mut self, pageidx: PageIdx, page: Page) -> Result<(), FjallStorageErr> {
        self.page_count = self.page_count.max(pageidx.pages());
        self.segment.write_page(pageidx, page)
    }

    fn truncate(&mut self, page_count: PageCount) -> Result<(), FjallStorageErr> {
        self.page_count = page_count;
        self.segment.truncate(page_count)
    }

    fn commit(self) -> Result<(), FjallStorageErr> {
        let (storage, _, snapshot) = self.reader.unpack();
        storage.commit(snapshot, self.page_count, self.segment)
    }
}
