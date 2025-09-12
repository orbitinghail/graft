use culprit::Result;
use graft_core::{PageCount, PageIdx, SegmentId, page::Page};
use splinter_rs::{PartitionRead, PartitionWrite, Splinter};

use crate::{
    local::fjall_storage::FjallStorageErr,
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
    sid: SegmentId,
    graft: Splinter,
}

impl VolumeWriter {
    pub fn from_reader(reader: VolumeReader) -> Result<Self, FjallStorageErr> {
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
    fn page_count(&self) -> Result<PageCount, FjallStorageErr> {
        Ok(self.page_count)
    }

    fn read_page(
        &self,
        pageidx: graft_core::PageIdx,
    ) -> Result<graft_core::page::Page, FjallStorageErr> {
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

impl VolumeWrite for VolumeWriter {
    fn write_page(&mut self, pageidx: PageIdx, page: Page) -> Result<(), FjallStorageErr> {
        self.graft.insert(pageidx.to_u32());
        self.page_count = self.page_count.max(pageidx.pages());
        self.reader
            .storage()
            .write_page(self.sid.clone(), pageidx, page)
    }

    fn truncate(&mut self, page_count: PageCount) -> Result<(), FjallStorageErr> {
        self.page_count = page_count;
        let start = page_count
            .last_index()
            .unwrap_or_default()
            .saturating_next();

        // remove all pages from the graft which are no longer valid
        todo!("self.graft.remove_range");

        // remove all pages from the writer segment which are no longer valid
        self.reader
            .storage()
            .remove_page_range(&self.sid, start..=PageIdx::LAST)?;
    }

    fn commit(self) -> Result<(), FjallStorageErr> {
        let (storage, rpc, snapshot) = self.reader.unpack();
        let commit_lsn = snapshot
            .lsn()
            .unwrap_or_default()
            .next()
            .expect("LSN overflow");
        todo!()
    }
}
