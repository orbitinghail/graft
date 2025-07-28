use culprit::ResultExt;
use graft_core::{PageCount, SegmentId, graft::Graft, page_count};
use splinter_rs::Splinter;

use crate::{local::fjall_storage::FjallStorageErr, volume_reader::VolumeReader};

pub struct VolumeWriter {
    reader: VolumeReader,
    page_count: PageCount,
    sid: SegmentId,
    graft: Splinter,
}

impl VolumeWriter {
    pub fn new(reader: VolumeReader) -> culprit::Result<Self, FjallStorageErr> {
        let page_count = reader.page_count()?;
        Ok(Self {
            reader,
            page_count,
            sid: SegmentId::random(),
            graft: Splinter::default(),
        })
    }
}
