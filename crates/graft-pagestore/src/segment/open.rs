//! An open segment is a segment that is currently being written to. It can be serialized into a Closed segment.

use std::{
    collections::BTreeMap,
    io::{self, Seek, Write},
};

use graft_core::{
    offset::Offset,
    page::{Page, PAGESIZE},
    volume_id::VolumeId,
};
use zerocopy::AsBytes;

use super::closed::{SegmentHeaderPage, SegmentIndex, SegmentIndexKey};

#[derive(Debug, Default)]
pub struct OpenSegment {
    index: BTreeMap<(VolumeId, Offset), Page>,
}

impl OpenSegment {
    pub fn write_to(self, mut writer: impl Write + Seek) -> io::Result<()> {
        let mut index_builder = SegmentIndex::builder(self.index.len());

        // seek to the start of the data section
        writer.seek(io::SeekFrom::Start(PAGESIZE as u64))?;

        for (local_offset, ((vid, off), page)) in (0_u16..).zip(self.index.into_iter()) {
            writer.write_all(&page)?;
            index_builder.insert(SegmentIndexKey::new(vid, off, 0), local_offset);
        }

        let header_page = if index_builder.is_inline() {
            SegmentHeaderPage::new_with_inline_index(index_builder)
        } else {
            let index_bytes = index_builder.as_bytes();
            let index_size: u32 = index_bytes.len().try_into().unwrap();
            let index_offset: usize = writer.stream_position()?.try_into().unwrap();
            assert!(
                index_offset % PAGESIZE == 0,
                "index_offset must be page aligned"
            );
            writer.write_all(index_bytes)?;
            SegmentHeaderPage::new((index_offset / PAGESIZE).try_into().unwrap(), index_size)
        };

        writer.seek(io::SeekFrom::Start(0))?;
        writer.write_all(header_page.as_bytes())?;

        Ok(())
    }
}
