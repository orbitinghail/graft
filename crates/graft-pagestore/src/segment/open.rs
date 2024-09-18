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
    pub fn insert(&mut self, vid: VolumeId, offset: Offset, page: Page) {
        // TODO: need to return an error if we have more pages than will fit in
        // a maximally sized segment

        self.index.insert((vid, offset), page);
    }

    pub fn find_page(&self, vid: VolumeId, offset: Offset) -> Option<&Page> {
        self.index.get(&(vid, offset))
    }

    pub fn write_to(self, mut writer: impl Write + Seek) -> io::Result<()> {
        let mut index_builder = SegmentIndex::builder(self.index.len());

        // seek to the start of the data section
        writer.seek(io::SeekFrom::Start(PAGESIZE as u64))?;

        for (local_offset, ((vid, off), page)) in (0_u16..).zip(self.index.into_iter()) {
            writer.write_all(&page)?;
            index_builder.insert(SegmentIndexKey::new(vid, off), local_offset);
        }

        let header_page = if index_builder.is_inline() {
            SegmentHeaderPage::new_with_inline_index(index_builder)
        } else {
            let index_bytes = index_builder.as_bytes();
            let index_size: u32 = index_bytes.len().try_into().unwrap();
            writer.write_all(index_bytes)?;
            SegmentHeaderPage::new(index_size)
        };

        writer.seek(io::SeekFrom::Start(0))?;
        writer.write_all(header_page.as_bytes())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::closed::ClosedSegment;

    #[test]
    fn test_segment_sanity() {
        let mut open_segment = OpenSegment::default();

        let vid = VolumeId::random();
        let page0 = Page::from(&[1; PAGESIZE]);
        let page1 = Page::from(&[2; PAGESIZE]);

        open_segment.insert(vid.clone(), 0, page0.clone());
        open_segment.insert(vid.clone(), 1, page1.clone());

        // ensure that we can query pages in the open_segment
        assert_eq!(open_segment.find_page(vid.clone(), 0), Some(&page0));
        assert_eq!(open_segment.find_page(vid.clone(), 1), Some(&page1));

        let mut writer = io::Cursor::new(vec![]);
        open_segment.write_to(&mut writer).unwrap();

        let buf = writer.into_inner();
        let closed_segment = ClosedSegment::from_bytes(&buf).unwrap();

        assert_eq!(
            closed_segment.find_page(vid.clone(), 0),
            Some(page0.as_ref())
        );
        assert_eq!(
            closed_segment.find_page(vid.clone(), 1),
            Some(page1.as_ref())
        );
    }

    #[test]
    fn test_zero_length_segment() {
        let open_segment = OpenSegment::default();

        let mut writer = io::Cursor::new(vec![]);
        open_segment.write_to(&mut writer).unwrap();

        let buf = writer.into_inner();

        assert_eq!(
            buf.len(),
            PAGESIZE,
            "an empty segment should fit in a single page"
        );

        let closed_segment = ClosedSegment::from_bytes(&buf).unwrap();

        assert_eq!(closed_segment.len(), 0);
        assert!(closed_segment.is_empty());
    }

    #[test]
    fn test_full_segment() {
        let mut open_segment = OpenSegment::default();

        let vid = VolumeId::random();
        let page = Page::from(&[1; PAGESIZE]);

        // calculated by hand via inspecting odht and current segment encoding
        let num_pages = 4071;
        for i in 0..num_pages {
            open_segment.insert(vid.clone(), i * 2, page.clone());
        }

        let mut writer = io::Cursor::new(vec![]);
        open_segment.write_to(&mut writer).unwrap();

        let buf = writer.into_inner();
        let closed_segment = ClosedSegment::from_bytes(&buf).unwrap();

        assert_eq!(closed_segment.len(), num_pages as usize);

        // now let's try to write one more page - this should fail
        let mut open_segment = OpenSegment::default();
        for i in 0..(num_pages + 1) {
            open_segment.insert(vid.clone(), i * 2, page.clone());
        }
        let mut writer = io::Cursor::new(vec![]);
        open_segment.write_to(&mut writer).unwrap();

        let buf = writer.into_inner();
        let failure = std::panic::catch_unwind(|| {
            ClosedSegment::from_bytes(&buf).unwrap();
        });
        assert!(failure.is_err());
    }
}
