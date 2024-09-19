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
use thiserror::Error;
use zerocopy::AsBytes;

use super::closed::{SegmentHeaderPage, SegmentIndex, SegmentIndexKey, SEGMENT_MAX_PAGES};

#[derive(Error, Debug, PartialEq, Eq)]
#[error("segment is full")]
pub struct SegmentFullErr;

#[derive(Debug, Default)]
pub struct OpenSegment {
    index: BTreeMap<(VolumeId, Offset), Page>,
}

impl OpenSegment {
    pub fn insert(
        &mut self,
        vid: VolumeId,
        offset: Offset,
        page: Page,
    ) -> Result<(), SegmentFullErr> {
        if self.index.len() >= SEGMENT_MAX_PAGES {
            return Err(SegmentFullErr);
        }

        self.index.insert((vid, offset), page);
        Ok(())
    }

    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        SEGMENT_MAX_PAGES
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
    use crate::segment::closed::{
        ClosedSegment, SegmentValidationErr, SEGMENT_MAGIC, SEGMENT_MAX_LEN, SEGMENT_MAX_PAGES,
    };

    #[test]
    fn test_segment_sanity() {
        let mut open_segment = OpenSegment::default();

        let vid = VolumeId::random();
        let page0 = Page::from(&[1; PAGESIZE]);
        let page1 = Page::from(&[2; PAGESIZE]);

        open_segment.insert(vid.clone(), 0, page0.clone()).unwrap();
        open_segment.insert(vid.clone(), 1, page1.clone()).unwrap();

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

        let num_pages = SEGMENT_MAX_PAGES as u32;
        for i in 0..num_pages {
            open_segment
                .insert(vid.clone(), i * 2, page.clone())
                .unwrap();
        }

        let mut writer = io::Cursor::new(vec![]);
        open_segment.write_to(&mut writer).unwrap();

        let buf = writer.into_inner();
        let closed_segment = ClosedSegment::from_bytes(&buf).unwrap();

        assert_eq!(closed_segment.len(), num_pages as usize);
    }

    #[test]
    fn test_overfull_segment() {
        let mut open_segment = OpenSegment::default();

        let vid = VolumeId::random();
        let page = Page::from(&[1; PAGESIZE]);

        let num_pages = SEGMENT_MAX_PAGES as u32 + 1;
        for i in 0..num_pages {
            if let Err(err) = open_segment.insert(vid.clone(), i * 2, page.clone()) {
                assert_eq!(err, SegmentFullErr);
            }
        }
    }
}
