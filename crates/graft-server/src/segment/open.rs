//! An open segment is a segment that is currently being written to. It can be serialized into a Closed segment.

use std::{collections::BTreeMap, fmt::Debug};

use bytes::{BufMut, Bytes, BytesMut};
use graft_core::{
    byte_unit::ByteUnit,
    page::{Page, PAGESIZE},
    page_offset::PageOffset,
    SegmentId, VolumeId,
};
use itertools::Itertools;
use thiserror::Error;
use zerocopy::IntoBytes;

use super::{
    closed::{
        closed_segment_size, SegmentHeaderPage, SegmentIndex, SegmentIndexKey, SEGMENT_MAX_PAGES,
    },
    offsets_map::OffsetsMap,
};

#[derive(Error, Debug, PartialEq, Eq)]
#[error("segment is full")]
pub struct SegmentFullErr;

#[derive(Default)]
pub struct OpenSegment {
    index: BTreeMap<(VolumeId, PageOffset), Page>,
}

impl Debug for OpenSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("OpenSegment");
        for (count, vid) in self.index.keys().map(|(vid, _)| vid).dedup_with_count() {
            out.field(&vid.short(), &count);
        }
        out.finish()
    }
}

impl OpenSegment {
    pub fn insert(
        &mut self,
        vid: VolumeId,
        offset: PageOffset,
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
    pub fn is_full(&self) -> bool {
        self.index.len() >= SEGMENT_MAX_PAGES
    }

    pub fn find_page(&self, vid: VolumeId, offset: PageOffset) -> Option<&Page> {
        self.index.get(&(vid, offset))
    }

    pub fn encoded_size(&self) -> ByteUnit {
        closed_segment_size(self.index.len())
    }

    pub fn serialize(self, sid: SegmentId) -> (Bytes, OffsetsMap) {
        let mut buf = BytesMut::with_capacity(self.encoded_size().as_usize());
        let mut index_builder = SegmentIndex::builder(self.index.len());
        let mut offsets_builder = OffsetsMap::builder();

        // split the buffer into header and data
        let mut data = buf.split_off(PAGESIZE.as_usize());

        // write pages to buffer while building index
        for (local_offset, ((vid, off), page)) in (0_u16..).zip(self.index.into_iter()) {
            data.put_slice(&page);
            index_builder.insert(SegmentIndexKey::new(vid.clone(), off), local_offset);
            offsets_builder.insert(vid, off);
        }

        // build the header and write the index if it's not inline
        let header_page = if index_builder.is_inline() {
            SegmentHeaderPage::new_with_inline_index(sid, index_builder)
        } else {
            let index_bytes = index_builder.as_bytes();
            let index_size: ByteUnit = index_bytes.len().into();
            data.put_slice(index_bytes);
            SegmentHeaderPage::new(sid, index_size)
        };

        // write the header
        buf.put_slice(header_page.as_bytes());

        // unsplit the segment and freeze it
        buf.unsplit(data);
        (buf.freeze(), offsets_builder.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::closed::{ClosedSegment, SEGMENT_MAX_PAGES};

    #[test]
    fn test_segment_sanity() {
        let mut open_segment = OpenSegment::default();

        let vid = VolumeId::random();
        let page0 = Page::test_filled(1);
        let page1 = Page::test_filled(2);

        open_segment
            .insert(vid.clone(), PageOffset::new(0), page0.clone())
            .unwrap();
        open_segment
            .insert(vid.clone(), PageOffset::new(1), page1.clone())
            .unwrap();

        // ensure that we can query pages in the open_segment
        assert_eq!(
            open_segment.find_page(vid.clone(), PageOffset::new(0)),
            Some(&page0)
        );
        assert_eq!(
            open_segment.find_page(vid.clone(), PageOffset::new(1)),
            Some(&page1)
        );

        let expected_size = open_segment.encoded_size();

        let sid = SegmentId::random();
        let (buf, offsets) = open_segment.serialize(sid.clone());

        assert_eq!(buf.len(), expected_size);

        let closed_segment = ClosedSegment::from_bytes(&buf).unwrap();

        assert_eq!(closed_segment.sid(), &sid);
        assert_eq!(closed_segment.len(), 2);
        assert!(!closed_segment.is_empty());
        assert_eq!(
            closed_segment.find_page(vid.clone(), PageOffset::new(0)),
            Some(page0)
        );
        assert_eq!(
            closed_segment.find_page(vid.clone(), PageOffset::new(1)),
            Some(page1)
        );

        // validate the offsets map
        assert!(!offsets.is_empty());
        assert!(offsets.contains(&vid, PageOffset::new(0)));
        assert!(offsets.contains(&vid, PageOffset::new(1)));
        assert!(!offsets.contains(&vid, PageOffset::new(2)));
        assert!(!offsets.contains(&VolumeId::random(), PageOffset::new(0)));
    }

    #[test]
    fn test_zero_length_segment() {
        let open_segment = OpenSegment::default();
        let expected_size = open_segment.encoded_size();

        let (buf, offsets) = open_segment.serialize(SegmentId::random());

        assert_eq!(buf.len(), expected_size);
        assert!(offsets.is_empty());

        assert_eq!(
            buf.len(),
            PAGESIZE,
            "an empty segment should fit in the page header"
        );

        let closed_segment = ClosedSegment::from_bytes(&buf).unwrap();

        assert_eq!(closed_segment.len(), 0);
        assert!(closed_segment.is_empty());
    }

    #[test]
    fn test_full_segment() {
        let mut open_segment = OpenSegment::default();

        let vid = VolumeId::random();
        let page = Page::test_filled(1);

        let num_pages = SEGMENT_MAX_PAGES as u32;
        for i in 0..num_pages {
            open_segment
                .insert(vid.clone(), (i * 2).into(), page.clone())
                .unwrap();
        }

        let expected_size = open_segment.encoded_size();

        let (buf, offsets) = open_segment.serialize(SegmentId::random());

        assert_eq!(buf.len(), expected_size);

        assert!(!offsets.is_empty());
        for i in 0..num_pages {
            assert!(offsets.contains(&vid, (i * 2).into()));
        }

        let closed_segment = ClosedSegment::from_bytes(&buf).unwrap();

        assert_eq!(closed_segment.len(), num_pages as usize);
    }

    #[test]
    fn test_overfull_segment() {
        let mut open_segment = OpenSegment::default();

        let vid = VolumeId::random();
        let page = Page::test_filled(1);

        let num_pages = SEGMENT_MAX_PAGES as u32;
        for i in 0..num_pages {
            open_segment
                .insert(vid.clone(), (i * 2).into(), page.clone())
                .unwrap();
        }

        // insert one more page; should fail
        let err = open_segment
            .insert(vid.clone(), PageOffset::MAX, page.clone())
            .expect_err("expected segment to be full");
        assert_eq!(err, SegmentFullErr);
    }
}
