//! An open segment is a segment that is currently being written to. It can be serialized into a Closed segment.

use std::{collections::BTreeMap, fmt::Debug};

use culprit::Culprit;
use graft_core::{
    byte_unit::ByteUnit, page::Page, page_count::PageCount, PageIdx, SegmentId, VolumeId,
};
use thiserror::Error;
use zerocopy::IntoBytes;

use crate::bytes_vec::BytesVec;

use super::{
    closed::{closed_segment_size, SegmentFooter, SEGMENT_MAX_PAGES, SEGMENT_MAX_VOLUMES},
    index::SegmentIndexBuilder,
    multigraft::MultiGraft,
};

#[derive(Error, Debug, PartialEq, Eq)]
#[error("segment is full")]
pub struct SegmentFullErr;

#[derive(Default)]
pub struct OpenSegment {
    index: BTreeMap<VolumeId, BTreeMap<PageIdx, Page>>,
}

impl Debug for OpenSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("OpenSegment");
        for (vid, pages) in &self.index {
            out.field(&vid.short(), &pages.len());
        }
        out.finish()
    }
}

impl OpenSegment {
    /// returns true if the segment can accept another page for the specified volume
    pub fn has_space_for(&self, vid: &VolumeId) -> bool {
        match self.index.contains_key(vid) {
            true => self.pages() < SEGMENT_MAX_PAGES,
            false => self.volumes() < SEGMENT_MAX_VOLUMES && self.pages() < SEGMENT_MAX_PAGES,
        }
    }

    pub fn insert(
        &mut self,
        vid: VolumeId,
        offset: PageIdx,
        page: Page,
    ) -> Result<(), Culprit<SegmentFullErr>> {
        if !self.has_space_for(&vid) {
            return Err(Culprit::new(SegmentFullErr));
        }
        self.index.entry(vid).or_default().insert(offset, page);
        Ok(())
    }

    #[inline]
    #[must_use]
    pub fn volumes(&self) -> usize {
        self.index.len()
    }

    #[inline]
    #[must_use]
    pub fn pages(&self) -> PageCount {
        PageCount::new(self.index.values().map(|p| p.len() as u32).sum())
    }

    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    #[cfg(test)]
    pub fn find_page(&self, vid: &VolumeId, offset: PageIdx) -> Option<&Page> {
        self.index.get(vid)?.get(&offset)
    }

    pub fn serialized_size(&self) -> ByteUnit {
        closed_segment_size(self.volumes(), self.pages())
    }

    pub fn serialize(self, sid: SegmentId) -> (BytesVec, MultiGraft) {
        let volumes = self.volumes();
        let pages = self.pages();
        // +2 for the index, +1 for the footer
        let mut data = BytesVec::with_capacity(pages.to_usize() + 2 + 1);
        dbg!(volumes, pages);
        let mut index_builder = SegmentIndexBuilder::new_with_capacity(volumes, pages);
        let mut offsets_builder = MultiGraft::builder();

        // write pages to buffer while building index
        for (vid, pages) in self.index {
            for (off, page) in pages {
                data.put(page.into());
                index_builder.insert(&vid, off);
                offsets_builder.insert(&vid, off);
            }
        }

        // write out the index
        let index_size = index_builder.finish(&mut data);
        debug_assert_eq!(
            index_size,
            SegmentIndexBuilder::serialized_size(volumes, pages),
            "index size mismatch"
        );

        // write out the footer
        let footer = SegmentFooter::new(sid, volumes, index_size);
        data.put_slice(footer.as_bytes());

        (data, offsets_builder.build())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::closed::{ClosedSegment, SEGMENT_MAX_PAGES};
    use assert_matches::assert_matches;
    use bytes::Buf;
    use graft_core::pageidx;

    #[graft_test::test]
    fn test_segment_sanity() {
        let mut open_segment = OpenSegment::default();

        let vid = VolumeId::random();
        let page0 = Page::test_filled(1);
        let page1 = Page::test_filled(2);

        open_segment
            .insert(vid.clone(), pageidx!(1), page0.clone())
            .unwrap();
        open_segment
            .insert(vid.clone(), pageidx!(2), page1.clone())
            .unwrap();

        // ensure that we can query pages in the open_segment
        assert_eq!(open_segment.find_page(&vid, pageidx!(1)), Some(&page0));
        assert_eq!(open_segment.find_page(&vid, pageidx!(2)), Some(&page1));

        let expected_size = open_segment.serialized_size();

        let sid = SegmentId::random();
        let (buf, offsets) = open_segment.serialize(sid.clone());

        assert_eq!(buf.remaining(), expected_size);

        let buf = buf.into_bytes();
        let closed_segment = ClosedSegment::from_bytes(&buf).unwrap();

        assert_eq!(closed_segment.sid(), &sid);
        assert_eq!(closed_segment.pages(), 2);
        assert!(!closed_segment.is_empty());
        assert_eq!(
            closed_segment.find_page(vid.clone(), pageidx!(1)),
            Some(page0)
        );
        assert_eq!(
            closed_segment.find_page(vid.clone(), pageidx!(2)),
            Some(page1)
        );

        // validate the offsets map
        assert!(!offsets.is_empty());
        assert!(offsets.contains(&vid, pageidx!(1)));
        assert!(offsets.contains(&vid, pageidx!(2)));
        assert!(!offsets.contains(&vid, pageidx!(3)));
        assert!(!offsets.contains(&VolumeId::random(), pageidx!(1)));
    }

    #[graft_test::test]
    fn test_zero_length_segment() {
        let open_segment = OpenSegment::default();
        let expected_size = open_segment.serialized_size();

        let (buf, offsets) = open_segment.serialize(SegmentId::random());

        assert_eq!(buf.remaining(), expected_size);
        assert!(offsets.is_empty());

        // an empty segment should just be a footer
        assert_eq!(expected_size, size_of::<SegmentFooter>());

        let buf = buf.into_bytes();
        let closed_segment = ClosedSegment::from_bytes(&buf).unwrap();

        assert!(closed_segment.pages().is_empty());
        assert!(closed_segment.is_empty());
    }

    #[graft_test::test]
    fn test_full_segment() {
        let mut open_segment = OpenSegment::default();

        // generate many volumes
        let vids = (0..SEGMENT_MAX_VOLUMES)
            .map(|_| VolumeId::random())
            .collect::<Vec<_>>();

        // insert SEGMENT_MAX_PAGES pages while cycling through the volumes
        let page = Page::test_filled(1);
        let mut vid_cycle = vids.iter().cycle();
        for offset in SEGMENT_MAX_PAGES.iter() {
            open_segment
                .insert(vid_cycle.next().unwrap().clone(), offset, page.clone())
                .unwrap();
        }

        // the segment should not be able to accept any more pages for any volume
        assert!(!open_segment.has_space_for(&vids[0]));
        assert!(!open_segment.has_space_for(&VolumeId::random()));

        let expected_size = open_segment.serialized_size();
        let (buf, offsets) = open_segment.serialize(SegmentId::random());
        assert_eq!(buf.remaining(), expected_size);

        assert!(!offsets.is_empty());
        let mut vid_cycle = vids.iter().cycle();
        for offset in SEGMENT_MAX_PAGES.iter() {
            assert!(offsets.contains(vid_cycle.next().unwrap(), offset));
        }

        let buf = buf.into_bytes();
        let closed_segment = ClosedSegment::from_bytes(&buf).unwrap();
        assert_eq!(closed_segment.pages(), SEGMENT_MAX_PAGES);
    }

    #[graft_test::test]
    fn test_overfull_segment() {
        let mut open_segment = OpenSegment::default();

        // generate many volumes
        let vids = (0..SEGMENT_MAX_VOLUMES)
            .map(|_| VolumeId::random())
            .collect::<Vec<_>>();
        let page = Page::test_filled(1);

        // fill the segment with one fewer page than the max
        let mut vid_cycle = vids.iter().cycle();
        for offset in SEGMENT_MAX_PAGES.saturating_decr().iter() {
            open_segment
                .insert(vid_cycle.next().unwrap().clone(), offset, page.clone())
                .unwrap();
        }

        // the segment should be able to accept one more page for an existing volume
        assert!(open_segment.has_space_for(&vids[0]));
        // but not for a new volume
        assert!(!open_segment.has_space_for(&VolumeId::random()));

        // insert a page for the last volume
        open_segment
            .insert(vids[0].clone(), PageIdx::LAST, page.clone())
            .expect("expected segment to accept one more page");

        // the segment should not be able to accept any more pages for any volume
        assert!(!open_segment.has_space_for(&vids[0]));
        assert!(!open_segment.has_space_for(&VolumeId::random()));

        // insert one more page; should fail
        let err = open_segment
            .insert(vids[0].clone(), PageIdx::LAST, page.clone())
            .expect_err("expected segment to be full");
        assert_matches!(err.ctx(), SegmentFullErr);
    }
}
