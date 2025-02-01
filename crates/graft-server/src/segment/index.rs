//! Segment Index
//!
//! The Segment Index is made up of two sections. a Volume Index and a list of PageOffsets.
//!
//! The Volume Index is a list of (VolumeId, Start, Pages) tuples.
//!     VolumeId: The VolumeId for this set of pages
//!     Start: The position of the first page and page offset for this Volume
//!     Pages: The number of pages stored in this Segment for this Volume
//!
//! The VolumeId Table is sorted by VolumeId.
//!
//! The list of PageOffsets is stored in the same order as pages are stored in
//! this segment, and the index requires that each set of Offsets corresponding
//! to a Volume is sorted.

use std::mem::size_of;

use bytes::BytesMut;
use graft_core::{
    byte_unit::ByteUnit, page_count::PageCount, page_offset::PageOffset, zerocopy_err::ZerocopyErr,
    VolumeId,
};
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::bytes_vec::BytesVec;

#[derive(Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone)]
#[repr(C)]
struct VolumeMeta {
    vid: VolumeId,

    /// The position of the first page for this Volume in the segment
    start: u16,

    /// The number of pages stored in this Segment for this Volume
    pages: u16,
}

pub struct SegmentIndex<'a> {
    volume_index: &'a [VolumeMeta],
    page_offsets: &'a [PageOffset],
}

impl<'a> SegmentIndex<'a> {
    pub fn from_bytes(data: &'a [u8], volumes: usize) -> Result<Self, ZerocopyErr> {
        let volume_index_size = volumes * size_of::<VolumeMeta>();
        assert!(
            data.len() >= volume_index_size,
            "segment must be at least as long as the volume index"
        );
        let (volume_index, page_offsets) = data.split_at(volume_index_size);
        let volume_index = <[VolumeMeta]>::try_ref_from_bytes(volume_index)?;
        let page_offsets = <[PageOffset]>::try_ref_from_bytes(page_offsets)?;
        Ok(Self { volume_index, page_offsets })
    }

    pub fn pages(&self) -> PageCount {
        PageCount::new(self.page_offsets.len() as u32)
    }

    pub fn is_empty(&self) -> bool {
        self.page_offsets.is_empty()
    }

    /// Lookup the local offset of a page by (VolumeId, PageOffset)
    /// The returned value can be used to index into the segment's page list
    pub fn lookup(&self, vid: &VolumeId, offset: PageOffset) -> Option<usize> {
        let meta_idx = self
            .volume_index
            .binary_search_by(|meta| meta.vid.cmp(vid))
            .ok()?;

        let meta = &self.volume_index[meta_idx];

        let start = meta.start as usize;
        let end = start + (meta.pages as usize);

        let relative_offset = self.page_offsets[start..end].binary_search(&offset).ok()?;

        Some(start + relative_offset)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&VolumeId, PageOffset)> {
        self.volume_index.iter().flat_map(|meta| {
            let start = meta.start as usize;
            let end = start + (meta.pages as usize);
            self.page_offsets[start..end]
                .iter()
                .map(move |offset| (&meta.vid, *offset))
        })
    }
}

#[derive(Default)]
pub struct SegmentIndexBuilder {
    volume_index: BytesMut,
    page_offsets: BytesMut,

    current: Option<VolumeMeta>,
    pages: u16,
    last_offset: Option<PageOffset>,
}

impl SegmentIndexBuilder {
    pub fn new_with_capacity(volumes: usize, pages: PageCount) -> Self {
        Self {
            volume_index: BytesMut::with_capacity(volumes * size_of::<VolumeMeta>()),
            page_offsets: BytesMut::with_capacity(pages.as_usize() * size_of::<PageOffset>()),
            current: None,
            pages: 0,
            last_offset: None,
        }
    }

    pub fn serialized_size(num_volumes: usize, num_pages: PageCount) -> ByteUnit {
        let volume_index_size = (num_volumes * size_of::<VolumeMeta>()) as u64;
        let page_offsets_size = (num_pages.as_usize() * size_of::<PageOffset>()) as u64;
        ByteUnit::new(volume_index_size + page_offsets_size)
    }

    pub fn insert(&mut self, vid: &VolumeId, offset: PageOffset) {
        let current = self.current.get_or_insert_with(|| VolumeMeta {
            vid: vid.clone(),
            start: self.pages,
            pages: 0,
        });

        // If the VolumeId has changed, write the current VolumeMeta to the volume_index
        if &current.vid != vid {
            assert!(
                vid > &current.vid,
                "Volumes must be inserted in order by ID"
            );
            let last = std::mem::replace(
                current,
                VolumeMeta {
                    vid: vid.clone(),
                    start: self.pages,
                    pages: 0,
                },
            );
            self.volume_index.extend_from_slice(last.as_bytes());
            self.last_offset = None;
        }

        // verify that offset is larger than the last offset, while also updating the last offset
        if let Some(last_offset) = self.last_offset.replace(offset) {
            assert!(
                offset > last_offset,
                "Offsets must be inserted in order (per volume)"
            );
        }

        // Write the PageOffset to the page_offsets
        self.page_offsets.extend_from_slice(offset.as_bytes());

        // Increment page counters
        current.pages = current.pages.checked_add(1).expect("pages overflow");
        self.pages = self.pages.checked_add(1).expect("pages overflow");
    }

    pub fn finish(self) -> BytesVec {
        let mut volume_index = self.volume_index;
        if let Some(current) = self.current {
            volume_index.extend_from_slice(current.as_bytes());
        }
        vec![volume_index.freeze(), self.page_offsets.freeze()].into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[graft_test::test]
    fn test_segment_index_sanity() {
        let mut builder = SegmentIndexBuilder::default();

        let mut vids = vec![VolumeId::random(), VolumeId::random(), VolumeId::random()];
        vids.sort();

        // insert 100 offsets for each vid
        for vid in &vids {
            for i in 0..100 {
                builder.insert(vid, PageOffset::new(i));
            }
        }

        let data = builder.finish().into_bytes();
        let index =
            SegmentIndex::from_bytes(&data, vids.len()).expect("failed to load segment index");

        // lookup all the offsets
        for (volume_offset, vid) in vids.iter().enumerate() {
            for i in 0..100 {
                let offset = PageOffset::new(i);
                let idx = index.lookup(vid, offset).expect("offset not found");
                assert_eq!(idx, (volume_offset * 100) + i as usize);
            }
        }
    }
}
