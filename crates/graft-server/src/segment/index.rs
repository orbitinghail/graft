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

use bytes::{Buf, BytesMut};
use graft_core::{page_count::PageCount, page_offset::PageOffset, VolumeId};
use thiserror::Error;
use zerocopy::{little_endian::U32, ConvertError, Immutable, IntoBytes, KnownLayout, TryFromBytes};

#[derive(Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone)]
#[repr(C)]
struct VolumeMeta {
    vid: VolumeId,
    start: U32,
    pages: PageCount,
}

impl VolumeMeta {
    fn add_page(&mut self) {
        self.pages.incr();
    }
}

#[derive(Debug, Error)]
pub enum SegmentIndexErr {
    #[error("Invalid Alignment")]
    InvalidAlignment,

    #[error("Invalid Size")]
    InvalidSize,

    #[error("Corrupt Segment Index")]
    Corrupt,
}

pub struct SegmentIndex<'a> {
    volume_index: &'a [VolumeMeta],
    page_offsets: &'a [PageOffset],
}

impl<'a> SegmentIndex<'a> {
    pub fn from_bytes(data: &'a [u8], volumes: usize) -> Result<Self, SegmentIndexErr> {
        let volume_index_size = volumes * size_of::<VolumeMeta>();
        assert!(data.len() >= volume_index_size);
        let (volume_index, page_offsets) = data.split_at(volume_index_size);

        let volume_index =
            <[VolumeMeta]>::try_ref_from_bytes(volume_index).map_err(|err| match err {
                ConvertError::Alignment(_) => SegmentIndexErr::InvalidAlignment,
                ConvertError::Size(_) => SegmentIndexErr::InvalidSize,
                ConvertError::Validity(_) => SegmentIndexErr::Corrupt,
            })?;

        let page_offsets =
            <[PageOffset]>::try_ref_from_bytes(page_offsets).map_err(|err| match err {
                ConvertError::Alignment(_) => SegmentIndexErr::InvalidAlignment,
                ConvertError::Size(_) => SegmentIndexErr::InvalidSize,
                ConvertError::Validity(_) => SegmentIndexErr::Corrupt,
            })?;

        Ok(Self { volume_index, page_offsets })
    }

    /// Lookup the local offset of a page by (VolumeId, PageOffset)
    /// The returned value can be used to index into the segment's page list
    pub fn lookup(&self, vid: &VolumeId, offset: PageOffset) -> Option<usize> {
        let meta_idx = self
            .volume_index
            .binary_search_by(|meta| meta.vid.cmp(vid))
            .ok()?;

        let meta = &self.volume_index[meta_idx];

        let start = meta.start.get() as usize;
        let end = start + meta.pages.as_usize();

        let relative_offset = self.page_offsets[start..end].binary_search(&offset).ok()?;

        Some(start + relative_offset)
    }
}

#[derive(Default)]
pub struct SegmentIndexBuilder {
    volume_index: BytesMut,
    page_offsets: BytesMut,

    current: Option<VolumeMeta>,
    pages: PageCount,
    last_offset: Option<PageOffset>,
}

impl SegmentIndexBuilder {
    pub fn new_with_capacity(num_volumes: usize, pages: PageCount) -> Self {
        Self {
            volume_index: BytesMut::with_capacity(num_volumes * size_of::<VolumeMeta>()),
            page_offsets: BytesMut::with_capacity(pages.as_usize() * size_of::<PageOffset>()),
            current: None,
            pages: PageCount::ZERO,
            last_offset: None,
        }
    }

    pub fn insert(&mut self, vid: VolumeId, offset: PageOffset) {
        let current = self.current.get_or_insert_with(|| VolumeMeta {
            vid: vid.clone(),
            start: self.pages.into(),
            pages: PageCount::ZERO,
        });

        // If the VolumeId has changed, write the current VolumeMeta to the volume_index
        if current.vid != vid {
            assert!(vid > current.vid, "Volumes must be inserted in order by ID");
            let last = std::mem::replace(
                current,
                VolumeMeta {
                    vid: vid.clone(),
                    start: self.pages.into(),
                    pages: PageCount::ZERO,
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
        current.add_page();
        self.pages.incr();
    }

    pub fn finish(self) -> impl Buf {
        let mut volume_index = self.volume_index;
        if let Some(current) = self.current {
            volume_index.extend_from_slice(current.as_bytes());
        }
        volume_index.freeze().chain(self.page_offsets.freeze())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn test_segment_index_sanity() {
        let mut builder = SegmentIndexBuilder::default();

        let mut vids = vec![VolumeId::random(), VolumeId::random(), VolumeId::random()];
        vids.sort();

        // insert 100 offsets for each vid
        for vid in &vids {
            for i in 0..100 {
                builder.insert(vid.clone(), PageOffset::new(i));
            }
        }

        let mut data = builder.finish();
        let bytes: Bytes = data.copy_to_bytes(data.remaining());
        let index =
            SegmentIndex::from_bytes(&bytes, vids.len()).expect("failed to load segment index");

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
