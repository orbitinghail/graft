//! Segment Index
//!
//! The Segment Index is made up of two sections. a Volume Index and a list of `PageIdxs`.
//!
//! The Volume Index is a list of (`VolumeId`, Start, Pages) tuples.
//!     `VolumeId`: The `VolumeId` for this set of pages
//!     Start: The position of the first page and `PageIdx` for this Volume
//!     Pages: The number of pages stored in this Segment for this Volume
//!
//! The `VolumeId` Table is sorted by `VolumeId`.
//!
//! The list of `PageIdxs` is stored in the same order as pages are stored in
//! this segment, and the index requires that each set of `PageIdxs` corresponding
//! to a Volume is sorted.

use std::mem::size_of;

use bytes::{Buf, Bytes, BytesMut};
use graft_core::{
    PageCount, PageIdx, VolumeId,
    byte_unit::ByteUnit,
    zerocopy_ext::{TryFromBytesExt, ZerocopyErr},
};
use zerocopy::{
    Immutable, IntoBytes, KnownLayout, LittleEndian, TryFromBytes, U16, U32, Unaligned,
};

use crate::bytes_vec::BytesVec;

#[derive(Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone, Unaligned)]
#[repr(C)]
struct VolumeMeta {
    vid: VolumeId,

    /// The position of the first page for this Volume in the segment
    start: U16<LittleEndian>,

    /// The number of pages stored in this Segment for this Volume
    pages: U16<LittleEndian>,
}

impl VolumeMeta {
    fn incr_pages(&mut self) {
        self.pages = self
            .pages
            .get()
            .checked_add(1)
            .expect("pages overflow")
            .into();
    }
}

pub struct SegmentIndex<'a> {
    volume_index: &'a [VolumeMeta],
    page_idxs: &'a [U32<LittleEndian>],
}

impl<'a> SegmentIndex<'a> {
    pub fn from_bytes(data: &'a [u8], volumes: usize) -> Result<Self, ZerocopyErr> {
        let volume_index_size = volumes * size_of::<VolumeMeta>();
        assert!(
            data.len() >= volume_index_size,
            "segment must be at least as long as the volume index"
        );
        let (volume_index, page_idxs) = data.split_at(volume_index_size);
        Ok(Self {
            volume_index: TryFromBytesExt::try_ref_from_unaligned_bytes(volume_index)?,
            page_idxs: TryFromBytesExt::try_ref_from_unaligned_bytes(page_idxs)?,
        })
    }

    pub fn pages(&self) -> PageCount {
        PageCount::new(self.page_idxs.len() as u32)
    }

    pub fn is_empty(&self) -> bool {
        self.page_idxs.is_empty()
    }

    /// Lookup the local offset of a page by (`VolumeId`, `PageIdx`)
    /// The returned value can be used to index into the segment's page list
    pub fn lookup(&self, vid: &VolumeId, pageidx: PageIdx) -> Option<usize> {
        let meta_idx = self
            .volume_index
            .binary_search_by(|meta| meta.vid.cmp(vid))
            .ok()?;

        let meta = &self.volume_index[meta_idx];

        let start = meta.start.get() as usize;
        let end = start + (meta.pages.get() as usize);

        let relative_offset = self.page_idxs[start..end]
            .binary_search(&pageidx.into())
            .ok()?;

        Some(start + relative_offset)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&VolumeId, PageIdx)> {
        self.volume_index.iter().flat_map(|meta| {
            let start = meta.start.get() as usize;
            let end = start + (meta.pages.get() as usize);
            self.page_idxs[start..end].iter().map(move |&pageidx| {
                (
                    &meta.vid,
                    PageIdx::try_from(pageidx).expect("bug: PageIdx should never be zero"),
                )
            })
        })
    }
}

struct TypedByteArray<T> {
    data: BytesMut,
    _marker: std::marker::PhantomData<T>,
}

impl<T: IntoBytes + Immutable> TypedByteArray<T> {
    fn serialized_size(count: usize) -> usize {
        count * size_of::<T>()
    }

    fn with_capacity(count: usize) -> Self {
        Self {
            data: BytesMut::with_capacity(count * size_of::<T>()),
            _marker: std::marker::PhantomData,
        }
    }

    fn push(&mut self, item: T) {
        self.data.extend_from_slice(item.as_bytes());
    }

    fn freeze(self) -> Bytes {
        assert_eq!(
            self.data.capacity() - self.data.remaining(),
            0,
            "not all data was consumed"
        );
        self.data.freeze()
    }
}

pub struct SegmentIndexBuilder {
    volume_index: TypedByteArray<VolumeMeta>,
    page_idxs: TypedByteArray<U32<LittleEndian>>,

    current: Option<VolumeMeta>,
    pages: u16,
    last_pageidx: Option<PageIdx>,
}

impl SegmentIndexBuilder {
    pub fn new_with_capacity(volumes: usize, pages: PageCount) -> Self {
        Self {
            volume_index: TypedByteArray::with_capacity(volumes),
            page_idxs: TypedByteArray::with_capacity(pages.to_usize()),
            current: None,
            pages: 0,
            last_pageidx: None,
        }
    }

    pub fn serialized_size(num_volumes: usize, num_pages: PageCount) -> ByteUnit {
        let volume_index_size = TypedByteArray::<VolumeMeta>::serialized_size(num_volumes) as u64;
        let page_idxs_size =
            TypedByteArray::<U32<LittleEndian>>::serialized_size(num_pages.to_usize()) as u64;
        ByteUnit::new(volume_index_size + page_idxs_size)
    }

    pub fn insert(&mut self, vid: &VolumeId, pageidx: PageIdx) {
        let current = self.current.get_or_insert_with(|| VolumeMeta {
            vid: vid.clone(),
            start: self.pages.into(),
            pages: 0.into(),
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
                    start: self.pages.into(),
                    pages: 0.into(),
                },
            );
            self.volume_index.push(last);
            self.last_pageidx = None;
        }

        // verify that pageidx is larger than the last pageidx, while also updating the last pageidx
        if let Some(last_pageidx) = self.last_pageidx.replace(pageidx) {
            assert!(
                pageidx > last_pageidx,
                "bug: pages must be inserted in order (per volume)"
            );
        }

        // Write the PageIdx to the page_idxs array
        self.page_idxs.push(pageidx.into());

        // Increment page counters
        current.incr_pages();
        self.pages = self.pages.checked_add(1).expect("pages overflow");
    }

    pub fn finish(self, out: &mut BytesVec) -> ByteUnit {
        let mut volume_index = self.volume_index;
        if let Some(current) = self.current {
            volume_index.push(current);
        }
        let (volume_index, page_idxs) = (volume_index.freeze(), self.page_idxs.freeze());
        let size = volume_index.len() + page_idxs.len();
        out.put(volume_index);
        out.put(page_idxs);
        size.into()
    }
}

#[cfg(test)]
mod tests {

    use bytes::Buf;

    use super::*;

    #[graft_test::test]
    fn test_segment_index_sanity() {
        let mut builder = SegmentIndexBuilder::new_with_capacity(3, 300.into());

        let mut vids = vec![VolumeId::random(), VolumeId::random(), VolumeId::random()];
        vids.sort();

        // insert 100 page indexes for each vid
        for vid in &vids {
            for i in 1..=100 {
                builder.insert(vid, PageIdx::new(i));
            }
        }

        let mut data = BytesVec::default();
        let size = builder.finish(&mut data);
        assert_eq!(data.remaining(), size);
        assert_eq!(
            size,
            SegmentIndexBuilder::serialized_size(vids.len(), 300.into()).as_usize()
        );
        let data = data.into_bytes();

        let index =
            SegmentIndex::from_bytes(&data, vids.len()).expect("failed to load segment index");

        // lookup all the offsets
        for (volume_offset, vid) in vids.iter().enumerate() {
            for i in 0..100 {
                let pageidx = PageIdx::new(i + 1);
                let local_offset = index.lookup(vid, pageidx).expect("page not found");
                assert_eq!(local_offset, (volume_offset * 100) + i as usize);
            }
        }
    }
}
