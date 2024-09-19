//! A closed segment is immutable and serialized. It can be directly mapped into
//! memory and read from in an efficient way.

use std::{error::Error, fmt::Debug};

use anyhow::{anyhow, bail};
use graft_core::{
    offset::Offset,
    page::{PageRef, PAGESIZE},
    volume_id::VolumeId,
};
use odht::FxHashFn;
use thiserror::Error;
use zerocopy::{byteorder::little_endian::U32, little_endian::U16, AsBytes, FromBytes, Ref};

pub const SEGMENT_MAGIC: U32 = U32::from_bytes([0xB8, 0x3B, 0x41, 0xC0]);
pub const SEGMENT_VERSION: u8 = 1;

// segments must be no larger than 16 MB
pub const SEGMENT_MAX_LEN: usize = 1024 * 1024 * 16;
pub const SEGMENT_INLINE_INDEX_SIZE: usize = PAGESIZE - size_of::<SegmentHeader>();

// the maximum number of pages a segment can store taking into account index/header overhead
// calculated by hand via inspecting odht and current segment encoding
pub const SEGMENT_MAX_PAGES: usize = 4071;

// an offset within a segment, in pages
type LocalOffset = U16;

// assert that local offset can address all of the segment's pages
static_assertions::assert_eq_size!(LocalOffset, u16);
static_assertions::const_assert!(SEGMENT_MAX_PAGES <= u16::MAX as usize);

#[derive(Clone, zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes)]
#[repr(C)]
pub struct SegmentHeader {
    magic: U32,
    version: u8,
    // size of the index in bytes, if <= SEGMENT_INLINE_INDEX_SIZE the
    // index is stored inline
    index_size: U32,

    // pad to 16 bytes for nicer alignment (not required for safety)
    padding: [u8; 7],
}

#[derive(zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes)]
#[repr(C)]
pub struct SegmentHeaderPage {
    header: SegmentHeader,
    index: [u8; SEGMENT_INLINE_INDEX_SIZE],
}

static_assertions::const_assert_eq!(size_of::<SegmentHeader>(), 16);
static_assertions::const_assert_eq!(size_of::<SegmentHeaderPage>(), PAGESIZE);

impl SegmentHeaderPage {
    pub fn new(index_size: u32) -> Self {
        assert!(
            index_size > SEGMENT_INLINE_INDEX_SIZE as u32,
            "must use new_with_inline if index fits inline"
        );
        Self {
            header: SegmentHeader {
                magic: SEGMENT_MAGIC,
                version: SEGMENT_VERSION,
                index_size: U32::new(index_size),
                padding: [0; 7],
            },
            index: [0; SEGMENT_INLINE_INDEX_SIZE],
        }
    }

    pub fn new_with_inline_index(index: SegmentIndexBuilder) -> Self {
        let index_bytes = index.as_bytes();
        assert!(
            index_bytes.len() <= SEGMENT_INLINE_INDEX_SIZE,
            "index too large"
        );
        let mut page = Self {
            header: SegmentHeader {
                magic: SEGMENT_MAGIC,
                version: SEGMENT_VERSION,
                index_size: U32::new(index_bytes.len().try_into().unwrap()),
                padding: [0; 7],
            },
            index: [0; SEGMENT_INLINE_INDEX_SIZE],
        };
        page.index[..index_bytes.len()].copy_from_slice(index_bytes);
        page
    }
}

#[derive(zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes)]
#[repr(C)]
pub struct SegmentIndexKey {
    vid: VolumeId,
    offset: U32,
}

impl SegmentIndexKey {
    pub fn new(vid: VolumeId, offset: Offset) -> Self {
        Self { vid, offset: U32::new(offset) }
    }
}

pub struct SegmentIndex;

impl SegmentIndex {
    pub fn builder(pages: usize) -> SegmentIndexBuilder {
        SegmentIndexBuilder::new(pages)
    }
}

impl odht::Config for SegmentIndex {
    type H = FxHashFn;

    type Key = SegmentIndexKey;
    type EncodedKey = [u8; size_of::<SegmentIndexKey>()];

    type Value = LocalOffset;
    type EncodedValue = [u8; size_of::<LocalOffset>()];

    fn encode_key(k: &Self::Key) -> Self::EncodedKey {
        k.as_bytes().try_into().unwrap()
    }

    fn decode_key(k: &Self::EncodedKey) -> Self::Key {
        SegmentIndexKey::read_from(k).expect("invalid key")
    }

    fn encode_value(v: &Self::Value) -> Self::EncodedValue {
        v.as_bytes().try_into().unwrap()
    }

    fn decode_value(v: &Self::EncodedValue) -> Self::Value {
        LocalOffset::read_from(v).expect("invalid value")
    }
}

pub struct SegmentIndexBuilder {
    ht: odht::HashTable<SegmentIndex, Vec<u8>>,
}

impl SegmentIndexBuilder {
    fn new(pages: usize) -> Self {
        // we need to add 1 to the number of pages to account for how odht
        // handles slot allocation
        let data = vec![0; odht::bytes_needed::<SegmentIndex>(pages + 1, 100)];
        Self {
            ht: odht::HashTable::init_in_place(data, pages + 1, 100).unwrap(),
        }
    }

    #[inline]
    pub fn is_inline(&self) -> bool {
        self.ht.raw_bytes().len() <= SEGMENT_INLINE_INDEX_SIZE
    }

    /// Inserts the given key-value pair into the table.
    /// Panics if the table is full.
    #[inline]
    pub fn insert(&mut self, key: SegmentIndexKey, local_offset: u16) {
        assert!(
            (local_offset as usize) < SEGMENT_MAX_PAGES,
            "local_offset must be a local offset in pages smaller than {SEGMENT_MAX_PAGES}"
        );
        self.ht.insert(&key, &LocalOffset::new(local_offset));
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.ht.raw_bytes()
    }
}

#[derive(Debug, Error)]
pub enum SegmentValidationErr {
    #[error("segment must be at least {} bytes", PAGESIZE)]
    TooSmall,
    #[error("segment must be smaller than {} bytes", SEGMENT_MAX_LEN)]
    TooLarge,
    #[error("invalid magic number")]
    Magic,
    #[error("invalid version number")]
    Version,
    #[error("index size too large")]
    IndexSize,
    #[error("invalid index: {source}")]
    Index { source: Box<dyn Error> },
    #[error("page storage length must be a multiple of {}", PAGESIZE)]
    PageStorageSize,
}

pub struct ClosedSegment<'a> {
    pages: &'a [u8],
    index: odht::HashTable<SegmentIndex, &'a [u8]>,
}

impl<'a> ClosedSegment<'a> {
    pub fn from_bytes(data: &'a [u8]) -> Result<ClosedSegment<'a>, SegmentValidationErr> {
        if data.len() < PAGESIZE {
            return Err(SegmentValidationErr::TooSmall);
        }
        if data.len() > SEGMENT_MAX_LEN {
            return Err(SegmentValidationErr::TooLarge);
        }

        let (header, rest) = Ref::<_, SegmentHeader>::new_from_prefix(data).unwrap();

        if header.magic != SEGMENT_MAGIC {
            return Err(SegmentValidationErr::Magic);
        }
        if header.version != SEGMENT_VERSION {
            return Err(SegmentValidationErr::Version);
        }
        let index_size = header.index_size.get() as usize;
        let (pages, index_bytes) = if index_size <= SEGMENT_INLINE_INDEX_SIZE {
            // index is inline
            let (full_index, data) = rest.split_at(SEGMENT_INLINE_INDEX_SIZE);
            (data, &full_index[..index_size])
        } else {
            // the index is not inline; it is stored at the end of the segment
            // start by jumping to the end of the header page
            let (_, rest) = rest.split_at(SEGMENT_INLINE_INDEX_SIZE);
            if rest.len() < index_size {
                return Err(SegmentValidationErr::IndexSize);
            }
            rest.split_at(rest.len() - index_size)
        };
        let index = odht::HashTable::<SegmentIndex, _>::from_raw_bytes(index_bytes)
            .map_err(|source| SegmentValidationErr::Index { source })?;

        if pages.len() % PAGESIZE != 0 {
            return Err(SegmentValidationErr::PageStorageSize);
        }

        Ok(Self { pages, index })
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn find_page(&self, vid: VolumeId, offset: Offset) -> Option<PageRef<'_>> {
        let key = SegmentIndexKey::new(vid, offset);
        self.index.get(&key).map(|local_offset| {
            let start = local_offset.get() as usize * PAGESIZE;
            let end = start + PAGESIZE;
            (&self.pages[start..end]).try_into().expect("invalid page")
        })
    }
}

impl Debug for ClosedSegment<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosedSegment")
            .field("len", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};

    use super::*;

    #[test]
    fn test_segment_validation() {
        // test an empty segment
        let buf = io::Cursor::new(vec![0; 0]);
        assert!(matches!(
            ClosedSegment::from_bytes(&buf.into_inner()).unwrap_err(),
            SegmentValidationErr::TooSmall
        ));

        // test a massive segment
        let buf = io::Cursor::new(vec![0; SEGMENT_MAX_LEN + 1]);
        assert!(matches!(
            ClosedSegment::from_bytes(&buf.into_inner()).unwrap_err(),
            SegmentValidationErr::TooLarge
        ));

        // test an all zero segment
        let buf = io::Cursor::new(vec![0; PAGESIZE]);
        assert!(matches!(
            ClosedSegment::from_bytes(&buf.into_inner()).unwrap_err(),
            SegmentValidationErr::Magic
        ));

        // test a bad magic number
        let mut buf = io::Cursor::new(vec![0; PAGESIZE]);
        buf.write_all(
            SegmentHeader {
                magic: U32::new(0),
                version: SEGMENT_VERSION,
                index_size: U32::new(0),
                padding: [0; 7],
            }
            .as_bytes(),
        )
        .unwrap();
        assert!(matches!(
            ClosedSegment::from_bytes(&buf.into_inner()).unwrap_err(),
            SegmentValidationErr::Magic
        ));

        // test a bad version number
        let mut buf = io::Cursor::new(vec![0; PAGESIZE]);
        buf.write_all(
            SegmentHeader {
                magic: SEGMENT_MAGIC,
                version: SEGMENT_VERSION + 1,
                index_size: U32::new(0),
                padding: [0; 7],
            }
            .as_bytes(),
        )
        .unwrap();
        assert!(matches!(
            ClosedSegment::from_bytes(&buf.into_inner()).unwrap_err(),
            SegmentValidationErr::Version
        ));

        // test a bad index size
        let mut buf = io::Cursor::new(vec![0; PAGESIZE]);
        buf.write_all(
            SegmentHeader {
                magic: SEGMENT_MAGIC,
                version: SEGMENT_VERSION,
                index_size: U32::new((SEGMENT_INLINE_INDEX_SIZE as u32) + 1),
                padding: [0; 7],
            }
            .as_bytes(),
        )
        .unwrap();
        assert!(matches!(
            ClosedSegment::from_bytes(&buf.into_inner()).unwrap_err(),
            SegmentValidationErr::IndexSize
        ));

        // test a bad index
        let mut buf = io::Cursor::new(vec![0; PAGESIZE]);
        buf.write_all(
            SegmentHeader {
                magic: SEGMENT_MAGIC,
                version: SEGMENT_VERSION,
                index_size: U32::new(SEGMENT_INLINE_INDEX_SIZE as u32),
                padding: [0; 7],
            }
            .as_bytes(),
        )
        .unwrap();
        assert!(matches!(
            ClosedSegment::from_bytes(&buf.into_inner()).unwrap_err(),
            SegmentValidationErr::Index { .. }
        ));
    }
}
