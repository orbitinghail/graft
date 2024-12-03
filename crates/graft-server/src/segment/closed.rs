//! A closed segment is immutable and serialized. It can be directly mapped into
//! memory and read from in an efficient way.

use std::fmt::Debug;

use graft_core::{
    byte_unit::ByteUnit,
    page::{Page, PAGESIZE},
    page_offset::PageOffset,
    VolumeId,
};
use odht::FxHashFn;
use thiserror::Error;
use zerocopy::{
    byteorder::little_endian::U32, little_endian::U16, FromBytes, Immutable, IntoBytes,
    KnownLayout, TryFromBytes,
};

pub const SEGMENT_MAGIC: U32 = U32::from_bytes([0xB8, 0x3B, 0x41, 0x00]);

// segments must be no larger than 16 MB
pub const SEGMENT_MAX_SIZE: ByteUnit = ByteUnit::from_mb(16);
pub const SEGMENT_INLINE_INDEX_SIZE: ByteUnit = PAGESIZE.diff(ByteUnit::size_of::<SegmentHeader>());

// the maximum number of pages a segment can store taking into account index/header overhead
// calculated by hand via inspecting odht and current segment encoding
// This calculation is validated in test_segment_max_pages
pub const SEGMENT_MAX_PAGES: usize = 4071;

// an offset within a segment, in pages
type LocalOffset = U16;

// assert that local offset can address all of the segment's pages
static_assertions::assert_eq_size!(LocalOffset, u16);
static_assertions::const_assert!(SEGMENT_MAX_PAGES <= u16::MAX as usize);

#[derive(Clone, IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct SegmentHeader {
    magic: U32,
    // size of the index in bytes, if <= SEGMENT_INLINE_INDEX_SIZE the
    // index is stored inline
    index_size: U32,

    // pad to 16 bytes for nicer alignment (not required for safety)
    _padding: [u8; 8],
}

#[derive(IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct SegmentHeaderPage {
    header: SegmentHeader,
    index: [u8; SEGMENT_INLINE_INDEX_SIZE.as_usize()],
}

static_assertions::const_assert_eq!(size_of::<SegmentHeader>(), 16);
static_assertions::const_assert_eq!(size_of::<SegmentHeaderPage>(), PAGESIZE.as_usize());

impl SegmentHeaderPage {
    pub fn new(index_size: ByteUnit) -> Self {
        assert!(
            index_size > SEGMENT_INLINE_INDEX_SIZE,
            "must use new_with_inline if index fits inline"
        );
        Self {
            header: SegmentHeader {
                magic: SEGMENT_MAGIC,
                index_size: U32::new(index_size.as_u32()),
                _padding: Default::default(),
            },
            index: [0; SEGMENT_INLINE_INDEX_SIZE.as_usize()],
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
                index_size: U32::new(index_bytes.len().try_into().unwrap()),
                _padding: Default::default(),
            },
            index: [0; SEGMENT_INLINE_INDEX_SIZE.as_usize()],
        };
        page.index[..index_bytes.len()].copy_from_slice(index_bytes);
        page
    }
}

#[derive(IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct SegmentIndexKey {
    vid: VolumeId,
    offset: U32,
}

impl SegmentIndexKey {
    pub fn new(vid: VolumeId, offset: PageOffset) -> Self {
        Self { vid, offset: offset.into() }
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
        SegmentIndexKey::try_read_from_bytes(k).expect("invalid key")
    }

    fn encode_value(v: &Self::Value) -> Self::EncodedValue {
        v.as_bytes().try_into().unwrap()
    }

    fn decode_value(v: &Self::EncodedValue) -> Self::Value {
        LocalOffset::read_from_bytes(v).expect("invalid value")
    }
}

pub struct SegmentIndexBuilder {
    ht: odht::HashTable<SegmentIndex, Vec<u8>>,
}

impl SegmentIndexBuilder {
    fn new(pages: usize) -> Self {
        let data = vec![0; Self::size(pages).as_usize()];
        Self {
            // we need to add 1 to the number of pages to account for how odht
            // handles slot allocation
            ht: odht::HashTable::init_in_place(data, pages + 1, 100).unwrap(),
        }
    }

    pub fn size(pages: usize) -> ByteUnit {
        // we need to add 1 to the number of pages to account for how odht
        // handles slot allocation
        odht::bytes_needed::<SegmentIndex>(pages + 1, 100).into()
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
            "local_offset must be in the range 0..{SEGMENT_MAX_PAGES}"
        );
        self.ht.insert(&key, &LocalOffset::new(local_offset));
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self.ht.raw_bytes()
    }
}

pub fn closed_segment_size(pages: usize) -> ByteUnit {
    let index_size = SegmentIndexBuilder::size(pages);
    if index_size <= SEGMENT_INLINE_INDEX_SIZE {
        size_of::<SegmentHeaderPage>() + (PAGESIZE * pages)
    } else {
        size_of::<SegmentHeaderPage>() + (PAGESIZE * pages) + index_size
    }
}

#[derive(Debug, Error)]
pub enum SegmentValidationErr {
    #[error("segment must be at least {} bytes", PAGESIZE)]
    TooSmall,
    #[error("segment must be smaller than {} bytes", SEGMENT_MAX_SIZE)]
    TooLarge,
    #[error("invalid magic number")]
    Magic,
    #[error("index size too large")]
    IndexSize,
    #[error("invalid index: {error}")]
    Index { error: String },
    #[error("page storage length must be a multiple of {}", PAGESIZE)]
    PageStorageSize,
}

pub struct ClosedSegment<'a> {
    page_data: &'a [u8],
    index: odht::HashTable<SegmentIndex, &'a [u8]>,
}

impl<'a> ClosedSegment<'a> {
    pub fn from_bytes(data: &'a [u8]) -> Result<Self, SegmentValidationErr> {
        if data.len() < PAGESIZE {
            return Err(SegmentValidationErr::TooSmall);
        }
        if data.len() > SEGMENT_MAX_SIZE {
            return Err(SegmentValidationErr::TooLarge);
        }

        let (header, rest) = SegmentHeader::try_ref_from_prefix(data).unwrap();

        if header.magic != SEGMENT_MAGIC {
            return Err(SegmentValidationErr::Magic);
        }
        let index_size: ByteUnit = header.index_size.get().into();
        let (page_data, index_bytes) = if index_size <= SEGMENT_INLINE_INDEX_SIZE {
            // index is inline
            let (full_index, data) = rest.split_at(SEGMENT_INLINE_INDEX_SIZE.as_usize());
            (data, &full_index[..index_size.as_usize()])
        } else {
            // the index is not inline; it is stored at the end of the segment
            // start by jumping to the end of the header page
            let (_, rest) = rest.split_at(SEGMENT_INLINE_INDEX_SIZE.as_usize());
            if rest.len() < index_size {
                return Err(SegmentValidationErr::IndexSize);
            }
            rest.split_at((rest.len() - index_size).as_usize())
        };
        let index = odht::HashTable::<SegmentIndex, _>::from_raw_bytes(index_bytes)
            .map_err(|source| SegmentValidationErr::Index { error: source.to_string() })?;

        if page_data.len() % PAGESIZE != 0 {
            return Err(SegmentValidationErr::PageStorageSize);
        }

        Ok(Self { page_data, index })
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn find_page(&self, vid: VolumeId, offset: PageOffset) -> Option<Page> {
        let key = SegmentIndexKey::new(vid, offset);
        self.index.get(&key).map(|local_offset| {
            let start = local_offset.get() * PAGESIZE;
            let end = start + PAGESIZE;
            (&self.page_data[start.range(end)])
                .try_into()
                .expect("invalid page")
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (VolumeId, PageOffset, Page)> + '_ {
        self.index.iter().map(move |(key, local_offset)| {
            let start = local_offset.get() * PAGESIZE;
            let end = start + PAGESIZE;
            let page = (&self.page_data[start.range(end)])
                .try_into()
                .expect("invalid page");
            (key.vid, key.offset.into(), page)
        })
    }
}

impl Debug for ClosedSegment<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosedSegment")
            .field("pages", &self.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::io::{self, Write};

    use super::*;

    fn mk_cursor(size: impl Into<ByteUnit>) -> io::Cursor<Vec<u8>> {
        io::Cursor::new(vec![0; size.into().as_usize()])
    }

    #[test]
    fn test_segment_validation() {
        // test an empty segment
        let buf = mk_cursor(0);
        assert!(matches!(
            ClosedSegment::from_bytes(&buf.into_inner()).unwrap_err(),
            SegmentValidationErr::TooSmall
        ));

        // test a massive segment
        let buf = mk_cursor(SEGMENT_MAX_SIZE + 1);
        assert!(matches!(
            ClosedSegment::from_bytes(&buf.into_inner()).unwrap_err(),
            SegmentValidationErr::TooLarge
        ));

        // test an all zero segment
        let buf = mk_cursor(PAGESIZE);
        assert!(matches!(
            ClosedSegment::from_bytes(&buf.into_inner()).unwrap_err(),
            SegmentValidationErr::Magic
        ));

        // test a bad magic number
        let mut buf = mk_cursor(PAGESIZE);
        buf.write_all(
            SegmentHeader {
                magic: U32::new(0),
                index_size: U32::new(0),
                _padding: Default::default(),
            }
            .as_bytes(),
        )
        .unwrap();
        assert!(matches!(
            ClosedSegment::from_bytes(&buf.into_inner()).unwrap_err(),
            SegmentValidationErr::Magic
        ));

        // test a bad index size
        let mut buf = mk_cursor(PAGESIZE);
        buf.write_all(
            SegmentHeader {
                magic: SEGMENT_MAGIC,
                index_size: U32::new((SEGMENT_INLINE_INDEX_SIZE.as_u32()) + 1),
                _padding: Default::default(),
            }
            .as_bytes(),
        )
        .unwrap();
        assert!(matches!(
            ClosedSegment::from_bytes(&buf.into_inner()).unwrap_err(),
            SegmentValidationErr::IndexSize
        ));

        // test a bad index
        let mut buf = mk_cursor(PAGESIZE);
        buf.write_all(
            SegmentHeader {
                magic: SEGMENT_MAGIC,
                index_size: U32::new(SEGMENT_INLINE_INDEX_SIZE.as_u32()),
                _padding: Default::default(),
            }
            .as_bytes(),
        )
        .unwrap();
        assert!(matches!(
            ClosedSegment::from_bytes(&buf.into_inner()).unwrap_err(),
            SegmentValidationErr::Index { .. }
        ));
    }

    #[test]
    fn test_segment_max_pages() {
        let index_size = SegmentIndexBuilder::size(SEGMENT_MAX_PAGES);
        let mut index_pages = (index_size / PAGESIZE).as_usize();
        if index_size % PAGESIZE > 0 {
            index_pages += 1;
        }

        // if we add up the index pages, the header page, and the maximum number
        // of data pages it should equal the total number of pages that can fix
        // in a segment
        assert_eq!(
            index_pages + 1 + SEGMENT_MAX_PAGES,
            (SEGMENT_MAX_SIZE / PAGESIZE).as_usize(),
        );
    }
}
