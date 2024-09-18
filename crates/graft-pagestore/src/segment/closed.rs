//! A closed segment is immutable and serialized. It can be directly mapped into memory and read from in an efficient way.

use anyhow::{anyhow, bail, Context};
use graft_core::{page::PAGESIZE, volume_id::VolumeId};
use odht::FxHashFn;
use zerocopy::{
    byteorder::little_endian::U32, little_endian::U16, AsBytes, ByteSlice, FromBytes, Ref,
};

pub const SEGMENT_MAGIC: U32 = U32::from_bytes([0xB8, 0x3B, 0x41, 0xC0]);
pub const SEGMENT_VERSION: u8 = 1;

// segments must be no larger than 16 MB
pub const SEGMENT_MAX_LEN: u32 = 1024 * 1024 * 16;
pub const SEGMENT_INLINE_INDEX_SIZE: usize = PAGESIZE - std::mem::size_of::<SegmentHeader>();

// the maximum number of pages a segment can store
const SEGMENT_MAX_PAGES: usize = SEGMENT_MAX_LEN as usize / PAGESIZE;

#[allow(non_camel_case_types)]
type rLSN = U32;

// an offset within a segment, in pages
type LocalOffset = U16;

// assert that local offset can address all of the segment's pages
static_assertions::assert_eq_size!(LocalOffset, u16);
static_assertions::const_assert!(SEGMENT_MAX_PAGES <= u16::MAX as usize);

#[derive(
    Clone, zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes, zerocopy::Unaligned,
)]
#[repr(C)]
pub struct SegmentHeader {
    magic: U32,
    version: u8,
    // offset of the index in bytes; 0 means the index is inline
    index_offset: LocalOffset,
    // size of the index in bytes
    index_size: U32,

    // pad to 16 bytes for nicer alignment (not required for safety)
    padding: [u8; 5],
}

#[derive(zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes, zerocopy::Unaligned)]
#[repr(C)]
pub struct SegmentHeaderPage {
    header: SegmentHeader,
    index: [u8; SEGMENT_INLINE_INDEX_SIZE],
}

static_assertions::const_assert_eq!(std::mem::size_of::<SegmentHeader>(), 16);
static_assertions::const_assert_eq!(std::mem::size_of::<SegmentHeaderPage>(), PAGESIZE);

impl SegmentHeaderPage {
    pub fn new(index_offset: u16, index_size: u32) -> Self {
        assert!(
            index_offset > 0 && (index_offset as usize) < SEGMENT_MAX_PAGES,
            "index_offset must be a local offset in pages between 1 and {SEGMENT_MAX_PAGES}"
        );
        Self {
            header: SegmentHeader {
                magic: SEGMENT_MAGIC,
                version: SEGMENT_VERSION,
                index_offset: LocalOffset::new(index_offset),
                index_size: U32::new(index_size),
                padding: [0; 5],
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
                index_offset: 0.into(),
                index_size: U32::new(index_bytes.len().try_into().unwrap()),
                padding: [0; 5],
            },
            index: [0; SEGMENT_INLINE_INDEX_SIZE],
        };
        page.index[..index_bytes.len()].copy_from_slice(index_bytes);
        page
    }
}

#[derive(zerocopy::AsBytes, zerocopy::FromBytes, zerocopy::FromZeroes, zerocopy::Unaligned)]
#[repr(C)]
pub struct SegmentIndexKey {
    vid: VolumeId,
    offset: U32,
    rlsn: rLSN,
}

impl SegmentIndexKey {
    pub fn new(vid: VolumeId, offset: u32, rlsn: u32) -> Self {
        Self {
            vid,
            offset: U32::new(offset),
            rlsn: rLSN::new(rlsn),
        }
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
    type EncodedKey = [u8; std::mem::size_of::<SegmentIndexKey>()];

    type Value = LocalOffset;
    type EncodedValue = [u8; std::mem::size_of::<LocalOffset>()];

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
        let data = vec![0; odht::bytes_needed::<SegmentIndex>(pages, 100)];
        Self {
            ht: odht::HashTable::init_in_place(data, pages, 100).unwrap(),
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

struct ClosedSegment<'a> {
    header: Ref<&'a [u8], SegmentHeader>,
    data: &'a [u8],
    index: odht::HashTable<SegmentIndex, &'a [u8]>,
}

impl<'a> ClosedSegment<'a> {
    pub fn from_bytes(data: &'a [u8]) -> anyhow::Result<ClosedSegment<'a>> {
        assert!(
            data.len() % PAGESIZE == 0,
            "data must be a multiple of PAGESIZE"
        );
        assert!(data.len() >= PAGESIZE, "data must be at least one page");
        assert!(
            data.len() <= SEGMENT_MAX_LEN as usize,
            "data must be no larger than SEGMENT_MAX_LEN"
        );

        let (header, rest) = Ref::<_, SegmentHeader>::new_unaligned_from_prefix(data).unwrap();

        if header.magic != SEGMENT_MAGIC {
            bail!("invalid magic");
        }
        if header.version != SEGMENT_VERSION {
            bail!("invalid version");
        }
        let index_offset = header.index_offset.get() as usize;
        let index_size = header.index_size.get() as usize;
        if index_offset < SEGMENT_MAX_PAGES {
            bail!("index_offset must be less than {SEGMENT_MAX_PAGES}");
        }
        let (data, index_bytes) = if index_offset == 0 {
            // index is inline
            if index_size < SEGMENT_INLINE_INDEX_SIZE {
                bail!("index out of bounds");
            }
            rest.split_at(index_size)
        } else {
            // we need to subtract 1 from the offset because data starts at the
            // first page after the header
            let index_offset_abs = (index_offset - 1) * PAGESIZE;
            if index_offset_abs + index_size < data.len() {
                bail!("index out of bounds");
            }
            rest.split_at(index_offset_abs)
        };
        let index = odht::HashTable::<SegmentIndex, _>::from_raw_bytes(index_bytes)
            .map_err(|err| anyhow!("invalid index: {err}"))?;

        Ok(Self { header, data, index })
    }
}
