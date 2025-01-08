//! A closed segment is immutable and serialized. It can be directly mapped into
//! memory and read from in an efficient way.

use std::fmt::Debug;

use culprit::{Culprit, ResultExt};
use graft_core::{
    byte_unit::ByteUnit,
    page::{Page, PAGESIZE},
    page_count::PageCount,
    page_offset::PageOffset,
    zerocopy_err::ZerocopyErr,
    SegmentId, VolumeId,
};
use thiserror::Error;
use zerocopy::{
    little_endian::{U16, U32},
    Immutable, IntoBytes, KnownLayout, TryFromBytes,
};

use crate::segment::index::SegmentIndex;

use super::index::SegmentIndexBuilder;

pub const SEGMENT_MAGIC: U32 = U32::from_bytes([0xB8, 0x3B, 0x41, 0x00]);

// segments must be no larger than 16 MB
pub const SEGMENT_MAX_SIZE: ByteUnit = ByteUnit::from_mb(16);

// the maximum number of pages a segment can store taking into
// account index/footer overhead
// This calculation is validated in test_segment_max_pages
pub const SEGMENT_MAX_PAGES: PageCount = PageCount::new(4090);

// the maximum number of volumes a segment can store pages for
pub const SEGMENT_MAX_VOLUMES: usize = 400;

#[derive(Clone, IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct SegmentFooter {
    /// the Segment id
    sid: SegmentId,

    /// number of volumes contained by the index
    volumes: U16,

    /// size of the index in bytes
    index_size: U16,

    /// pad the footer to 32 bytes for future use
    _padding: [u8; 8],

    /// the last 4 bytes of the footer are reserved for a magic number
    magic: U32,
}

static_assertions::assert_eq_size!(SegmentFooter, [u8; 32]);

impl SegmentFooter {
    pub fn new(sid: SegmentId, volumes: usize, index_size: ByteUnit) -> Self {
        assert!(volumes <= u16::MAX as usize);
        assert!(index_size.as_usize() <= u16::MAX as usize);
        Self {
            _padding: Default::default(),
            sid,
            volumes: U16::new(volumes as u16),
            index_size: U16::new(index_size.as_u16()),
            magic: SEGMENT_MAGIC,
        }
    }

    fn sid(&self) -> &SegmentId {
        &self.sid
    }

    fn volumes(&self) -> usize {
        self.volumes.get().into()
    }

    fn index_size(&self) -> ByteUnit {
        self.index_size.get().into()
    }
}

static_assertions::const_assert_eq!(size_of::<SegmentFooter>(), 32);

pub fn closed_segment_size(volumes: usize, pages: PageCount) -> ByteUnit {
    let index_size = SegmentIndexBuilder::serialized_size(volumes, pages);
    (PAGESIZE * pages.as_usize()) + index_size + size_of::<SegmentFooter>()
}

#[derive(Debug, Error)]
pub enum SegmentValidationErr {
    #[error("segment must be smaller than {} bytes", SEGMENT_MAX_SIZE)]
    TooLarge,
    #[error("segment is too small")]
    TooSmall,
    #[error("corrupt segment footer")]
    CorruptFooter(ZerocopyErr),
    #[error("invalid magic number")]
    Magic,
    #[error("corrupt segment index")]
    CorruptIndex(ZerocopyErr),
    #[error("page storage length must be a multiple of {}", PAGESIZE)]
    InvalidPageSize,
    #[error("segment has invalid page count")]
    InvalidPageCount,
}

pub struct ClosedSegment<'a> {
    page_data: &'a [u8],
    index: SegmentIndex<'a>,
    footer: &'a SegmentFooter,
}

impl<'a> ClosedSegment<'a> {
    pub fn from_bytes(data: &'a [u8]) -> Result<Self, Culprit<SegmentValidationErr>> {
        if data.len() > SEGMENT_MAX_SIZE {
            let size = ByteUnit::new(data.len() as u64);
            return Err(Culprit::new_with_note(SegmentValidationErr::TooLarge, format!("closed segment size {size} must be smaller than max segment size {SEGMENT_MAX_SIZE}")));
        }

        let (data, footer) = SegmentFooter::try_ref_from_suffix(data)
            .or_ctx(|err| SegmentValidationErr::CorruptFooter(err.into()))?;

        if footer.magic != SEGMENT_MAGIC {
            return Err(Culprit::new(SegmentValidationErr::Magic));
        }

        let (page_data, index_data) = data
            .split_at_checked(data.len() - footer.index_size().as_usize())
            .ok_or_else(|| Culprit::new(SegmentValidationErr::TooSmall))?;

        // load the index
        let index = SegmentIndex::from_bytes(index_data, footer.volumes())
            .or_ctx(|err| SegmentValidationErr::CorruptIndex(err.into()))?;

        // validate pages
        if page_data.len() % PAGESIZE != 0 {
            return Err(Culprit::new(SegmentValidationErr::InvalidPageSize));
        }
        if page_data.len() / PAGESIZE != index.pages().as_usize() {
            let actual = (page_data.len() / PAGESIZE).as_usize();
            let expected = index.pages().as_usize();
            return Err(Culprit::new_with_note(
                SegmentValidationErr::InvalidPageCount,
                format!("segment contains {actual} pages; expected {expected}"),
            ));
        }

        Ok(Self { page_data, index, footer })
    }

    pub fn pages(&self) -> PageCount {
        self.index.pages()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn sid(&self) -> &SegmentId {
        self.footer.sid()
    }

    pub fn find_page(&self, vid: VolumeId, offset: PageOffset) -> Option<Page> {
        self.index.lookup(&vid, offset).map(|local_offset| {
            let start = local_offset * PAGESIZE;
            let end = start + PAGESIZE;
            (&self.page_data[start.range(end)])
                .try_into()
                .expect("invalid page")
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&VolumeId, PageOffset, Page)> {
        self.index
            .iter()
            .zip(0usize..)
            .map(|((vid, offset), local_offset)| {
                let start = local_offset * PAGESIZE;
                let end = start + PAGESIZE;
                let page = (&self.page_data[start.range(end)])
                    .try_into()
                    .expect("invalid page");
                (vid, offset, page)
            })
    }
}

impl Debug for ClosedSegment<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClosedSegment")
            .field("pages", &self.pages())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use assert_matches::assert_matches;
    use bytes::{Buf, BufMut, BytesMut};
    use zerocopy::U16;

    use super::*;

    #[test]
    fn test_segment_validation() {
        // test a massive segment
        let buf = vec![0; SEGMENT_MAX_SIZE.as_usize() + 1];
        assert_matches!(
            ClosedSegment::from_bytes(&buf).unwrap_err().ctx(),
            SegmentValidationErr::TooLarge
        );

        // test an empty segment
        let buf = vec![];
        assert_matches!(
            ClosedSegment::from_bytes(&buf).unwrap_err().ctx(),
            SegmentValidationErr::CorruptFooter(ZerocopyErr::InvalidSize)
        );

        // test an all zero segment
        let buf = vec![0; size_of::<SegmentFooter>()];
        assert_matches!(
            ClosedSegment::from_bytes(&buf).unwrap_err().ctx(),
            SegmentValidationErr::CorruptFooter(ZerocopyErr::InvalidData)
        );

        // test a bad magic value
        let footer = SegmentFooter {
            sid: SegmentId::random(),
            volumes: U16::new(0),
            index_size: U16::new(0),
            _padding: Default::default(),
            magic: U32::from_bytes([0x00, 0x3B, 0x41, 0x00]),
        };
        assert_matches!(
            ClosedSegment::from_bytes(footer.as_bytes())
                .unwrap_err()
                .ctx(),
            SegmentValidationErr::Magic
        );

        // test a bad segment id
        let footer = SegmentFooter {
            sid: SegmentId::random(),
            volumes: U16::new(0),
            index_size: U16::new(0),
            _padding: Default::default(),
            magic: U32::from_bytes([0x00, 0x3B, 0x41, 0x00]),
        };
        let mut bytes: BytesMut = footer.as_bytes().into();
        bytes[0] = 0; // corrupt the segment id
        assert_matches!(
            ClosedSegment::from_bytes(bytes.as_bytes())
                .unwrap_err()
                .ctx(),
            SegmentValidationErr::CorruptFooter(ZerocopyErr::InvalidData)
        );

        // test page alignment err
        let footer = SegmentFooter {
            sid: SegmentId::random(),
            volumes: U16::new(0),
            index_size: U16::new(0),
            _padding: Default::default(),
            magic: SEGMENT_MAGIC,
        };
        let mut bytes = BytesMut::zeroed((PAGESIZE / 2).as_usize());
        bytes.extend_from_slice(footer.as_bytes());
        assert_matches!(
            ClosedSegment::from_bytes(&bytes).unwrap_err().ctx(),
            SegmentValidationErr::InvalidPageSize
        );

        // test invalid page count err
        let mut bytes = BytesMut::zeroed(PAGESIZE.as_usize());
        let mut index = SegmentIndexBuilder::default();
        let vid = VolumeId::random();
        index.insert(&vid, PageOffset::new(0));
        index.insert(&vid, PageOffset::new(1));
        let index = index.finish();
        let index_size = index.remaining();
        bytes.put(index);
        bytes.put_slice(
            SegmentFooter {
                sid: SegmentId::random(),
                volumes: U16::new(1),
                index_size: U16::new(index_size as u16),
                _padding: Default::default(),
                magic: SEGMENT_MAGIC,
            }
            .as_bytes(),
        );
        let bytes = bytes.freeze();
        assert_matches!(
            ClosedSegment::from_bytes(&bytes).unwrap_err().ctx(),
            SegmentValidationErr::InvalidPageCount
        );
    }

    #[test]
    fn test_segment_max_pages() {
        let size_at_max = closed_segment_size(SEGMENT_MAX_VOLUMES, SEGMENT_MAX_PAGES);
        println!("size_at_max: {:?}", size_at_max);
        // size_at_max should be within one page of the max segment size
        assert!(size_at_max <= SEGMENT_MAX_SIZE && size_at_max >= (SEGMENT_MAX_SIZE - PAGESIZE));
    }
}
