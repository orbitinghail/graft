use bytes::{BufMut, Bytes, BytesMut};
use graft_core::{lsn::LSN, SegmentId, VolumeId};
use object_store::path::Path;
use splinter::SplinterRef;
use thiserror::Error;
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, LittleEndian, TryFromBytes, U32, U64,
};

pub const COMMIT_MAGIC: U32<LittleEndian> = U32::from_bytes([0x31, 0x99, 0xBF, 0x8D]);
pub const COMMIT_FORMAT: u8 = 1;

pub fn commit_key_prefix(vid: &VolumeId) -> Path {
    format!("volumes/{}/", vid.pretty()).into()
}

pub fn commit_key(vid: &VolumeId, lsn: LSN) -> Path {
    format!("{}/{:0>18x}", commit_key_prefix(vid), lsn).into()
}

#[derive(Clone, IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct CommitHeader {
    magic: U32<LittleEndian>,
    format: u8,
    vid: VolumeId,
    meta: CommitMeta,
}

#[derive(Clone, IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct CommitMeta {
    lsn: U64<LittleEndian>,
    last_offset: U32<LittleEndian>,
    timestamp: U64<LittleEndian>,
}

impl CommitMeta {
    pub fn new(lsn: LSN, last_offset: u32, timestamp: u64) -> Self {
        Self {
            lsn: lsn.into(),
            last_offset: last_offset.into(),
            timestamp: timestamp.into(),
        }
    }

    #[inline]
    pub fn lsn(&self) -> u64 {
        self.lsn.get()
    }

    #[inline]
    pub fn last_offset(&self) -> u32 {
        self.last_offset.get()
    }

    #[inline]
    pub fn timestamp(&self) -> u64 {
        self.timestamp.get()
    }
}

impl AsRef<[u8]> for CommitMeta {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[derive(Clone, IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct OffsetsHeader {
    sid: SegmentId,
    size: U32<LittleEndian>,
}

pub struct CommitBuilder {
    vid: VolumeId,
    lsn: LSN,
    buffer: BytesMut,
}

impl CommitBuilder {
    pub fn new(vid: VolumeId, lsn: LSN, last_offset: u32, timestamp: u64) -> Self {
        let mut buffer = BytesMut::default();
        let header = CommitHeader {
            magic: COMMIT_MAGIC,
            format: COMMIT_FORMAT,
            vid: vid.clone(),
            meta: CommitMeta {
                lsn: lsn.into(),
                last_offset: last_offset.into(),
                timestamp: timestamp.into(),
            },
        };
        buffer.put_slice(header.as_bytes());
        Self { vid, lsn, buffer }
    }

    pub fn write_offsets(&mut self, sid: SegmentId, splinter: &[u8]) {
        let header = OffsetsHeader {
            sid,
            size: (splinter.len() as u32).into(),
        };
        self.buffer.put_slice(header.as_bytes());
        self.buffer.put_slice(splinter);
    }

    pub fn freeze(self) -> (VolumeId, LSN, Bytes) {
        (self.vid, self.lsn, self.buffer.freeze())
    }
}

#[derive(Debug, Error)]
pub enum CommitValidationErr {
    #[error("segment must be at least {} bytes", size_of::<CommitHeader>())]
    TooSmall,
    #[error("invalid magic number")]
    Magic,
    #[error("invalid format version number")]
    FormatVersion,
}

pub struct Commit {
    header: CommitHeader,
    offsets: Bytes,
}

impl Commit {
    pub fn from_bytes(mut data: Bytes) -> Result<Self, CommitValidationErr> {
        let header = data.split_to(size_of::<CommitHeader>());
        let header = CommitHeader::try_read_from_bytes(&header)
            .map_err(|_| CommitValidationErr::TooSmall)?;

        if header.magic != COMMIT_MAGIC {
            return Err(CommitValidationErr::Magic);
        }
        if header.format != COMMIT_FORMAT {
            return Err(CommitValidationErr::FormatVersion);
        }

        Ok(Self { header, offsets: data })
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.header.vid
    }

    #[inline]
    pub fn meta(&self) -> &CommitMeta {
        &self.header.meta
    }

    pub fn iter_offsets(&self) -> OffsetsIter<'_> {
        OffsetsIter { offsets: &self.offsets }
    }
}

#[derive(Debug, Error)]
pub enum OffsetsValidationErr {
    #[error("offsets must be at least {} bytes", size_of::<OffsetsHeader>())]
    TooSmall,

    #[error(transparent)]
    SplinterDecodeErr(#[from] splinter::DecodeErr),
}

pub struct OffsetsIter<'a> {
    offsets: &'a [u8],
}

impl<'a> OffsetsIter<'a> {
    #[allow(clippy::type_complexity)]
    fn next_inner(
        &mut self,
    ) -> Result<Option<(&'a SegmentId, SplinterRef<&'a [u8]>)>, OffsetsValidationErr> {
        // read the next header
        let (header, rest) = OffsetsHeader::try_ref_from_prefix(self.offsets)
            .map_err(|_| OffsetsValidationErr::TooSmall)?;

        // read the splinter
        let (splinter, rest) = rest.split_at(header.size.get() as usize);
        let splinter = SplinterRef::from_bytes(splinter)?;

        // advance the offsets
        self.offsets = rest;

        Ok(Some((&header.sid, splinter)))
    }
}

impl<'a> Iterator for OffsetsIter<'a> {
    type Item = Result<(&'a SegmentId, SplinterRef<&'a [u8]>), OffsetsValidationErr>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner().transpose()
    }
}
