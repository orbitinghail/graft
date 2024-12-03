use std::time::{Duration, SystemTime};

use bytes::{BufMut, Bytes, BytesMut};
use graft_core::{
    gid::GidParseErr, lsn::LSN, page_count::PageCount, page_range::PageRange, SegmentId, VolumeId,
};
use graft_proto::common::v1::Snapshot;
use object_store::{path::Path, PutPayload};
use splinter::SplinterRef;
use thiserror::Error;
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, LittleEndian, TryFromBytes, U32, U64,
};

pub const COMMIT_MAGIC: U32<LittleEndian> = U32::from_bytes([0x31, 0x99, 0xBF, 0x00]);

pub fn commit_key_prefix(vid: &VolumeId) -> Path {
    Path::parse(format!("volumes/{}", vid.pretty())).expect("invalid object_store path")
}

pub fn commit_key(vid: &VolumeId, lsn: LSN) -> Path {
    commit_key_prefix(vid).child(lsn.format_fixed_hex())
}

fn time_to_millis(time: SystemTime) -> u64 {
    time.duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn millis_to_time(millis: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_millis(millis)
}

#[derive(Debug, Error)]
pub enum CommitKeyParseErr {
    #[error("invalid commit key structure: {0}")]
    Structure(Path),
    #[error("invalid volume id: {0}")]
    VolumeId(#[from] GidParseErr),
    #[error("invalid lsn: {0}")]
    Lsn(#[from] std::num::ParseIntError),
}

pub fn parse_commit_key(key: &Path) -> Result<(VolumeId, LSN), CommitKeyParseErr> {
    let mut parts = key.parts();
    if parts.next().as_ref().map(|p| p.as_ref()) != Some("volumes") {
        return Err(CommitKeyParseErr::Structure(key.clone()));
    }
    let vid: VolumeId = parts
        .next()
        .ok_or_else(|| CommitKeyParseErr::Structure(key.clone()))?
        .as_ref()
        .parse()?;
    let lsn: LSN = LSN::from_hex(
        parts
            .next()
            .ok_or_else(|| CommitKeyParseErr::Structure(key.clone()))?
            .as_ref(),
    )?;
    // ensure there are no trailing parts
    if parts.next().is_some() {
        return Err(CommitKeyParseErr::Structure(key.clone()));
    }
    Ok((vid, lsn))
}

#[derive(Clone, IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct CommitHeader {
    magic: U32<LittleEndian>,
    vid: VolumeId,
    meta: CommitMeta,
}

static_assertions::const_assert_eq!(size_of::<CommitHeader>(), 48);

#[derive(Clone, IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct CommitMeta {
    lsn: U64<LittleEndian>,
    checkpoint_lsn: U64<LittleEndian>,
    page_count: U32<LittleEndian>,
    timestamp: U64<LittleEndian>,
}

impl CommitMeta {
    pub fn new(lsn: LSN, checkpoint: LSN, page_count: PageCount, timestamp: SystemTime) -> Self {
        assert!(
            checkpoint <= lsn,
            "checkpoint must be less than or equal to lsn"
        );
        Self {
            lsn: lsn.into(),
            checkpoint_lsn: checkpoint.into(),
            page_count: page_count.into(),
            timestamp: time_to_millis(timestamp).into(),
        }
    }

    #[inline]
    pub fn lsn(&self) -> LSN {
        self.lsn.into()
    }

    #[inline]
    pub fn checkpoint(&self) -> LSN {
        self.checkpoint_lsn.into()
    }

    #[inline]
    pub fn page_count(&self) -> PageCount {
        self.page_count.into()
    }

    pub fn offsets(&self) -> PageRange {
        self.page_count().offsets()
    }

    #[inline]
    pub fn timestamp(&self) -> u64 {
        self.timestamp.get()
    }

    #[inline]
    pub fn system_time(&self) -> SystemTime {
        millis_to_time(self.timestamp())
    }

    pub fn into_snapshot(self, vid: &VolumeId) -> Snapshot {
        Snapshot::new(
            vid,
            self.lsn(),
            self.checkpoint(),
            self.page_count(),
            self.system_time(),
        )
    }
}

impl AsRef<[u8]> for CommitMeta {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Snapshot> for CommitMeta {
    fn from(snapshot: Snapshot) -> Self {
        Self::new(
            snapshot.lsn(),
            snapshot.checkpoint(),
            snapshot.page_count(),
            snapshot
                .system_time()
                .unwrap_or_default()
                .unwrap_or(SystemTime::UNIX_EPOCH),
        )
    }
}

#[derive(Clone, IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct OffsetsHeader {
    sid: SegmentId,
    size: U32<LittleEndian>,
}

#[derive(Default)]
pub struct CommitBuilder {
    offsets: BytesMut,
}

impl CommitBuilder {
    pub fn write_offsets(&mut self, sid: SegmentId, splinter: &[u8]) {
        let header = OffsetsHeader {
            sid,
            size: (splinter.len() as u32).into(),
        };
        self.offsets.put_slice(header.as_bytes());
        self.offsets.put_slice(splinter);
    }

    pub fn build(self, vid: VolumeId, meta: CommitMeta) -> Commit {
        let header = CommitHeader { magic: COMMIT_MAGIC, vid, meta };
        Commit { header, offsets: self.offsets.freeze() }
    }
}

#[derive(Debug, Error)]
pub enum CommitValidationErr {
    #[error("segment must be at least {} bytes", size_of::<CommitHeader>())]
    TooSmall,
    #[error("invalid magic number")]
    Magic,
}

#[derive(Clone)]
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

    pub fn into_payload(self) -> PutPayload {
        let header = Bytes::copy_from_slice(self.header.as_bytes());
        [header, self.offsets].into_iter().collect()
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
        if self.offsets.is_empty() {
            return Ok(None);
        }

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
