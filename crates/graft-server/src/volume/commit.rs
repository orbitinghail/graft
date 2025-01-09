use std::{
    num::ParseIntError,
    time::{Duration, SystemTime},
};

use bytes::{BufMut, Bytes, BytesMut};
use culprit::{Culprit, ResultExt};
use fjall::Slice;
use graft_core::{
    gid::GidParseErr, lsn::LSN, page_count::PageCount, page_range::PageRange,
    zerocopy_err::ZerocopyErr, SegmentId, VolumeId,
};
use graft_proto::common::v1::Snapshot;
use object_store::{path::Path, PutPayload};
use splinter::SplinterRef;
use thiserror::Error;
use zerocopy::{
    ConvertError, FromBytes, Immutable, IntoBytes, KnownLayout, LittleEndian, TryFromBytes, U32,
    U64,
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
    #[error("invalid commit key structure")]
    InvalidStructure,
    #[error("invalid volume id: {0}")]
    InvalidVolumeId(#[from] GidParseErr),
    #[error("invalid lsn")]
    InvalidLsn,
}

impl From<ParseIntError> for CommitKeyParseErr {
    fn from(_: ParseIntError) -> Self {
        Self::InvalidLsn
    }
}

pub fn parse_commit_key(key: &Path) -> Result<(VolumeId, LSN), Culprit<CommitKeyParseErr>> {
    macro_rules! invalid_key {
        () => {
            Culprit::new_with_note(
                CommitKeyParseErr::InvalidStructure,
                format!("invalid key: {key}"),
            )
        };
    }

    let mut parts = key.parts();
    if parts.next().as_ref().map(|p| p.as_ref()) != Some("volumes") {
        return Err(invalid_key!());
    }
    let vid: VolumeId = parts
        .next()
        .ok_or_else(|| invalid_key!())?
        .as_ref()
        .parse()?;
    let lsn: LSN = LSN::from_hex(parts.next().ok_or_else(|| invalid_key!())?.as_ref())?;
    // ensure there are no trailing parts
    if parts.next().is_some() {
        return Err(invalid_key!());
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

impl Into<Slice> for CommitMeta {
    fn into(self) -> Slice {
        self.as_bytes().into()
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
    #[error("corrupt commit header: {0}")]
    Corrupt(#[from] ZerocopyErr),

    #[error("serialized commit is too short")]
    InvalidLength,

    #[error("invalid magic number")]
    Magic,
}

impl<A, S, V> From<ConvertError<A, S, V>> for CommitValidationErr {
    #[inline]
    #[track_caller]
    fn from(value: ConvertError<A, S, V>) -> Self {
        Self::Corrupt(value.into())
    }
}

#[derive(Clone)]
pub struct Commit {
    header: CommitHeader,
    offsets: Bytes,
}

impl Commit {
    pub fn from_bytes(mut data: Bytes) -> Result<Self, Culprit<CommitValidationErr>> {
        if data.len() < size_of::<CommitHeader>() {
            return Err(Culprit::new(CommitValidationErr::InvalidLength));
        }
        let header = data.split_to(size_of::<CommitHeader>());
        let header = CommitHeader::try_read_from_bytes(&header)?;

        if header.magic != COMMIT_MAGIC {
            return Err(Culprit::new(CommitValidationErr::Magic));
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

    pub fn iter_offsets(&self) -> OffsetsIter {
        OffsetsIter { offsets: self.offsets.clone() }
    }

    pub fn into_payload(self) -> PutPayload {
        let header = Bytes::copy_from_slice(self.header.as_bytes());
        [header, self.offsets].into_iter().collect()
    }
}

#[derive(Debug, Error)]
pub enum OffsetsValidationErr {
    #[error("corrupt offsets header: {0}")]
    CorruptHeader(#[from] ZerocopyErr),

    #[error("invalid commit size")]
    InvalidSize,

    #[error("invalid splinter: {0}")]
    SplinterDecodeErr(#[from] splinter::DecodeErr),
}

impl<A, S, V> From<ConvertError<A, S, V>> for OffsetsValidationErr {
    #[inline]
    #[track_caller]
    fn from(value: ConvertError<A, S, V>) -> Self {
        Self::CorruptHeader(value.into())
    }
}

pub struct OffsetsIter {
    offsets: Bytes,
}

impl OffsetsIter {
    #[allow(clippy::type_complexity)]
    fn next_inner(
        &mut self,
    ) -> Result<Option<(SegmentId, SplinterRef<Bytes>)>, Culprit<OffsetsValidationErr>> {
        if self.offsets.is_empty() {
            return Ok(None);
        }

        // read the next header
        if self.offsets.len() < std::mem::size_of::<OffsetsHeader>() {
            return Err(Culprit::new_with_note(
                OffsetsValidationErr::InvalidSize,
                "header size is larger than remaining offsets data",
            ));
        }
        let header = self.offsets.split_to(std::mem::size_of::<OffsetsHeader>());
        let header = OffsetsHeader::try_read_from_bytes(&header)?;

        // read the splinter
        let splinter_len = header.size.get() as usize;
        if self.offsets.len() < splinter_len {
            return Err(Culprit::new_with_note(
                OffsetsValidationErr::InvalidSize,
                "splinter size is larger than remaining offsets data",
            ));
        }
        let splinter = self.offsets.split_to(splinter_len);
        let splinter = SplinterRef::from_bytes(splinter).or_into_ctx()?;

        Ok(Some((header.sid, splinter)))
    }
}

impl Iterator for OffsetsIter {
    type Item = Result<(SegmentId, SplinterRef<Bytes>), Culprit<OffsetsValidationErr>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner().transpose()
    }
}
