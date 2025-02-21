use std::{
    fmt::Debug,
    iter::once,
    num::ParseIntError,
    time::{Duration, SystemTime},
};

use bytes::{Buf, Bytes};
use culprit::{Culprit, ResultExt};
use fjall::Slice;
use graft_core::{
    gid::{ClientId, GidParseErr},
    lsn::{InvalidLSN, LSN},
    page_count::PageCount,
    zerocopy_ext::ZerocopyErr,
    SegmentId, VolumeId,
};
use graft_proto::common::v1::Snapshot;
use object_store::{path::Path, PutPayload};
use prost_types::TimestampError;
use splinter::SplinterRef;
use thiserror::Error;
use zerocopy::{ConvertError, Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::bytes_vec::BytesVec;

pub fn commit_key_path_prefix(vid: &VolumeId) -> Path {
    Path::parse(format!("volumes/{}", vid.pretty())).expect("invalid object_store path")
}

pub fn commit_key_path(vid: &VolumeId, lsn: LSN) -> Path {
    commit_key_path_prefix(vid).child(lsn.format_fixed_hex())
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
#[repr(u32)]
enum CommitMagic {
    Magic = 0x71DB116B,
}

impl Debug for CommitMagic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CommitMagic")
    }
}

#[derive(Clone, IntoBytes, TryFromBytes, Immutable, KnownLayout, Debug)]
#[repr(C)]
pub struct CommitMeta {
    magic: CommitMagic,
    page_count: PageCount,
    vid: VolumeId,
    cid: ClientId,
    lsn: LSN,
    checkpoint_lsn: LSN,
    timestamp: u64,
}

static_assertions::const_assert_eq!(size_of::<CommitMeta>(), 64);

impl CommitMeta {
    pub fn new(
        vid: VolumeId,
        cid: ClientId,
        lsn: LSN,
        checkpoint: LSN,
        page_count: PageCount,
        timestamp: SystemTime,
    ) -> Self {
        assert!(
            checkpoint <= lsn,
            "checkpoint must be less than or equal to lsn"
        );
        Self {
            magic: CommitMagic::Magic,
            vid,
            cid,
            lsn: lsn.into(),
            checkpoint_lsn: checkpoint.into(),
            page_count: page_count.into(),
            timestamp: time_to_millis(timestamp).into(),
        }
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    pub fn cid(&self) -> &ClientId {
        &self.cid
    }

    #[inline]
    pub fn lsn(&self) -> LSN {
        self.lsn
    }

    #[inline]
    pub fn checkpoint(&self) -> LSN {
        self.checkpoint_lsn
    }

    #[inline]
    pub fn page_count(&self) -> PageCount {
        self.page_count
    }

    #[inline]
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    #[inline]
    pub fn system_time(&self) -> SystemTime {
        millis_to_time(self.timestamp())
    }

    pub fn into_snapshot(self) -> Snapshot {
        Snapshot::new(
            &self.vid,
            &self.cid,
            self.lsn,
            self.checkpoint_lsn,
            self.page_count,
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

#[derive(Debug, Error)]
pub enum TryFromSnapshotErr {
    #[error("invalid client id: {0}")]
    InvalidCid(#[from] GidParseErr),

    #[error("invalid lsn: {0}")]
    InvalidLsn(#[from] InvalidLSN),

    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(#[from] TimestampError),
}

impl TryFrom<Snapshot> for CommitMeta {
    type Error = Culprit<TryFromSnapshotErr>;

    fn try_from(snapshot: Snapshot) -> Result<Self, Self::Error> {
        let ts = snapshot.system_time()?.unwrap_or(SystemTime::UNIX_EPOCH);
        Ok(Self::new(
            snapshot.vid().cloned().or_into_ctx()?,
            snapshot.cid().cloned().or_into_ctx()?,
            snapshot.lsn().or_into_ctx()?,
            snapshot.checkpoint().or_into_ctx()?,
            snapshot.pages(),
            ts,
        ))
    }
}

#[derive(Clone, IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct GraftHeader {
    sid: SegmentId,
    size: u32,
}

pub struct CommitBuilder {
    meta: CommitMeta,
    grafts: BytesVec,
}

impl CommitBuilder {
    pub fn new_with_capacity(meta: CommitMeta, capacity: usize) -> Self {
        Self {
            meta,
            grafts: BytesVec::with_capacity(capacity),
        }
    }

    pub fn write_graft(&mut self, sid: SegmentId, graft: Bytes) {
        let header = GraftHeader {
            sid,
            size: graft.len().try_into().expect("bug: splinter too large"),
        };
        self.grafts.put_slice(header.as_bytes());
        self.grafts.put(graft);
    }

    pub fn build(self) -> Commit<BytesVec> {
        Commit { header: self.meta, grafts: self.grafts }
    }
}

#[derive(Debug, Error)]
#[error("corrupt commit header: {0}")]
pub struct CommitValidationErr(ZerocopyErr);

impl<A, S, V> From<ConvertError<A, S, V>> for CommitValidationErr {
    #[inline]
    #[track_caller]
    fn from(value: ConvertError<A, S, V>) -> Self {
        Self(value.into())
    }
}

#[derive(Clone)]
pub struct Commit<T> {
    header: CommitMeta,
    grafts: T,
}

impl<T> Commit<T> {
    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.header.vid
    }

    #[inline]
    pub fn meta(&self) -> &CommitMeta {
        &self.header
    }

    pub fn into_snapshot(self) -> Snapshot {
        self.header.into_snapshot()
    }
}

impl<T: Buf + Clone> Commit<T> {
    pub fn from_bytes(mut data: T) -> Result<Self, Culprit<CommitValidationErr>> {
        if data.remaining() < size_of::<CommitMeta>() {
            return Err(Culprit::new(CommitValidationErr(ZerocopyErr::InvalidSize)));
        }
        let header = data.copy_to_bytes(size_of::<CommitMeta>());
        let header = CommitMeta::try_read_from_bytes(&header)?;

        Ok(Self { header, grafts: data })
    }

    pub fn iter_grafts(&self) -> GraftIter<T> {
        GraftIter { grafts: self.grafts.clone() }
    }
}

impl Commit<BytesVec> {
    pub fn into_payload(self) -> PutPayload {
        let header = Bytes::copy_from_slice(self.header.as_bytes());
        once(header).chain(self.grafts.into_iter()).collect()
    }
}

#[derive(Debug, Error)]
pub enum GraftValidationErr {
    #[error("corrupt graft header: {0}")]
    CorruptHeader(#[from] ZerocopyErr),

    #[error("invalid commit size")]
    InvalidSize,

    #[error("invalid splinter: {0}")]
    SplinterDecodeErr(#[from] splinter::DecodeErr),
}

impl<A, S, V> From<ConvertError<A, S, V>> for GraftValidationErr {
    #[inline]
    #[track_caller]
    fn from(value: ConvertError<A, S, V>) -> Self {
        Self::CorruptHeader(value.into())
    }
}

pub struct GraftIter<T> {
    grafts: T,
}

impl<T: Buf> GraftIter<T> {
    fn next_inner(
        &mut self,
    ) -> Result<Option<(SegmentId, SplinterRef<Bytes>)>, Culprit<GraftValidationErr>> {
        if !self.grafts.has_remaining() {
            return Ok(None);
        }

        // read the next header
        if self.grafts.remaining() < std::mem::size_of::<GraftHeader>() {
            return Err(Culprit::new_with_note(
                GraftValidationErr::InvalidSize,
                "header size is larger than remaining data",
            ));
        }
        let header = self
            .grafts
            .copy_to_bytes(std::mem::size_of::<GraftHeader>());
        let header = GraftHeader::try_read_from_bytes(&header)?;

        // read the splinter
        let splinter_len = header.size as usize;
        if self.grafts.remaining() < splinter_len {
            return Err(Culprit::new_with_note(
                GraftValidationErr::InvalidSize,
                "graft size is larger than remaining data",
            ));
        }
        let splinter = self.grafts.copy_to_bytes(splinter_len);
        let splinter = SplinterRef::from_bytes(splinter).or_into_ctx()?;

        Ok(Some((header.sid, splinter)))
    }
}

impl<T: Buf> Iterator for GraftIter<T> {
    type Item = Result<(SegmentId, SplinterRef<Bytes>), Culprit<GraftValidationErr>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner().transpose()
    }
}
