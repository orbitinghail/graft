use std::ops::{Range, RangeInclusive};

use graft_core::{
    guid::{SegmentId, VolumeId},
    lsn::LSN,
    offset::Offset,
};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, BE, LE, U32, U64};

#[derive(KnownLayout, Immutable, FromBytes, IntoBytes)]
pub struct Snapshot {
    lsn: U64<LE>,
    last_offset: U32<LE>,
}

impl Snapshot {
    pub fn new(lsn: LSN, last_offset: Offset) -> Self {
        Self {
            lsn: U64::new(lsn),
            last_offset: U32::new(last_offset),
        }
    }

    pub fn lsn(&self) -> LSN {
        self.lsn.get()
    }

    pub fn last_offset(&self) -> Offset {
        self.last_offset.get()
    }
}

impl AsRef<[u8]> for Snapshot {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[derive(KnownLayout, Immutable, FromBytes, IntoBytes)]
pub struct SegmentKeyPrefix {
    vid: VolumeId,
    lsn: U64<BE>,
}

impl SegmentKeyPrefix {
    pub fn new(vid: VolumeId, lsn: LSN) -> Self {
        Self { vid, lsn: U64::new(lsn) }
    }

    pub fn range(vid: VolumeId, end_lsn: LSN) -> RangeInclusive<Self> {
        let start = Self::new(vid.clone(), 0);
        let end = Self::new(vid, end_lsn);
        start..=end
    }
}

impl AsRef<[u8]> for SegmentKeyPrefix {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[derive(KnownLayout, Immutable, FromBytes, IntoBytes)]
pub struct SegmentKey {
    prefix: SegmentKeyPrefix,
    sid: SegmentId,
}

impl SegmentKey {
    pub fn new(vid: VolumeId, lsn: LSN, sid: SegmentId) -> Self {
        Self {
            prefix: SegmentKeyPrefix::new(vid, lsn),
            sid,
        }
    }

    pub fn vid(&self) -> &VolumeId {
        &self.prefix.vid
    }

    pub fn lsn(&self) -> LSN {
        self.prefix.lsn.get()
    }

    pub fn sid(&self) -> &SegmentId {
        &self.sid
    }
}

impl AsRef<[u8]> for SegmentKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}
