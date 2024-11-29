use std::{
    fmt::{Debug, Display},
    ops::{Range, RangeBounds},
};

use graft_core::{
    lsn::LSN,
    {SegmentId, VolumeId},
};
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, BE, U64};

#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes)]
#[repr(C, packed)]
pub struct CommitKey {
    vid: VolumeId,
    lsn: U64<BE>,
}

impl CommitKey {
    pub fn new(vid: VolumeId, lsn: LSN) -> Self {
        Self { vid, lsn: lsn.into() }
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    pub fn lsn(&self) -> LSN {
        self.lsn.into()
    }

    pub fn range<R: RangeBounds<LSN>>(vid: &VolumeId, lsns: &R) -> Range<CommitKey> {
        Range {
            start: match lsns.start_bound() {
                std::ops::Bound::Included(lsn) => CommitKey::new(vid.clone(), *lsn),
                std::ops::Bound::Excluded(lsn) => CommitKey::new(vid.clone(), lsn.next()),
                std::ops::Bound::Unbounded => CommitKey::new(vid.clone(), LSN::ZERO),
            },
            end: match lsns.end_bound() {
                std::ops::Bound::Included(lsn) => CommitKey::new(vid.clone(), lsn.next()),
                std::ops::Bound::Excluded(lsn) => CommitKey::new(vid.clone(), *lsn),
                std::ops::Bound::Unbounded => CommitKey::new(vid.clone(), LSN::MAX),
            },
        }
    }
}

impl AsRef<[u8]> for CommitKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Display for CommitKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.vid, self.lsn)
    }
}

impl Debug for CommitKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Clone for CommitKey {
    fn clone(&self) -> Self {
        Self { vid: self.vid.clone(), lsn: self.lsn }
    }
}

#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes)]
#[repr(C, packed)]
pub struct SegmentKey {
    commit: CommitKey,
    sid: SegmentId,
}

impl SegmentKey {
    pub fn new(commit: CommitKey, sid: SegmentId) -> Self {
        Self { commit, sid }
    }

    pub fn vid(&self) -> &VolumeId {
        &self.commit.vid
    }

    pub fn lsn(&self) -> LSN {
        self.commit.lsn.into()
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

impl Display for SegmentKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.commit, self.sid)
    }
}

impl Debug for SegmentKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Clone for SegmentKey {
    fn clone(&self) -> Self {
        Self {
            commit: self.commit.clone(),
            sid: self.sid.clone(),
        }
    }
}
