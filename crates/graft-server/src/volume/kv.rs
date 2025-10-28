use std::{
    fmt::{Debug, Display},
    ops::RangeInclusive,
};

use fjall::Slice;
use graft_core::{
    lsn::LSN,
    {SegmentId, VolumeId},
};
use zerocopy::{BE, Immutable, IntoBytes, KnownLayout, TryFromBytes, U64, Unaligned};

#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned)]
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
        self.lsn.try_into().expect("invalid LSN")
    }

    pub fn range(vid: &VolumeId, lsns: &RangeInclusive<LSN>) -> RangeInclusive<CommitKey> {
        let start = CommitKey::new(vid.clone(), *lsns.start());
        let end = CommitKey::new(vid.clone(), *lsns.end());
        start..=end
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

#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned)]
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
        self.commit.lsn.try_into().expect("invalid LSN")
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

impl From<SegmentKey> for Slice {
    fn from(key: SegmentKey) -> Slice {
        key.as_bytes().into()
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
