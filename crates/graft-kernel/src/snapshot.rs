use std::ops::RangeInclusive;

use graft_core::{
    VolumeId,
    lsn::{LSN, LSNRangeExt},
    volume_ref::VolumeRef,
};
use smallvec::SmallVec;

/// A `Snapshot` represents a logical view of a Volume, possibly made
/// up of LSN ranges from multiple physical Volumes.
#[derive(Clone, Hash, PartialEq, Eq)]
pub struct Snapshot {
    path: SmallVec<[VolumeRangeRef; 1]>,
}

/// A reference to a volume and a range of LSNs within that volume.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct VolumeRangeRef {
    pub vid: VolumeId,
    pub lsns: RangeInclusive<LSN>,
}

impl std::fmt::Debug for VolumeRangeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}[{}]", self.vid, self.lsns.to_string())
    }
}

impl VolumeRangeRef {
    pub fn start_ref(&self) -> VolumeRef {
        VolumeRef::new(self.vid.clone(), *self.lsns.start())
    }

    pub fn end_ref(&self) -> VolumeRef {
        VolumeRef::new(self.vid.clone(), *self.lsns.end())
    }
}

impl Snapshot {
    pub const EMPTY: Self = Self { path: SmallVec::new_const() };

    pub fn new(vid: VolumeId, lsns: RangeInclusive<LSN>) -> Self {
        assert!(!lsns.is_empty());
        Self {
            path: SmallVec::from_const([VolumeRangeRef { vid, lsns }]),
        }
    }

    pub fn head(&self) -> Option<(&VolumeId, LSN)> {
        self.path
            .first()
            .map(|entry| (&entry.vid, *entry.lsns.end()))
    }

    pub fn is_empty(&self) -> bool {
        self.path.is_empty()
    }

    pub fn append(&mut self, vid: VolumeId, lsns: RangeInclusive<LSN>) {
        assert!(!lsns.is_empty());
        self.path.push(VolumeRangeRef { vid, lsns });
    }

    /// iterate through all of the volume range references in the snapshot
    pub fn iter(&self) -> std::slice::Iter<'_, VolumeRangeRef> {
        self.path.iter()
    }
}

impl IntoIterator for Snapshot {
    type Item = VolumeRangeRef;
    type IntoIter = smallvec::IntoIter<[VolumeRangeRef; 1]>;
    fn into_iter(self) -> Self::IntoIter {
        self.path.into_iter()
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Snapshot").field(&self.path).finish()
    }
}
