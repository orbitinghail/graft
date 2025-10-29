use std::ops::RangeInclusive;

use graft_core::{VolumeId, lsn::LSN};
use smallvec::SmallVec;

/// A `SearchPath` represents a ordered set of Volumes along with a LSN range for
/// each Volume. `SearchPaths` are used to search for the latest commit containing
/// a page. Volumes appearing earlier in a `SearchPath` will shadow Volumes
/// appearing later.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchPath {
    path: SmallVec<[PathEntry; 1]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathEntry {
    pub vid: VolumeId,
    pub lsns: RangeInclusive<LSN>,
}

impl SearchPath {
    pub const EMPTY: SearchPath = SearchPath { path: SmallVec::new_const() };

    pub fn new(vid: VolumeId, lsns: RangeInclusive<LSN>) -> Self {
        SearchPath {
            path: SmallVec::from_const([PathEntry { vid, lsns }]),
        }
    }

    pub fn append(&mut self, vid: VolumeId, lsns: RangeInclusive<LSN>) {
        assert!(!lsns.is_empty(), "LSN range must not be empty");
        self.path.push(PathEntry { vid, lsns });
    }

    pub fn first(&self) -> Option<(&VolumeId, LSN)> {
        self.path
            .first()
            .map(|entry| (&entry.vid, *entry.lsns.end()))
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &PathEntry> {
        self.path.iter()
    }
}

impl IntoIterator for SearchPath {
    type Item = PathEntry;
    type IntoIter = smallvec::IntoIter<[PathEntry; 1]>;
    fn into_iter(self) -> Self::IntoIter {
        self.path.into_iter()
    }
}

impl PathEntry {
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    pub fn lsns(&self) -> &RangeInclusive<LSN> {
        &self.lsns
    }
}
