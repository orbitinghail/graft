use graft_core::{VolumeId, lsn::LSN};
use smallvec::SmallVec;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SearchPath {
    path: SmallVec<[PathEntry; 1]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PathEntry {
    vid: VolumeId,
    lsn_high: LSN,
    lsn_low: LSN,
}

impl SearchPath {
    pub fn push(&mut self, vid: VolumeId, lsn_high: LSN, lsn_low: LSN) {
        assert!(
            lsn_high >= lsn_low,
            "lsn_high must be greater than or equal to lsn_low"
        );
        self.path.push(PathEntry { vid, lsn_high, lsn_low });
    }
}
