use graft_core::{VolumeId, lsn::LSN};
use smallvec::SmallVec;

pub struct SearchPath {
    path: SmallVec<[PathEntry; 1]>,
}

struct PathEntry {
    vid: VolumeId,
    lsn_high: LSN,
    lsn_low: LSN,
}
