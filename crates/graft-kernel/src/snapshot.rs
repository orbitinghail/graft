use graft_core::{VolumeId, lsn::LSN};

use crate::search_path::SearchPath;

#[derive(Debug, Clone)]
pub struct Snapshot {
    vid: VolumeId,
    path: SearchPath,
}

impl Snapshot {
    pub fn new(vid: VolumeId, path: SearchPath) -> Self {
        Self { vid, path }
    }

    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    pub fn lsn(&self) -> Option<LSN> {
        self.path.first().map(|(_, lsn)| lsn)
    }

    pub fn search_path(&self) -> &SearchPath {
        &self.path
    }
}

impl PartialEq for Snapshot {
    fn eq(&self, other: &Self) -> bool {
        // 1. We check the LSN rather than the whole path, as checkpoints may
        // cause the path to change without changing the logical representation
        // of the snapshot.
        // 2. We check that the VolumeId is the same
        self.vid == other.vid && self.lsn() == other.lsn()
    }
}
impl Eq for Snapshot {}
