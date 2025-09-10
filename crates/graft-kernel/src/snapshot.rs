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
