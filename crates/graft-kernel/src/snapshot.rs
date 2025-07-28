use graft_core::{VolumeId, lsn::LSN};

use crate::search_path::SearchPath;

#[derive(Debug, Clone)]
pub struct Snapshot {
    vid: VolumeId,
    lsn: Option<LSN>,
    path: SearchPath,
}

impl Snapshot {
    pub fn new(vid: VolumeId, lsn: Option<LSN>, path: SearchPath) -> Self {
        Self { vid, lsn, path }
    }

    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    pub fn lsn(&self) -> Option<LSN> {
        self.lsn.clone()
    }

    pub fn search_path(&self) -> &SearchPath {
        &self.path
    }
}
