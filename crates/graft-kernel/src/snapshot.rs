use graft_core::volume_ref::VolumeRef;

use crate::search_path::SearchPath;

#[derive(Debug, Clone)]
pub struct Snapshot {
    vref: VolumeRef,
    path: SearchPath,
}

impl Snapshot {
    pub fn new(vref: VolumeRef, path: SearchPath) -> Self {
        Self { vref, path }
    }

    pub fn vref(&self) -> &VolumeRef {
        &self.vref
    }

    pub fn search_path(&self) -> &SearchPath {
        &self.path
    }
}
