use bilrost::Message;

use crate::{
    VolumeId,
    checkpoints::{CachedCheckpoints, Checkpoints},
    lsn::LSN,
    volume_ref::VolumeRef,
};

#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct VolumeMeta {
    /// The Volume's ID
    #[bilrost(1)]
    vid: VolumeId,

    /// The parent reference if this Volume is a fork.
    #[bilrost(2)]
    parent: Option<VolumeRef>,

    /// The set of checkpoint LSNs.
    #[bilrost(3)]
    checkpoints: CachedCheckpoints,
}

impl VolumeMeta {
    pub fn new(vid: VolumeId, parent: Option<VolumeRef>, checkpoints: CachedCheckpoints) -> Self {
        Self { vid, parent, checkpoints }
    }

    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    pub fn parent(&self) -> Option<&VolumeRef> {
        self.parent.as_ref()
    }

    pub fn checkpoints_etag(&self) -> Option<&str> {
        self.checkpoints.etag()
    }

    pub fn cached_checkpoints(&self) -> &CachedCheckpoints {
        &self.checkpoints
    }

    pub fn checkpoints(&self) -> &Checkpoints {
        self.checkpoints.checkpoints()
    }

    pub fn checkpoint_for(&self, lsn: LSN) -> Option<LSN> {
        self.checkpoints.checkpoint_for(lsn)
    }

    #[must_use]
    pub fn with_checkpoints(self, checkpoints: CachedCheckpoints) -> Self {
        Self { checkpoints, ..self }
    }
}
