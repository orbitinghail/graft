use bilrost::Message;

use crate::{VolumeId, checkpoint_set::CheckpointSet, etag::ETag, lsn::LSN, volume_ref::VolumeRef};

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
    checkpoints: Option<(ETag, CheckpointSet)>,
}

impl VolumeMeta {
    pub fn new(
        vid: VolumeId,
        parent: Option<VolumeRef>,
        checkpoints: Option<(ETag, CheckpointSet)>,
    ) -> Self {
        Self { vid, parent, checkpoints }
    }

    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    pub fn parent(&self) -> Option<&VolumeRef> {
        self.parent.as_ref()
    }

    pub fn checkpoints(&self) -> Option<&(ETag, CheckpointSet)> {
        self.checkpoints.as_ref()
    }

    pub fn checkpoint_for(&self, lsn: LSN) -> Option<LSN> {
        self.checkpoints().and_then(|(_, c)| c.checkpoint_for(lsn))
    }

    #[must_use]
    pub fn with_checkpoints(self, checkpoints: Option<(ETag, CheckpointSet)>) -> Self {
        Self { checkpoints, ..self }
    }
}
