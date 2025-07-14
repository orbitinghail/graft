use bilrost::Message;
use bytes::Bytes;

use crate::{VolumeId, checkpoint_set::CheckpointSet, volume_ref::VolumeRef};

#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct VolumeMeta {
    /// The Volume's ID
    #[bilrost(1)]
    vid: VolumeId,

    /// The parent reference if this Volume is a fork.
    #[bilrost(2)]
    parent: Option<VolumeRef>,

    /// The etag from the last time we pulled the `CheckpointSet`, used to only
    /// pull changed `CheckpointSets`
    #[bilrost(3)]
    etag: Option<Bytes>,

    /// The set of checkpoint LSNs.
    #[bilrost(4)]
    checkpoints: CheckpointSet,
}

impl VolumeMeta {
    pub fn new(
        vid: VolumeId,
        parent: Option<VolumeRef>,
        etag: Option<Bytes>,
        checkpoints: CheckpointSet,
    ) -> Self {
        Self { vid, parent, etag, checkpoints }
    }

    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    pub fn parent(&self) -> Option<&VolumeRef> {
        self.parent.as_ref()
    }

    pub fn etag(&self) -> Option<&Bytes> {
        self.etag.as_ref()
    }

    pub fn checkpoints(&self) -> &CheckpointSet {
        &self.checkpoints
    }
}
