use std::time::SystemTime;

use bilrost::Message;

use crate::{VolumeId, volume_ref::VolumeRef};

/// A Volume has a top level control file stored at
/// `{prefix}/{vid}/control`
/// Control files are immutable.
#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct VolumeControl {
    /// The Volume's ID
    #[bilrost(1)]
    vid: VolumeId,

    /// The parent reference if this Volume is a fork.
    #[bilrost(2)]
    parent: Option<VolumeRef>,

    /// The creation timestamp of this Volume.
    #[bilrost(3)]
    created_at: SystemTime,
}

impl VolumeControl {
    pub fn new(vid: VolumeId, parent: Option<VolumeRef>, created_at: SystemTime) -> Self {
        Self { vid, parent, created_at }
    }

    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    pub fn parent(&self) -> Option<&VolumeRef> {
        self.parent.as_ref()
    }
}
