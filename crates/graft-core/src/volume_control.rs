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
    pub vid: VolumeId,

    /// The parent reference if this Volume is a fork.
    #[bilrost(2)]
    pub parent: Option<VolumeRef>,

    /// The creation timestamp of this Volume.
    #[bilrost(3)]
    pub created_at: SystemTime,
}

impl VolumeControl {
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    pub fn parent(&self) -> Option<&VolumeRef> {
        self.parent.as_ref()
    }
}
