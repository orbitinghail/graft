use bilrost::Message;

use crate::{VolumeId, volume_ref::VolumeRef};

/// When a Volume is forked, a ref is first written to the parent Volume:
/// `{prefix}/{parent-vid}/forks/{fork-vid}`
/// Forks are immutable.
#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct VolumeFork {
    /// The VID of the fork.
    #[bilrost(1)]
    vid: VolumeId,

    /// The fork point. Must match the parent field in the Fork's Control file.
    #[bilrost(2)]
    parent: VolumeRef,
}

impl VolumeFork {
    pub fn new(vid: VolumeId, parent: VolumeRef) -> Self {
        Self { vid, parent }
    }

    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    pub fn parent(&self) -> &VolumeRef {
        &self.parent
    }
}
