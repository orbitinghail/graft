use bilrost::Message;
use smallvec::SmallVec;

use crate::{VolumeId, lsn::LSN};

/// A Volume's `CheckpointSet` is stored at `{prefix}/{vid}/checkpoints`.
/// `CheckpointSets` are updated by the checkpointer via compare-and-swap.
#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct CheckpointSet {
    /// The ID of the Volume containing this `CheckpointSet`
    #[bilrost(1)]
    vid: VolumeId,

    /// The set of checkpoint LSNs.
    #[bilrost(2)]
    lsns: SmallVec<[LSN; 2]>,
}

impl CheckpointSet {
    pub fn new(vid: VolumeId, lsns: &[LSN]) -> Self {
        Self { vid, lsns: lsns.into() }
    }

    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    pub fn lsns(&self) -> &[LSN] {
        &self.lsns
    }
}
