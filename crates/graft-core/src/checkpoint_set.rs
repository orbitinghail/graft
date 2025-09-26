use std::ops::Deref;

use bilrost::Message;
use smallvec::SmallVec;

use crate::lsn::LSN;

/// A Volume's `CheckpointSet` is stored at `{prefix}/{vid}/checkpoints`.
/// `CheckpointSets` are updated by the checkpointer via compare-and-swap.
#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct CheckpointSet {
    /// The set of checkpoint LSNs sorted in ascending order.
    #[bilrost(1)]
    lsns: SmallVec<[LSN; 2]>,
}

impl CheckpointSet {
    pub const EMPTY: CheckpointSet = CheckpointSet { lsns: SmallVec::new_const() };

    /// Returns the largest LSN which is <= the provided lsn in the set
    pub fn checkpoint_for(&self, target: LSN) -> Option<LSN> {
        // self.lsns is sorted ascending, so search for the lsn in reverse
        self.lsns.iter().rev().copied().find(|&lsn| lsn <= target)
    }
}

impl Deref for CheckpointSet {
    type Target = [LSN];

    fn deref(&self) -> &Self::Target {
        &self.lsns
    }
}

impl From<&[LSN]> for CheckpointSet {
    fn from(lsns: &[LSN]) -> Self {
        Self { lsns: SmallVec::from_slice(lsns) }
    }
}
