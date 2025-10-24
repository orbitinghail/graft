use std::ops::Deref;

use bilrost::Message;
use bytestring::ByteString;
use smallvec::SmallVec;

use crate::lsn::LSN;

/// A Volume's `CheckpointSet` is stored at `{prefix}/{vid}/checkpoints`.
/// `CheckpointSets` are updated by the checkpointer via compare-and-swap.
#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct Checkpoints {
    /// The set of checkpoint LSNs sorted in ascending order.
    #[bilrost(1)]
    lsns: SmallVec<[LSN; 2]>,
}

impl Checkpoints {
    pub const EMPTY: Checkpoints = Checkpoints { lsns: SmallVec::new_const() };

    /// Returns the largest LSN which is <= the provided lsn in the set
    pub fn checkpoint_for(&self, target: LSN) -> Option<LSN> {
        // self.lsns is sorted ascending, so search for the lsn in reverse
        self.lsns.iter().rev().copied().find(|&lsn| lsn <= target)
    }
}

impl Deref for Checkpoints {
    type Target = [LSN];

    fn deref(&self) -> &Self::Target {
        &self.lsns
    }
}

impl From<&[LSN]> for Checkpoints {
    fn from(lsns: &[LSN]) -> Self {
        Self { lsns: SmallVec::from_slice(lsns) }
    }
}

/// `CachedCheckpoints` stores Checkpoints alongside an optional cache etag to
/// manage consistency.
#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct CachedCheckpoints {
    #[bilrost(1)]
    checkpoints: Checkpoints,
    #[bilrost(2)]
    etag: Option<ByteString>,
}

impl CachedCheckpoints {
    pub const EMPTY: CachedCheckpoints = CachedCheckpoints {
        checkpoints: Checkpoints::EMPTY,
        etag: None,
    };

    pub fn new<T: Into<ByteString>>(checkpoints: Checkpoints, etag: Option<T>) -> Self {
        Self { checkpoints, etag: etag.map(Into::into) }
    }

    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    pub fn checkpoints(&self) -> &Checkpoints {
        &self.checkpoints
    }

    pub fn checkpoint_for(&self, lsn: LSN) -> Option<LSN> {
        self.checkpoints.checkpoint_for(lsn)
    }
}
