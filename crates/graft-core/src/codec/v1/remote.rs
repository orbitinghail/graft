use std::time::SystemTime;

use bilrost::Message;
use smallvec::SmallVec;

use crate::{
    PageCount, PageIdx, SegmentId, VolumeId, commit_hash::CommitHash, graft::Graft, lsn::LSN,
};

/// A reference to a Volume at a particular LSN.
#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct VolumeRef {
    /// The referenced Volume ID
    #[bilrost(1)]
    pub vid: VolumeId,

    /// The referenced LSN.
    #[bilrost(2)]
    pub lsn: LSN,
}

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

/// When a Volume is forked, a ref is first written to the parent Volume:
/// `{prefix}/{parent-vid}/forks/{fork-vid}`
/// Forks are immutable.
#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct VolumeFork {
    /// The VID of the fork.
    #[bilrost(1)]
    pub vid: VolumeId,

    /// The fork point. Must match the parent field in the Fork's Control file.
    #[bilrost(2)]
    pub parent: VolumeRef,
}

/// A Volume's `CheckpointSet` is stored at `{prefix}/{vid}/checkpoints`.
/// `CheckpointSets` are updated by the checkpointer via compare-and-swap.
#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct CheckpointSet {
    /// The ID of the Volume containing this `CheckpointSet`
    #[bilrost(1)]
    pub vid: VolumeId,

    /// The set of checkpoint LSNs.
    #[bilrost(2)]
    pub lsns: SmallVec<[LSN; 2]>,
}

/// A Volume Snapshot.
#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct Snapshot {
    /// The Volume's ID
    #[bilrost(1)]
    pub vid: VolumeId,

    /// The Snapshot LSN
    #[bilrost(2)]
    pub lsn: LSN,

    /// The Volume's `PageCount` at this LSN.
    #[bilrost(3)]
    pub page_count: PageCount,
}

/// Commits are stored at `{prefix}/{vid}/log/{lsn}`.
/// A commit may not include a `SegmentRef` if only the Volume's page count has
/// changed. This happens when the Volume is extended or truncated without
/// additional writes.
/// Commits are immutable.
#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct Commit {
    /// The Volume Snapshot at this Commit.
    #[bilrost(1)]
    pub snapshot: Snapshot,

    /// An optional `CommitHash` for this Commit.
    /// Always present on Remote Volume commits.
    /// May be omitted on Local commits.
    #[bilrost(2)]
    pub commit_hash: Option<CommitHash>,

    /// If this Commit contains any pages, `segment_ref` records details on the
    /// relevant Segment.
    #[bilrost(3)]
    pub segment_ref: Option<SegmentRef>,

    /// If this commit is a checkpoint, this timestamp is set and records the time
    /// the commit was made a checkpoint
    #[bilrost(4)]
    pub checkpointed_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct SegmentRef {
    /// The Segment ID
    #[bilrost(1)]
    pub sid: SegmentId,

    /// The Graft of `PageIdxs` contained by this Segment.
    #[bilrost(2)]
    pub graft: Graft,

    /// An index of `SegmentFrames` contained by this Segment.
    /// Empty on local Segments which have not been encoded and uploaded to object storage.
    #[bilrost(3)]
    pub frames: SmallVec<[SegmentFrame; 2]>,
}

#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct SegmentFrame {
    /// The number of Pages contained in this `SegmentFrame`.
    #[bilrost(1)]
    pub page_count: PageCount,

    /// The last `PageIdx` contained by this `SegmentFrame`.
    #[bilrost(2)]
    pub last_pageidx: PageIdx,
}
