use std::time::SystemTime;

use bilrost::Message;
use smallvec::SmallVec;

use crate::{
    PageCount, PageIdx, SegmentId, commit_hash::CommitHash, graft::Graft, snapshot::Snapshot,
};

/// Commits are stored at `{prefix}/{vid}/log/{lsn}`.
/// A commit may not include a `SegmentRef` if only the Volume's page count has
/// changed. This happens when the Volume is extended or truncated without
/// additional writes.
/// Commits are immutable.
#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct Commit {
    /// The Volume Snapshot at this Commit.
    #[bilrost(1)]
    snapshot: Snapshot,

    /// An optional `CommitHash` for this Commit.
    /// Always present on Remote Volume commits.
    /// May be omitted on Local commits.
    #[bilrost(2)]
    commit_hash: Option<CommitHash>,

    /// If this Commit contains any pages, `segment_ref` records details on the
    /// relevant Segment.
    #[bilrost(3)]
    segment_ref: Option<SegmentRef>,

    /// If this commit is a checkpoint, this timestamp is set and records the time
    /// the commit was made a checkpoint
    #[bilrost(4)]
    checkpointed_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct SegmentRef {
    /// The Segment ID
    #[bilrost(1)]
    sid: SegmentId,

    /// The Graft of `PageIdxs` contained by this Segment.
    #[bilrost(2)]
    graft: Graft,

    /// An index of `SegmentFrames` contained by this Segment.
    /// Empty on local Segments which have not been encoded and uploaded to object storage.
    #[bilrost(3)]
    frames: SmallVec<[SegmentFrame; 2]>,
}

#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct SegmentFrame {
    /// The number of Pages contained in this `SegmentFrame`.
    #[bilrost(1)]
    page_count: PageCount,

    /// The last `PageIdx` contained by this `SegmentFrame`.
    #[bilrost(2)]
    last_pageidx: PageIdx,
}
