use std::time::SystemTime;

use bilrost::Message;
use smallvec::SmallVec;

use crate::{
    PageCount, PageIdx, SegmentId, VolumeId, commit_hash::CommitHash, graft::Graft, lsn::LSN,
    volume_ref::VolumeRef,
};

/// Commits are stored at `{prefix}/{vid}/log/{lsn}`.
/// A commit may not include a `SegmentRef` if only the Volume's page count has
/// changed. This happens when the Volume is extended or truncated without
/// additional writes.
/// Commits are immutable.
#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct Commit {
    /// The Volume's ID.
    #[bilrost(1)]
    vid: VolumeId,

    /// The LSN of the Commit.
    #[bilrost(2)]
    lsn: LSN,

    /// The Volume's `PageCount` as of this Commit.
    #[bilrost(3)]
    page_count: PageCount,

    /// An optional `CommitHash` for this Commit.
    /// Always present on Remote Volume commits.
    /// May be omitted on Local commits.
    #[bilrost(4)]
    commit_hash: Option<CommitHash>,

    /// If this Commit contains any pages, `segment_ref` records details on the
    /// relevant Segment.
    #[bilrost(5)]
    segment_ref: Option<SegmentRef>,

    /// If this commit is a checkpoint, this timestamp is set and records the time
    /// the commit was made a checkpoint
    #[bilrost(6)]
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

impl Commit {
    /// Creates a new Commit for the given snapshot info
    pub fn new(vid: VolumeId, lsn: LSN, page_count: PageCount) -> Self {
        Self {
            vid,
            lsn,
            page_count,
            commit_hash: None,
            segment_ref: None,
            checkpointed_at: None,
        }
    }

    pub fn with_commit_hash(self, commit_hash: Option<CommitHash>) -> Self {
        Self { commit_hash, ..self }
    }

    /// Sets the segment reference for this commit.
    pub fn with_segment_ref(self, segment_ref: Option<SegmentRef>) -> Self {
        Self { segment_ref, ..self }
    }

    /// Sets the checkpointed timestamp for this commit.
    pub fn with_checkpointed_at(self, checkpointed_at: Option<SystemTime>) -> Self {
        Self { checkpointed_at, ..self }
    }

    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    pub fn lsn(&self) -> LSN {
        self.lsn
    }

    pub fn vref(&self) -> VolumeRef {
        VolumeRef::new(self.vid.clone(), self.lsn)
    }

    pub fn page_count(&self) -> PageCount {
        self.page_count
    }

    pub fn commit_hash(&self) -> Option<&CommitHash> {
        self.commit_hash.as_ref()
    }

    pub fn segment_ref(&self) -> Option<&SegmentRef> {
        self.segment_ref.as_ref()
    }

    pub fn checkpointed_at(&self) -> Option<&SystemTime> {
        self.checkpointed_at.as_ref()
    }

    pub fn is_checkpoint(&self) -> bool {
        self.checkpointed_at.is_some()
    }
}
