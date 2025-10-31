use std::{
    ops::{Deref, DerefMut, Range},
    time::SystemTime,
};

use bilrost::Message;
use smallvec::SmallVec;
use splinter_rs::Splinter;

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

    /// If this Commit contains any pages, `segment_idx` records details on the
    /// relevant Segment.
    #[bilrost(5)]
    segment_idx: Option<SegmentIdx>,

    /// If this commit is a checkpoint, this timestamp is set and records the time
    /// the commit was made a checkpoint
    #[bilrost(6)]
    checkpointed_at: Option<SystemTime>,
}

impl Commit {
    /// Creates a new Commit for the given snapshot info
    pub fn new(vid: VolumeId, lsn: LSN, page_count: PageCount) -> Self {
        Self {
            vid,
            lsn,
            page_count,
            commit_hash: None,
            segment_idx: None,
            checkpointed_at: None,
        }
    }

    pub fn with_vid(self, vid: VolumeId) -> Self {
        Self { vid, ..self }
    }

    pub fn with_lsn(self, lsn: LSN) -> Self {
        Self { lsn, ..self }
    }

    pub fn with_commit_hash(self, commit_hash: Option<CommitHash>) -> Self {
        Self { commit_hash, ..self }
    }

    /// Sets the segment index for this commit.
    pub fn with_segment_idx(self, segment_idx: Option<SegmentIdx>) -> Self {
        Self { segment_idx, ..self }
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

    pub fn segment_idx(&self) -> Option<&SegmentIdx> {
        self.segment_idx.as_ref()
    }

    pub fn checkpointed_at(&self) -> Option<&SystemTime> {
        self.checkpointed_at.as_ref()
    }

    pub fn is_checkpoint(&self) -> bool {
        self.checkpointed_at.is_some()
    }
}

#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct SegmentIdx {
    /// The Segment ID
    #[bilrost(1)]
    sid: SegmentId,

    /// The Graft of `PageIdxs` contained by this Segment.
    #[bilrost(2)]
    graft: Graft,

    /// An index of `SegmentFrameIdxs` contained by this Segment.
    /// Empty on local Segments which have not been encoded and uploaded to object storage.
    #[bilrost(3)]
    frames: SmallVec<[SegmentFrameIdx; 1]>,
}

impl SegmentIdx {
    pub fn new(sid: SegmentId, graft: Graft) -> Self {
        SegmentIdx { sid, graft, frames: SmallVec::new() }
    }

    pub fn with_frames(self, frames: SmallVec<[SegmentFrameIdx; 1]>) -> Self {
        Self { frames, ..self }
    }

    pub fn sid(&self) -> &SegmentId {
        &self.sid
    }

    pub fn graft(&self) -> &Graft {
        &self.graft
    }

    pub fn frame_for_pageidx(&self, pageidx: PageIdx) -> Option<SegmentFrameRef> {
        self.frames
            .iter()
            .scan((0, PageIdx::FIRST), |(bytes_acc, pages_acc), frame| {
                let bytes = *bytes_acc..(*bytes_acc + frame.frame_size);
                let pages = *pages_acc..=frame.last_pageidx;

                *bytes_acc += frame.frame_size;
                *pages_acc = frame.last_pageidx.saturating_next();

                Some((bytes, pages))
            })
            .find(|(_, pages)| pages.contains(&pageidx))
            .map(|(bytes, pages)| {
                let pages = pages.start().to_u32()..=pages.end().to_u32();
                let graft = (Splinter::from(pages) & self.graft.splinter()).into();
                SegmentFrameRef { sid: self.sid.clone(), bytes, graft }
            })
    }
}

impl Deref for SegmentIdx {
    type Target = Graft;

    fn deref(&self) -> &Self::Target {
        &self.graft
    }
}

impl DerefMut for SegmentIdx {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.graft
    }
}

#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct SegmentFrameIdx {
    /// The length of the compressed frame in bytes.
    #[bilrost(1)]
    frame_size: usize,

    /// The last `PageIdx` contained by this `SegmentFrame`.
    #[bilrost(2)]
    last_pageidx: PageIdx,
}

impl SegmentFrameIdx {
    pub fn new(frame_size: usize, last_pageidx: PageIdx) -> Self {
        Self { frame_size, last_pageidx }
    }

    pub fn frame_size(&self) -> usize {
        self.frame_size
    }

    pub fn last_pageidx(&self) -> PageIdx {
        self.last_pageidx
    }
}

/// A `SegmentFrameRef` contains the byte range and corresponding pages for a
/// particular frame in a segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentFrameRef {
    pub sid: SegmentId,
    pub bytes: Range<usize>,
    pub graft: Graft,
}

impl SegmentFrameRef {
    /// The size of the frame in bytes
    pub fn size(&self) -> usize {
        self.bytes.end - self.bytes.start
    }
}

#[cfg(test)]
mod tests {
    use crate::pageidx;

    use super::*;

    #[test]
    fn test_frame_for_pageidx() {
        let graft = Graft::from_range(pageidx!(5)..=pageidx!(25));
        let mut frames = SmallVec::new();
        frames.push(SegmentFrameIdx {
            frame_size: 100,
            last_pageidx: pageidx!(10),
        });
        frames.push(SegmentFrameIdx {
            frame_size: 200,
            last_pageidx: pageidx!(20),
        });
        frames.push(SegmentFrameIdx {
            frame_size: 150,
            last_pageidx: pageidx!(25),
        });

        let segment_idx = SegmentIdx { sid: SegmentId::EMPTY, graft, frames };

        let tests = [
            (pageidx!(4), None),
            (
                pageidx!(5),
                Some(SegmentFrameRef {
                    sid: SegmentId::EMPTY,
                    bytes: 0..100,
                    graft: Graft::from_range(pageidx!(5)..=pageidx!(10)),
                }),
            ),
            (
                pageidx!(10),
                Some(SegmentFrameRef {
                    sid: SegmentId::EMPTY,
                    bytes: 0..100,
                    graft: Graft::from_range(pageidx!(5)..=pageidx!(10)),
                }),
            ),
            (
                pageidx!(11),
                Some(SegmentFrameRef {
                    sid: SegmentId::EMPTY,
                    bytes: 100..300,
                    graft: Graft::from_range(pageidx!(11)..=pageidx!(20)),
                }),
            ),
            (
                pageidx!(20),
                Some(SegmentFrameRef {
                    sid: SegmentId::EMPTY,
                    bytes: 100..300,
                    graft: Graft::from_range(pageidx!(11)..=pageidx!(20)),
                }),
            ),
            (
                pageidx!(25),
                Some(SegmentFrameRef {
                    sid: SegmentId::EMPTY,
                    bytes: 300..450,
                    graft: Graft::from_range(pageidx!(21)..=pageidx!(25)),
                }),
            ),
            (pageidx!(26), None),
        ];

        for (pageidx, expected) in tests {
            assert_eq!(
                segment_idx.frame_for_pageidx(pageidx),
                expected,
                "wrong frame for pageidx {pageidx}"
            );
        }
    }

    #[test]
    fn test_frame_for_pageidx_empty_frames() {
        let segment_idx = SegmentIdx {
            sid: SegmentId::random(),
            graft: Graft::EMPTY,
            frames: SmallVec::new(),
        };

        let result = segment_idx.frame_for_pageidx(pageidx!(1));
        assert!(result.is_none());
    }
}
