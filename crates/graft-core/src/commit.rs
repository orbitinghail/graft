use std::{ops::RangeInclusive, time::SystemTime};

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

    /// If this Commit contains any pages, `segment_idx` records details on the
    /// relevant Segment.
    #[bilrost(5)]
    segment_idx: Option<SegmentIdx>,

    /// If this commit is a checkpoint, this timestamp is set and records the time
    /// the commit was made a checkpoint
    #[bilrost(6)]
    checkpointed_at: Option<SystemTime>,
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
    frames: SmallVec<[SegmentFrameIdx; 2]>,
}

#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
struct SegmentFrameIdx {
    /// The length of the compressed frame in bytes.
    #[bilrost(1)]
    frame_size: usize,

    /// The last `PageIdx` contained by this `SegmentFrame`.
    #[bilrost(2)]
    last_pageidx: PageIdx,
}

/// A `SegmentFrameRef` contains the byte range and corresponding page range for a
/// particular frame in a segment.
pub struct SegmentFrameRef {
    sid: SegmentId,
    bytes: RangeInclusive<usize>,
    pages: RangeInclusive<PageIdx>,
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

impl SegmentIdx {
    pub fn sid(&self) -> &SegmentId {
        &self.sid
    }

    pub fn contains(&self, pageidx: PageIdx) -> bool {
        self.graft.contains(pageidx)
    }

    pub fn frame_for_pageidx(&self, pageidx: PageIdx) -> Option<SegmentFrameRef> {
        self.frames
            .iter()
            .scan((0, PageIdx::FIRST), |(bytes_acc, pages_acc), frame| {
                let bytes = *bytes_acc..=(*bytes_acc + frame.frame_size - 1);
                let pages = *pages_acc..=frame.last_pageidx;

                *bytes_acc += frame.frame_size;
                *pages_acc = frame.last_pageidx.saturating_next();

                Some((bytes, pages))
            })
            .find(|(_, pages)| pages.contains(&pageidx))
            .map(|(bytes, pages)| SegmentFrameRef { sid: self.sid.clone(), bytes, pages })
    }
}

impl SegmentFrameRef {
    pub fn sid(&self) -> &SegmentId {
        &self.sid
    }

    /// The size of the frame in bytes
    pub fn size(&self) -> usize {
        // add 1 because it's RangeInclusive
        self.bytes.end() - self.bytes.start() + 1
    }

    pub fn bytes(&self) -> &RangeInclusive<usize> {
        &self.bytes
    }

    pub fn pages(&self) -> &RangeInclusive<PageIdx> {
        &self.pages
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SegmentId, graft::Graft};

    #[test]
    fn test_frame_for_pageidx() {
        let mut frames = SmallVec::new();
        frames.push(SegmentFrameIdx {
            frame_size: 100,
            last_pageidx: PageIdx::new(10),
        });
        frames.push(SegmentFrameIdx {
            frame_size: 200,
            last_pageidx: PageIdx::new(25),
        });
        frames.push(SegmentFrameIdx {
            frame_size: 150,
            last_pageidx: PageIdx::new(40),
        });

        let segment_idx = SegmentIdx {
            sid: SegmentId::random(),
            graft: Graft::EMPTY,
            frames,
        };

        let test_cases = [
            (
                PageIdx::new(5),
                Some((0..=99, PageIdx::FIRST..=PageIdx::new(10))),
            ),
            (
                PageIdx::new(10),
                Some((0..=99, PageIdx::FIRST..=PageIdx::new(10))),
            ),
            (
                PageIdx::new(20),
                Some((100..=299, PageIdx::new(11)..=PageIdx::new(25))),
            ),
            (
                PageIdx::new(35),
                Some((300..=449, PageIdx::new(26)..=PageIdx::new(40))),
            ),
            (PageIdx::new(50), None),
        ];

        for (pageidx, expected) in test_cases {
            let result = segment_idx.frame_for_pageidx(pageidx);

            match expected {
                Some((expected_bytes, expected_pages)) => {
                    assert!(
                        result.is_some(),
                        "Expected frame for PageIdx {}",
                        pageidx.to_u32()
                    );
                    let frame_ref = result.unwrap();
                    assert_eq!(*frame_ref.bytes(), expected_bytes);
                    assert_eq!(*frame_ref.pages(), expected_pages);
                }
                None => {
                    assert!(
                        result.is_none(),
                        "Expected no frame for PageIdx {}",
                        pageidx.to_u32()
                    );
                }
            }
        }
    }

    #[test]
    fn test_frame_for_pageidx_empty_frames() {
        let segment_idx = SegmentIdx {
            sid: SegmentId::random(),
            graft: Graft::EMPTY,
            frames: SmallVec::new(),
        };

        let result = segment_idx.frame_for_pageidx(PageIdx::new(1));
        assert!(result.is_none());
    }
}
