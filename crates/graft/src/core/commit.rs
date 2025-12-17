use std::ops::{Deref, DerefMut, Range, RangeInclusive};

use bilrost::Message;
use splinter_rs::Splinter;
use thin_vec::ThinVec;

use crate::core::{
    LogId, PageCount, PageIdx, SegmentId, commit_hash::CommitHash, logref::LogRef, lsn::LSN,
    pageset::PageSet,
};

/// A Commit tracks which pages have changed in a volume at a particular point in time (LSN).
/// A commit's `SegmentIdx` may be omitted if only the Volume's `PageCount` has changed.
#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
#[bilrost(reserved_tags(6))]
pub struct Commit {
    /// The ID of the Log which this Commit is part of.
    #[bilrost(1)]
    pub log: LogId,

    /// The LSN of the Commit.
    #[bilrost(2)]
    pub lsn: LSN,

    /// The Volume's `PageCount` as of this Commit.
    #[bilrost(3)]
    pub page_count: PageCount,

    /// An optional `CommitHash` for this Commit.
    /// Always present on Commits uploaded to a remote Log.
    /// May be omitted on commits to a local Log.
    #[bilrost(4)]
    pub commit_hash: Option<CommitHash>,

    /// If this Commit contains any pages, `segment_idx` forms an index of those
    /// pages within associated Segments.
    #[bilrost(5)]
    pub segment_idx: Option<SegmentIdx>,

    /// Some commits are used to track the addition of new checkpoints. LSNs
    /// referred to by this field have been upgraded into Checkpoint Commits and
    /// should be re-fetched by clients.
    ///
    /// If this commit is a checkpoint, it will store its own LSN in this field.
    #[bilrost(7)]
    pub checkpoints: ThinVec<LSN>,
}

impl Commit {
    /// Creates a new Commit for the given snapshot info
    pub fn new(log: LogId, lsn: LSN, page_count: PageCount) -> Self {
        Self {
            log,
            lsn,
            page_count,
            commit_hash: None,
            segment_idx: None,
            checkpoints: Default::default(),
        }
    }

    pub fn with_log_id(self, log: LogId) -> Self {
        Self { log, ..self }
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

    /// Tracks the provided checkpoint LSNs in this commit.
    ///
    /// To mark this commit as a checkpoint, include this
    /// commit's LSN in the list.
    pub fn with_checkpoints(self, checkpoints: ThinVec<LSN>) -> Self {
        Self { checkpoints, ..self }
    }

    pub fn log(&self) -> &LogId {
        &self.log
    }

    pub fn lsn(&self) -> LSN {
        self.lsn
    }

    pub fn logref(&self) -> LogRef {
        LogRef::new(self.log.clone(), self.lsn)
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

    pub fn segment_id(&self) -> Option<&SegmentId> {
        self.segment_idx().map(|idx| &idx.sid)
    }

    pub fn checkpoints(&self) -> &ThinVec<LSN> {
        &self.checkpoints
    }

    pub fn is_checkpoint(&self) -> bool {
        self.checkpoints.contains(&self.lsn)
    }
}

#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct SegmentIdx {
    /// The Segment ID
    #[bilrost(1)]
    pub sid: SegmentId,

    /// The set of `PageIdxs` contained by this Segment.
    #[bilrost(2)]
    pub pageset: PageSet,

    /// An index of `SegmentFrameIdxs` contained by this Segment.
    /// Empty on local Segments which have not been encoded and uploaded to object storage.
    #[bilrost(3)]
    pub frames: ThinVec<SegmentFrameIdx>,
}

impl SegmentIdx {
    pub fn new(sid: SegmentId, pageset: PageSet) -> Self {
        SegmentIdx { sid, pageset, frames: Default::default() }
    }

    pub fn with_frames(self, frames: ThinVec<SegmentFrameIdx>) -> Self {
        Self { frames, ..self }
    }

    pub fn sid(&self) -> &SegmentId {
        &self.sid
    }

    pub fn pageset(&self) -> &PageSet {
        &self.pageset
    }

    pub fn page_count(&self) -> PageCount {
        self.pageset.cardinality()
    }

    pub fn iter_frames(
        &self,
        mut filter: impl FnMut(&RangeInclusive<PageIdx>) -> bool,
    ) -> impl Iterator<Item = SegmentRangeRef> {
        let first_page = self.pageset.iter().next().unwrap_or(PageIdx::FIRST);
        self.frames
            .iter()
            .scan((0, first_page), |(bytes_acc, pages_acc), frame| {
                let bytes = *bytes_acc..(*bytes_acc + frame.frame_size);
                let pages = *pages_acc..=frame.last_pageidx;

                *bytes_acc += frame.frame_size;
                *pages_acc = frame.last_pageidx.saturating_next();

                Some((bytes, pages))
            })
            .filter(move |(_, pages)| filter(pages))
            .map(|(bytes, pages)| {
                let pages = pages.start().to_u32()..=pages.end().to_u32();
                let graft = (Splinter::from(pages) & self.pageset.splinter()).into();
                SegmentRangeRef {
                    sid: self.sid.clone(),
                    bytes,
                    pageset: graft,
                }
            })
    }

    pub fn frame_for_pageidx(&self, pageidx: PageIdx) -> Option<SegmentRangeRef> {
        if !self.pageset.contains(pageidx) {
            return None;
        }
        self.iter_frames(|pages| pages.contains(&pageidx)).next()
    }
}

impl Deref for SegmentIdx {
    type Target = PageSet;

    fn deref(&self) -> &Self::Target {
        &self.pageset
    }
}

impl DerefMut for SegmentIdx {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.pageset
    }
}

#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct SegmentFrameIdx {
    /// The length of the compressed frame in bytes.
    #[bilrost(1)]
    frame_size: u64,

    /// The last `PageIdx` contained by this `SegmentFrame`.
    #[bilrost(2)]
    last_pageidx: PageIdx,
}

impl SegmentFrameIdx {
    pub fn new(frame_size: u64, last_pageidx: PageIdx) -> Self {
        Self { frame_size, last_pageidx }
    }

    pub fn frame_size(&self) -> u64 {
        self.frame_size
    }

    pub fn last_pageidx(&self) -> PageIdx {
        self.last_pageidx
    }
}

/// A `SegmentRangeRef` contains the byte range and corresponding pages for a
/// subset of a segment. The subset must correspond to one or more entire
/// `SegmentFrames`.
#[derive(Clone, PartialEq, Eq)]
pub struct SegmentRangeRef {
    pub sid: SegmentId,
    pub bytes: Range<u64>,
    pub pageset: PageSet,
}

impl std::fmt::Debug for SegmentRangeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentRangeRef")
            .field("sid", &self.sid)
            .field("bytes", &self.bytes)
            .field("pages", &self.pageset.cardinality())
            .finish()
    }
}

impl SegmentRangeRef {
    /// The size of the frame in bytes
    pub fn size(&self) -> u64 {
        self.bytes.end - self.bytes.start
    }

    /// Attempt to coalesce two frame refs together.
    /// Returns the two frame refs unmodified if coalescing is impossible.
    #[allow(clippy::result_large_err)]
    pub fn coalesce(self, other: Self) -> Result<Self, (Self, Self)> {
        if self.sid != other.sid {
            return Err((self, other));
        }

        let (left, right) = if self.bytes.end == other.bytes.start {
            (self, other)
        } else if other.bytes.end == self.bytes.start {
            (other, self)
        } else {
            return Err((self, other));
        };

        let left_splinter: Splinter = left.pageset.into();
        let right_splinter: Splinter = right.pageset.into();
        Ok(Self {
            sid: left.sid,
            bytes: left.bytes.start..right.bytes.end,
            pageset: (left_splinter | right_splinter).into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::pageidx;
    use test_log::test;

    use super::*;

    #[test]
    fn test_frame_for_pageidx() {
        let pageset = PageSet::from_range(pageidx!(5)..=pageidx!(25));
        let mut frames = ThinVec::new();
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

        let sid = SegmentId::random();
        let segment_idx = SegmentIdx { sid: sid.clone(), pageset, frames };

        let tests = [
            (pageidx!(4), None),
            (
                pageidx!(5),
                Some(SegmentRangeRef {
                    sid: sid.clone(),
                    bytes: 0..100,
                    pageset: PageSet::from_range(pageidx!(5)..=pageidx!(10)),
                }),
            ),
            (
                pageidx!(10),
                Some(SegmentRangeRef {
                    sid: sid.clone(),
                    bytes: 0..100,
                    pageset: PageSet::from_range(pageidx!(5)..=pageidx!(10)),
                }),
            ),
            (
                pageidx!(11),
                Some(SegmentRangeRef {
                    sid: sid.clone(),
                    bytes: 100..300,
                    pageset: PageSet::from_range(pageidx!(11)..=pageidx!(20)),
                }),
            ),
            (
                pageidx!(20),
                Some(SegmentRangeRef {
                    sid: sid.clone(),
                    bytes: 100..300,
                    pageset: PageSet::from_range(pageidx!(11)..=pageidx!(20)),
                }),
            ),
            (
                pageidx!(25),
                Some(SegmentRangeRef {
                    sid: sid.clone(),
                    bytes: 300..450,
                    pageset: PageSet::from_range(pageidx!(21)..=pageidx!(25)),
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
            pageset: PageSet::EMPTY,
            frames: ThinVec::new(),
        };

        let result = segment_idx.frame_for_pageidx(pageidx!(1));
        assert!(result.is_none());
    }

    #[test]
    fn test_segment_range_ref_coalesce_adjacent() {
        let sid = SegmentId::random();
        // Test coalescing two adjacent ranges (first before second)
        let frame1 = SegmentRangeRef {
            sid: sid.clone(),
            bytes: 0..100,
            pageset: PageSet::from_range(pageidx!(5)..=pageidx!(10)),
        };
        let frame2 = SegmentRangeRef {
            sid: sid.clone(),
            bytes: 100..200,
            pageset: PageSet::from_range(pageidx!(11)..=pageidx!(20)),
        };

        let result = frame1.clone().coalesce(frame2.clone()).unwrap();
        assert_eq!(result.bytes, 0..200);
        assert_eq!(
            result.pageset,
            PageSet::from_range(pageidx!(5)..=pageidx!(20))
        );

        // Test coalescing in reverse order (second before first)
        let result = frame2.coalesce(frame1).unwrap();
        assert_eq!(result.bytes, 0..200);
        assert_eq!(
            result.pageset,
            PageSet::from_range(pageidx!(5)..=pageidx!(20))
        );
    }

    #[test]
    fn test_segment_range_ref_coalesce_non_adjacent() {
        let sid = SegmentId::random();
        // Test that non-adjacent ranges cannot be coalesced
        let frame1 = SegmentRangeRef {
            sid: sid.clone(),
            bytes: 0..100,
            pageset: PageSet::from_range(pageidx!(5)..=pageidx!(10)),
        };
        let frame2 = SegmentRangeRef {
            sid: sid.clone(),
            bytes: 150..250,
            pageset: PageSet::from_range(pageidx!(20)..=pageidx!(30)),
        };

        let result = frame1.clone().coalesce(frame2.clone());
        assert!(result.is_err());
        let (f1, f2) = result.unwrap_err();
        assert_eq!(f1, frame1);
        assert_eq!(f2, frame2);
    }

    #[test]
    fn test_segment_range_ref_coalesce_diff_segment() {
        // Test that adjacent ranges from different segments don't combine
        let frame1 = SegmentRangeRef {
            sid: SegmentId::random(),
            bytes: 0..100,
            pageset: PageSet::from_range(pageidx!(5)..=pageidx!(10)),
        };
        let frame2 = SegmentRangeRef {
            sid: SegmentId::random(),
            bytes: 100..200,
            pageset: PageSet::from_range(pageidx!(11)..=pageidx!(20)),
        };

        let result = frame1.clone().coalesce(frame2.clone());
        assert!(result.is_err());
        let (f1, f2) = result.unwrap_err();
        assert_eq!(f1, frame1);
        assert_eq!(f2, frame2);
    }

    #[test]
    fn test_iter_frames_no_filter() {
        let pageset = PageSet::from_range(pageidx!(5)..=pageidx!(25));
        let mut frames = ThinVec::new();
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

        let segment_idx = SegmentIdx {
            sid: SegmentId::random(),
            pageset,
            frames,
        };

        // Collect all frames
        let all_frames: Vec<_> = segment_idx.iter_frames(|_| true).collect();
        assert_eq!(all_frames.len(), 3);

        assert_eq!(all_frames[0].bytes, 0..100);
        assert_eq!(
            all_frames[0].pageset,
            PageSet::from_range(pageidx!(5)..=pageidx!(10))
        );

        assert_eq!(all_frames[1].bytes, 100..300);
        assert_eq!(
            all_frames[1].pageset,
            PageSet::from_range(pageidx!(11)..=pageidx!(20))
        );

        assert_eq!(all_frames[2].bytes, 300..450);
        assert_eq!(
            all_frames[2].pageset,
            PageSet::from_range(pageidx!(21)..=pageidx!(25))
        );
    }

    #[test]
    fn test_iter_frames_with_filter() {
        let pageset = PageSet::from_range(pageidx!(5)..=pageidx!(25));
        let mut frames = ThinVec::new();
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

        let segment_idx = SegmentIdx {
            sid: SegmentId::random(),
            pageset,
            frames,
        };

        // Filter for frames containing page 15
        let filtered_frames: Vec<_> = segment_idx
            .iter_frames(|pages| pages.contains(&pageidx!(15)))
            .collect();
        assert_eq!(filtered_frames.len(), 1);
        assert_eq!(filtered_frames[0].bytes, 100..300);
        assert_eq!(
            filtered_frames[0].pageset,
            PageSet::from_range(pageidx!(11)..=pageidx!(20))
        );
    }

    #[test]
    fn test_iter_frames_empty() {
        let segment_idx = SegmentIdx {
            sid: SegmentId::random(),
            pageset: PageSet::EMPTY,
            frames: ThinVec::new(),
        };

        let frames: Vec<_> = segment_idx.iter_frames(|_| true).collect();
        assert_eq!(frames.len(), 0);
    }
}
