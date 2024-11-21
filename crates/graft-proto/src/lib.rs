include!("mod.rs");

use std::{
    error::Error,
    fmt::Display,
    ops::{Bound, Range, RangeBounds},
    time::SystemTime,
};

use bytes::Bytes;
use common::v1::{lsn_bound, Commit, GraftErr, LsnBound, LsnRange, SegmentInfo, Snapshot};
use graft_core::{gid::GidParseErr, lsn::LSN, SegmentId, VolumeId};
use prost_types::TimestampError;

pub use graft::*;
use splinter::{DecodeErr, SplinterRef};

impl Error for GraftErr {}
impl Display for GraftErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code(), self.message)
    }
}

impl Commit {
    pub fn snapshot(&self) -> &Snapshot {
        self.snapshot.as_ref().expect("snapshot is required")
    }
}

impl SegmentInfo {
    pub fn new(sid: &SegmentId, offsets: Bytes) -> Self {
        Self { sid: sid.copy_to_bytes(), offsets }
    }

    pub fn sid(&self) -> Result<&SegmentId, GidParseErr> {
        self.sid.as_ref().try_into()
    }

    pub fn offsets(&self) -> Result<SplinterRef<Bytes>, DecodeErr> {
        SplinterRef::from_bytes(self.offsets.clone())
    }
}

impl Snapshot {
    pub fn new(
        vid: &VolumeId,
        lsn: LSN,
        checkpoint_lsn: LSN,
        last_offset: u32,
        timestamp: SystemTime,
    ) -> Self {
        Self {
            vid: vid.copy_to_bytes(),
            lsn,
            checkpoint_lsn,
            last_offset,
            timestamp: Some(timestamp.into()),
        }
    }

    pub fn vid(&self) -> Result<&VolumeId, GidParseErr> {
        self.vid.as_ref().try_into()
    }

    pub fn lsn(&self) -> LSN {
        self.lsn
    }

    pub fn checkpoint(&self) -> LSN {
        self.checkpoint_lsn
    }

    pub fn last_offset(&self) -> u32 {
        self.last_offset
    }

    pub fn system_time(&self) -> Result<Option<SystemTime>, TimestampError> {
        self.timestamp.map(|ts| ts.try_into()).transpose()
    }
}

impl LsnBound {
    fn as_bound(&self) -> Bound<&LSN> {
        match &self.bound {
            Some(lsn_bound::Bound::Included(lsn)) => Bound::Included(lsn),
            Some(lsn_bound::Bound::Excluded(lsn)) => Bound::Excluded(lsn),
            None => Bound::Unbounded,
        }
    }
}

impl From<Bound<&LSN>> for LsnBound {
    fn from(bound: Bound<&LSN>) -> Self {
        let bound = match bound {
            Bound::Included(lsn) => Some(lsn_bound::Bound::Included(*lsn)),
            Bound::Excluded(lsn) => Some(lsn_bound::Bound::Excluded(*lsn)),
            Bound::Unbounded => None,
        };
        Self { bound }
    }
}

impl RangeBounds<LSN> for LsnRange {
    fn start_bound(&self) -> Bound<&LSN> {
        self.start
            .as_ref()
            .map(|b| b.as_bound())
            .unwrap_or(Bound::Unbounded)
    }

    fn end_bound(&self) -> Bound<&LSN> {
        self.end
            .as_ref()
            .map(|b| b.as_bound())
            .unwrap_or(Bound::Unbounded)
    }
}

impl LsnRange {
    pub fn from_bounds<R>(bounds: &R) -> Self
    where
        R: RangeBounds<LSN>,
    {
        Self {
            start: Some(bounds.start_bound().into()),
            end: Some(bounds.end_bound().into()),
        }
    }

    pub fn try_len(&self) -> Option<usize> {
        let start = self.start()?;
        let end = self.end()?;
        end.checked_sub(start).map(|len| len as usize)
    }

    pub fn start(&self) -> Option<LSN> {
        self.start.and_then(|b| match b.bound {
            Some(lsn_bound::Bound::Included(lsn)) => Some(lsn),
            Some(lsn_bound::Bound::Excluded(lsn)) => Some(lsn + 1),
            None => None,
        })
    }

    pub fn start_exclusive(&self) -> Option<LSN> {
        self.start.and_then(|b| match b.bound {
            Some(lsn_bound::Bound::Included(lsn)) => lsn.checked_sub(1),
            Some(lsn_bound::Bound::Excluded(lsn)) => Some(lsn),
            None => None,
        })
    }

    pub fn end(&self) -> Option<LSN> {
        self.end.and_then(|b| match b.bound {
            Some(lsn_bound::Bound::Included(lsn)) => Some(lsn),
            Some(lsn_bound::Bound::Excluded(lsn)) => Some(lsn.saturating_sub(1)),
            None => None,
        })
    }

    pub fn end_exclusive(&self) -> Option<LSN> {
        self.end.and_then(|b| match b.bound {
            Some(lsn_bound::Bound::Included(lsn)) => lsn.checked_add(1),
            Some(lsn_bound::Bound::Excluded(lsn)) => Some(lsn),
            None => None,
        })
    }

    pub fn canonical(&self) -> Range<LSN> {
        (*self).into()
    }
}

impl From<LsnRange> for Range<LSN> {
    fn from(range: LsnRange) -> Self {
        let start = range.start().unwrap_or(0);
        let end = range.end_exclusive().unwrap_or(LSN::MAX);
        start..end
    }
}

impl Display for LsnRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.start(), self.end_exclusive()) {
            (Some(start), Some(end)) => write!(f, "[{}..{})", start, end),
            (Some(start), None) => write!(f, "[{}..)", start),
            (None, Some(end)) => write!(f, "(..{})", end),
            (None, None) => write!(f, "(..)"),
        }
    }
}
