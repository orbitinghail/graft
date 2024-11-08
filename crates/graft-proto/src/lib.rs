include!("mod.rs");

use std::{
    ops::{Bound, Range, RangeBounds},
    time::SystemTime,
};

use common::v1::{lsn_bound, LsnBound, LsnRange, Snapshot};
use graft_core::{gid::GidParseErr, lsn::LSN, VolumeId};
use prost_types::TimestampError;

pub use graft::*;

impl Snapshot {
    pub fn new(vid: &VolumeId, lsn: LSN, last_offset: u32, timestamp: SystemTime) -> Self {
        Self {
            vid: vid.into(),
            lsn,
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
