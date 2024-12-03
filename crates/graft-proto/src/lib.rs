// pull in the generated types
include!("mod.rs");

use std::{
    error::Error,
    fmt::Display,
    ops::{RangeBounds, RangeInclusive},
    time::SystemTime,
};

use bytes::Bytes;
use common::v1::{Commit, GraftErr, LsnRange, SegmentInfo, Snapshot};
use graft_core::{
    gid::GidParseErr,
    lsn::{LSNRangeExt, LSN},
    page_count::PageCount,
    page_range::PageRange,
    SegmentId, VolumeId,
};
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
        page_count: PageCount,
        timestamp: SystemTime,
    ) -> Self {
        Self {
            vid: vid.copy_to_bytes(),
            lsn: lsn.into(),
            checkpoint_lsn: checkpoint_lsn.into(),
            page_count: page_count.into(),
            timestamp: Some(timestamp.into()),
        }
    }

    pub fn vid(&self) -> Result<&VolumeId, GidParseErr> {
        self.vid.as_ref().try_into()
    }

    pub fn lsn(&self) -> LSN {
        self.lsn.into()
    }

    pub fn checkpoint(&self) -> LSN {
        self.checkpoint_lsn.into()
    }

    pub fn page_count(&self) -> PageCount {
        self.page_count.into()
    }

    /// Returns the range of page offsets in the snapshot.
    pub fn offsets(&self) -> PageRange {
        self.page_count().offsets()
    }

    pub fn system_time(&self) -> Result<Option<SystemTime>, TimestampError> {
        self.timestamp.map(|ts| ts.try_into()).transpose()
    }
}

impl From<LsnRange> for RangeInclusive<LSN> {
    fn from(range: LsnRange) -> Self {
        range.inclusive_start.into()..=range.inclusive_end.into()
    }
}

impl<T: RangeBounds<LSN>> From<T> for LsnRange {
    fn from(range: T) -> Self {
        let inclusive_start = range.try_start().unwrap_or(LSN::ZERO).into();
        let inclusive_end = range.try_end().unwrap_or(LSN::MAX).into();
        Self { inclusive_start, inclusive_end }
    }
}
