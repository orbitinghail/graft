// pull in the generated types
include!("mod.rs");

use std::{error::Error, fmt::Display, ops::RangeBounds, time::SystemTime};

use bytes::Bytes;
use common::v1::{Commit, GraftErr, LsnRange, SegmentInfo};
use culprit::{Culprit, ResultExt};
use graft_core::{
    PageCount, PageIdx, SegmentId, VolumeId,
    gid::{ClientId, GidParseErr},
    lsn::{InvalidLSN, LSN, LSNRangeExt},
    page::{Page, PageSizeErr},
    page_idx::ConvertToPageIdxErr,
};
use pagestore::v1::PageAtIdx;
use prost_types::TimestampError;

pub use graft::common::v1::{GraftErrCode, Snapshot};
pub use graft::*;
use splinter_rs::{DecodeErr, SplinterRef};

impl Error for GraftErr {}
impl Display for GraftErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code(), self.message)
    }
}

impl GraftErrCode {
    pub fn is_client(self) -> bool {
        let code = self as i32;
        code >= GraftErrCode::Client as i32 && code < GraftErrCode::Server as i32
    }
}

impl Commit {
    pub fn snapshot(&self) -> &Snapshot {
        self.snapshot.as_ref().expect("snapshot is required")
    }
}

impl SegmentInfo {
    pub fn new(sid: &SegmentId, graft: Bytes) -> Self {
        Self { sid: sid.copy_to_bytes(), graft }
    }

    pub fn sid(&self) -> Result<&SegmentId, Culprit<GidParseErr>> {
        Ok(self.sid.as_ref().try_into()?)
    }

    pub fn graft(&self) -> Result<SplinterRef<Bytes>, Culprit<DecodeErr>> {
        SplinterRef::from_bytes(self.graft.clone())
    }
}

impl Snapshot {
    pub fn new(
        vid: &VolumeId,
        cid: &ClientId,
        lsn: LSN,
        checkpoint_lsn: LSN,
        page_count: PageCount,
        timestamp: SystemTime,
    ) -> Self {
        Self {
            vid: vid.copy_to_bytes(),
            cid: cid.copy_to_bytes(),
            lsn: lsn.into(),
            checkpoint_lsn: checkpoint_lsn.into(),
            page_count: page_count.into(),
            timestamp: Some(timestamp.into()),
        }
    }

    pub fn vid(&self) -> Result<&VolumeId, Culprit<GidParseErr>> {
        Ok(self.vid.as_ref().try_into()?)
    }

    pub fn cid(&self) -> Result<&ClientId, Culprit<GidParseErr>> {
        Ok(self.cid.as_ref().try_into()?)
    }

    pub fn lsn(&self) -> Result<LSN, Culprit<InvalidLSN>> {
        LSN::try_from(self.lsn).or_into_ctx()
    }

    pub fn checkpoint(&self) -> Result<LSN, Culprit<InvalidLSN>> {
        LSN::try_from(self.checkpoint_lsn).or_into_ctx()
    }

    pub fn pages(&self) -> PageCount {
        self.page_count.into()
    }

    pub fn system_time(&self) -> Result<Option<SystemTime>, TimestampError> {
        self.timestamp.map(|ts| ts.try_into()).transpose()
    }
}

impl LsnRange {
    pub fn from_range<T: RangeBounds<LSN>>(range: T) -> Self {
        let inclusive_start = range.try_start().unwrap_or(LSN::FIRST).into();
        let inclusive_end = range.try_end().map(|lsn| lsn.into());
        Self { inclusive_start, inclusive_end }
    }

    pub fn start(&self) -> Result<LSN, Culprit<InvalidLSN>> {
        LSN::try_from(self.inclusive_start).or_into_ctx()
    }

    pub fn end(&self) -> Result<Option<LSN>, Culprit<InvalidLSN>> {
        match self.inclusive_end {
            Some(end) => LSN::try_from(end).or_into_ctx().map(Some),
            None => Ok(None),
        }
    }
}

impl PageAtIdx {
    pub fn new(pageidx: PageIdx, page: Page) -> Self {
        Self {
            pageidx: pageidx.into(),
            data: page.into(),
        }
    }

    #[inline]
    pub fn pageidx(&self) -> Result<PageIdx, Culprit<ConvertToPageIdxErr>> {
        PageIdx::try_from(self.pageidx).or_into_ctx()
    }

    #[inline]
    pub fn page(&self) -> Result<Page, Culprit<PageSizeErr>> {
        self.data.clone().try_into()
    }
}
