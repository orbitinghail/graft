use bytes::Bytes;
use culprit::{Culprit, ResultExt};
use graft_core::{lsn::LSN, page_count::PageCount, VolumeId};
use graft_proto::{
    common::v1::{Commit, LsnRange, SegmentInfo, Snapshot},
    metastore::v1::{
        CommitRequest, CommitResponse, PullCommitsRequest, PullCommitsResponse, PullOffsetsRequest,
        PullOffsetsResponse, SnapshotRequest, SnapshotResponse,
    },
};
use splinter::SplinterRef;
use std::ops::RangeBounds;
use ureq::Agent;
use url::Url;

use crate::builder;
use crate::error;
use crate::net::prost_request;

#[derive(Debug, Clone)]
pub struct MetastoreClient {
    /// The metastore root URL (without any trailing path)
    endpoint: Url,
    agent: Agent,
}

impl TryFrom<builder::ClientBuilder> for MetastoreClient {
    type Error = Culprit<builder::ClientBuildErr>;

    fn try_from(builder: builder::ClientBuilder) -> Result<Self, Self::Error> {
        let endpoint = builder.endpoint().join("metastore/v1/")?;
        let agent = builder.agent();
        Ok(Self { endpoint, agent })
    }
}

impl MetastoreClient {
    pub fn snapshot(
        &self,
        vid: &VolumeId,
        lsn: Option<LSN>,
    ) -> Result<Option<Snapshot>, Culprit<error::ClientErr>> {
        let url = self.endpoint.join("snapshot").unwrap();
        let req = SnapshotRequest {
            vid: vid.copy_to_bytes(),
            lsn: lsn.map(Into::into),
        };
        match prost_request::<_, SnapshotResponse>(&self.agent, url, req) {
            Ok(resp) => Ok(resp.snapshot),
            Err(err) if err.ctx().is_snapshot_missing() => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn pull_offsets<R: RangeBounds<LSN>>(
        &self,
        vid: &VolumeId,
        range: R,
    ) -> Result<Option<(Snapshot, LsnRange, SplinterRef<Bytes>)>, Culprit<error::ClientErr>> {
        let url = self.endpoint.join("pull_offsets").unwrap();
        let req = PullOffsetsRequest {
            vid: vid.copy_to_bytes(),
            range: Some(LsnRange::from_range(range)),
        };
        match prost_request::<_, PullOffsetsResponse>(&self.agent, url, req) {
            Ok(resp) => {
                let snapshot = resp.snapshot.expect("snapshot is missing");
                let range = resp.range.expect("range is missing");
                let offsets = SplinterRef::from_bytes(resp.offsets).or_into_ctx()?;
                Ok(Some((snapshot, range, offsets)))
            }
            Err(err) if err.ctx().is_snapshot_missing() => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub fn pull_commits<R>(
        &self,
        vid: &VolumeId,
        range: R,
    ) -> Result<Vec<Commit>, Culprit<error::ClientErr>>
    where
        R: RangeBounds<LSN>,
    {
        let url = self.endpoint.join("pull_commits").unwrap();
        let req = PullCommitsRequest {
            vid: vid.copy_to_bytes(),
            range: Some(LsnRange::from_range(range)),
        };
        prost_request::<_, PullCommitsResponse>(&self.agent, url, req).map(|resp| resp.commits)
    }

    pub fn commit(
        &self,
        vid: &VolumeId,
        snapshot_lsn: Option<LSN>,
        page_count: PageCount,
        segments: Vec<SegmentInfo>,
    ) -> Result<Snapshot, Culprit<error::ClientErr>> {
        let url = self.endpoint.join("commit").unwrap();
        let req = CommitRequest {
            vid: vid.copy_to_bytes(),
            snapshot_lsn: snapshot_lsn.map(Into::into),
            page_count: page_count.into(),
            segments,
        };
        prost_request::<_, CommitResponse>(&self.agent, url, req)
            .map(|r| r.snapshot.expect("missing snapshot after commit"))
    }
}
