use bytes::Bytes;
use culprit::{Culprit, ResultExt};
use graft_core::{gid::ClientId, lsn::LSN, page_count::PageCount, VolumeId};
use graft_proto::{
    common::v1::{Commit, LsnRange, SegmentInfo, Snapshot},
    metastore::v1::{
        CommitRequest, CommitResponse, PullCommitsRequest, PullCommitsResponse, PullGraftRequest,
        PullGraftResponse, SnapshotRequest, SnapshotResponse,
    },
};
use splinter::SplinterRef;
use std::ops::RangeBounds;
use url::Url;

use crate::NetClient;
use crate::{error, net::EndpointBuilder};

#[derive(Debug, Clone)]
pub struct MetastoreClient {
    endpoint: EndpointBuilder,
    client: NetClient,
}

impl MetastoreClient {
    pub fn new(root: Url, client: NetClient) -> Self {
        Self { endpoint: root.into(), client }
    }

    pub fn snapshot(
        &self,
        vid: &VolumeId,
        lsn: Option<LSN>,
    ) -> Result<Option<Snapshot>, Culprit<error::ClientErr>> {
        let uri = self.endpoint.build("/metastore/v1/snapshot")?;
        let req = SnapshotRequest {
            vid: vid.copy_to_bytes(),
            lsn: lsn.map(Into::into),
        };
        match self.client.send::<_, SnapshotResponse>(uri, req) {
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
        let uri = self.endpoint.build("/metastore/v1/pull_offsets")?;
        let req = PullGraftRequest {
            vid: vid.copy_to_bytes(),
            range: Some(LsnRange::from_range(range)),
        };
        match self.client.send::<_, PullGraftResponse>(uri, req) {
            Ok(resp) => {
                let snapshot = resp.snapshot.expect("snapshot is missing");
                let range = resp.range.expect("range is missing");
                let graft = SplinterRef::from_bytes(resp.graft).or_into_ctx()?;
                Ok(Some((snapshot, range, graft)))
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
        let uri = self.endpoint.build("/metastore/v1/pull_commits")?;
        let req = PullCommitsRequest {
            vid: vid.copy_to_bytes(),
            range: Some(LsnRange::from_range(range)),
        };
        self.client
            .send::<_, PullCommitsResponse>(uri, req)
            .map(|resp| resp.commits)
    }

    pub fn commit(
        &self,
        vid: &VolumeId,
        cid: &ClientId,
        snapshot_lsn: Option<LSN>,
        page_count: PageCount,
        segments: Vec<SegmentInfo>,
    ) -> Result<Snapshot, Culprit<error::ClientErr>> {
        let uri = self.endpoint.build("/metastore/v1/commit")?;
        let req = CommitRequest {
            vid: vid.copy_to_bytes(),
            cid: cid.copy_to_bytes(),
            snapshot_lsn: snapshot_lsn.map(Into::into),
            page_count: page_count.into(),
            segments,
        };
        self.client
            .send::<_, CommitResponse>(uri, req)
            .map(|r| r.snapshot.expect("missing snapshot after commit"))
    }
}
