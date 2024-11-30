use bytes::Bytes;
use futures::TryFutureExt;
use graft_core::lsn::LSN;
use graft_core::offset::Offset;
use graft_core::VolumeId;
use graft_proto::common::v1::Commit;
use graft_proto::common::v1::LsnRange;
use graft_proto::common::v1::SegmentInfo;
use graft_proto::common::v1::Snapshot;
use graft_proto::metastore::v1::CommitRequest;
use graft_proto::metastore::v1::CommitResponse;
use graft_proto::metastore::v1::PullCommitsRequest;
use graft_proto::metastore::v1::PullCommitsResponse;
use graft_proto::metastore::v1::PullOffsetsRequest;
use graft_proto::metastore::v1::PullOffsetsResponse;
use graft_proto::metastore::v1::SnapshotRequest;
use graft_proto::metastore::v1::SnapshotResponse;
use reqwest::Url;
use splinter::SplinterRef;
use std::ops::RangeBounds;

use crate::builder;
use crate::error;
use crate::request::prost_request;

pub struct MetastoreClient {
    /// The metastore root URL (without any trailing path)
    pub(crate) endpoint: Url,
    pub(crate) http: reqwest::Client,
}

impl TryFrom<builder::ClientBuilder> for MetastoreClient {
    type Error = builder::ClientBuildErr;

    fn try_from(builder: builder::ClientBuilder) -> Result<Self, Self::Error> {
        let endpoint = builder.endpoint.join("metastore/v1/")?;
        let http = builder.http()?;
        Ok(Self { endpoint, http })
    }
}

impl MetastoreClient {
    pub async fn snapshot(
        &self,
        vid: &VolumeId,
        lsn: Option<LSN>,
    ) -> Result<Option<Snapshot>, error::ClientErr> {
        let url = self.endpoint.join("snapshot").unwrap();
        let req = SnapshotRequest {
            vid: vid.copy_to_bytes(),
            lsn: lsn.map(Into::into),
        };
        match prost_request::<_, SnapshotResponse>(&self.http, url, req).await {
            Ok(resp) => Ok(resp.snapshot),
            Err(err) if err.is_snapshot_missing() => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn pull_offsets<R: RangeBounds<LSN>>(
        &self,
        vid: &VolumeId,
        range: R,
    ) -> Result<Option<(Snapshot, LsnRange, SplinterRef<Bytes>)>, error::ClientErr> {
        let url = self.endpoint.join("pull_offsets").unwrap();
        let req = PullOffsetsRequest {
            vid: vid.copy_to_bytes(),
            range: Some(range.into()),
        };
        match prost_request::<_, PullOffsetsResponse>(&self.http, url, req).await {
            Ok(resp) => {
                let snapshot = resp.snapshot.expect("snapshot is missing");
                let range = resp.range.expect("range is missing");
                let offsets = SplinterRef::from_bytes(resp.offsets)?;
                Ok(Some((snapshot, range, offsets)))
            }
            Err(err) if err.is_snapshot_missing() => Ok(None),
            Err(err) => Err(err),
        }
    }

    pub async fn pull_commits<R>(
        &self,
        vid: &VolumeId,
        range: R,
    ) -> Result<Vec<Commit>, error::ClientErr>
    where
        R: RangeBounds<LSN>,
    {
        let url = self.endpoint.join("pull_commits").unwrap();
        let req = PullCommitsRequest {
            vid: vid.copy_to_bytes(),
            range: Some(range.into()),
        };
        prost_request::<_, PullCommitsResponse>(&self.http, url, req)
            .map_ok(|resp| resp.commits)
            .await
    }

    pub async fn commit(
        &self,
        vid: &VolumeId,
        snapshot: Option<LSN>,
        last_offset: Offset,
        segments: Vec<SegmentInfo>,
    ) -> Result<Snapshot, error::ClientErr> {
        let url = self.endpoint.join("commit").unwrap();
        let req = CommitRequest {
            vid: vid.copy_to_bytes(),
            snapshot_lsn: snapshot.map(Into::into),
            last_offset,
            segments,
        };
        prost_request::<_, CommitResponse>(&self.http, url, req)
            .map_ok(|r| r.snapshot.expect("missing snapshot after commit"))
            .await
    }
}
