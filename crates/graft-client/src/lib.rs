use std::ops::RangeBounds;

use bytes::Bytes;
use futures::TryFutureExt;
use graft_core::{lsn::LSN, offset::Offset, VolumeId};
use graft_proto::{
    common::v1::{Commit, GraftErr, GraftErrCode, LsnRange, SegmentInfo, Snapshot},
    metastore::v1::{
        CommitRequest, CommitResponse, PullCommitsRequest, PullCommitsResponse, PullOffsetsRequest,
        PullOffsetsResponse, SnapshotRequest, SnapshotResponse,
    },
    pagestore::v1::{
        PageAtOffset, ReadPagesRequest, ReadPagesResponse, WritePagesRequest, WritePagesResponse,
    },
};
use prost::Message;
use reqwest::{header::CONTENT_TYPE, Url};
use serde::{Deserialize, Serialize};
use splinter::SplinterRef;
use thiserror::Error;
use url::ParseError;

#[derive(Debug, Error)]
pub enum ClientBuildErr {
    #[error("failed to build reqwest client: {0}")]
    ReqwestErr(#[from] reqwest::Error),

    #[error("failed to parse URL: {0}")]
    UrlParseErr(#[from] ParseError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientBuilder {
    /// The root URL (without any trailing path)
    pub endpoint: Url,
}

impl ClientBuilder {
    pub fn new(endpoint: Url) -> Self {
        Self { endpoint }
    }

    fn http(&self) -> reqwest::Result<reqwest::Client> {
        reqwest::Client::builder().brotli(true).build()
    }

    pub fn build<T: TryFrom<ClientBuilder, Error = ClientBuildErr>>(
        self,
    ) -> Result<T, ClientBuildErr> {
        self.try_into()
    }
}

#[derive(Debug, Error)]
pub enum ClientErr {
    #[error("graft error: {0}")]
    GraftErr(#[from] GraftErr),

    #[error("request failed: {0}")]
    RequestErr(#[from] reqwest::Error),

    #[error("failed to parse response: {0}")]
    ResponseParseErr(#[from] prost::DecodeError),

    #[error("failed to parse splinter: {0}")]
    SplinterParseErr(#[from] splinter::DecodeErr),
}

impl ClientErr {
    fn is_snapshot_missing(&self) -> bool {
        match self {
            ClientErr::GraftErr(err) => err.code() == GraftErrCode::SnapshotMissing,
            _ => false,
        }
    }
}

async fn prost_request<Req: Message, Resp: Message + Default>(
    http: &reqwest::Client,
    url: Url,
    req: Req,
) -> Result<Resp, ClientErr> {
    let req = http
        .post(url)
        .body(req.encode_to_vec())
        .header(CONTENT_TYPE, "application/x-protobuf");
    log::trace!("sending request: {:?}", req);
    let resp = req.send().await?;
    log::trace!("received response: {:?}", resp);
    let success = resp.status().is_success();
    let body = resp.bytes().await?;
    if success {
        Ok(Resp::decode(body)?)
    } else {
        let err = GraftErr::decode(body)?;
        Err(ClientErr::GraftErr(err))
    }
}

pub struct MetastoreClient {
    /// The metastore root URL (without any trailing path)
    endpoint: Url,
    http: reqwest::Client,
}

impl TryFrom<ClientBuilder> for MetastoreClient {
    type Error = ClientBuildErr;

    fn try_from(builder: ClientBuilder) -> Result<Self, Self::Error> {
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
    ) -> Result<Option<Snapshot>, ClientErr> {
        let url = self.endpoint.join("snapshot").unwrap();
        let req = SnapshotRequest { vid: vid.copy_to_bytes(), lsn };
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
    ) -> Result<Option<(Snapshot, LsnRange, SplinterRef<Bytes>)>, ClientErr> {
        let url = self.endpoint.join("pull_offsets").unwrap();
        let req = PullOffsetsRequest {
            vid: vid.copy_to_bytes(),
            range: Some(LsnRange::from_bounds(&range)),
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

    pub async fn pull_commits<R>(&self, vid: &VolumeId, range: R) -> Result<Vec<Commit>, ClientErr>
    where
        R: RangeBounds<LSN>,
    {
        let url = self.endpoint.join("pull_commits").unwrap();
        let req = PullCommitsRequest {
            vid: vid.copy_to_bytes(),
            range: Some(LsnRange::from_bounds(&range)),
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
    ) -> Result<Snapshot, ClientErr> {
        let url = self.endpoint.join("commit").unwrap();
        let req = CommitRequest {
            vid: vid.copy_to_bytes(),
            snapshot_lsn: snapshot,
            last_offset,
            segments,
        };
        prost_request::<_, CommitResponse>(&self.http, url, req)
            .map_ok(|r| r.snapshot.expect("missing snapshot after commit"))
            .await
    }
}

pub struct PagestoreClient {
    endpoint: Url,
    http: reqwest::Client,
}

impl TryFrom<ClientBuilder> for PagestoreClient {
    type Error = ClientBuildErr;

    fn try_from(builder: ClientBuilder) -> Result<Self, Self::Error> {
        let endpoint = builder.endpoint.join("pagestore/v1/")?;
        let http = builder.http()?;
        Ok(Self { endpoint, http })
    }
}

impl PagestoreClient {
    pub async fn read_pages(
        &self,
        vid: &VolumeId,
        lsn: LSN,
        offsets: Bytes,
    ) -> Result<Vec<PageAtOffset>, ClientErr> {
        let url = self.endpoint.join("read_pages").unwrap();
        let req = ReadPagesRequest { vid: vid.copy_to_bytes(), lsn, offsets };
        prost_request::<_, ReadPagesResponse>(&self.http, url, req)
            .map_ok(|r| r.pages)
            .await
    }

    pub async fn write_pages(
        &self,
        vid: &VolumeId,
        pages: Vec<PageAtOffset>,
    ) -> Result<Vec<SegmentInfo>, ClientErr> {
        let url = self.endpoint.join("write_pages").unwrap();
        let req = WritePagesRequest { vid: vid.copy_to_bytes(), pages };
        prost_request::<_, WritePagesResponse>(&self.http, url, req)
            .map_ok(|r| r.segments)
            .await
    }
}
