use graft_proto::{
    metastore::v1::{
        CommitRequest, CommitResponse, PullCommitsRequest, PullCommitsResponse, PullOffsetsRequest,
        PullOffsetsResponse, SnapshotRequest, SnapshotResponse,
    },
    pagestore::v1::{ReadPagesRequest, ReadPagesResponse, WritePagesRequest, WritePagesResponse},
};
use prost::Message;
use reqwest::{header::CONTENT_TYPE, Url};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientErr {
    #[error("failed to make request: {0}")]
    RequestErr(#[from] reqwest::Error),

    #[error("failed to parse response: {0}")]
    ResponseParseErr(#[from] prost::DecodeError),
}

async fn prost_request<Req: Message, Resp: Message + Default>(
    http: &reqwest::Client,
    url: Url,
    req: Req,
) -> Result<Resp, ClientErr> {
    let resp = http
        .post(url)
        .body(req.encode_to_vec())
        .header(CONTENT_TYPE, "application/x-protobuf")
        .send()
        .await?
        .error_for_status()?;
    Ok(Resp::decode(resp.bytes().await?)?)
}

pub struct MetaStoreClient {
    endpoint: Url,
    http: reqwest::Client,
}

impl Default for MetaStoreClient {
    fn default() -> Self {
        Self {
            endpoint: Url::parse("http://localhost:3001/metastore/v1/").unwrap(),
            http: Default::default(),
        }
    }
}

impl MetaStoreClient {
    pub fn new(endpoint: Url, http: reqwest::Client) -> Self {
        Self { endpoint, http }
    }

    pub async fn snapshot(&self, req: SnapshotRequest) -> Result<SnapshotResponse, ClientErr> {
        let url = self.endpoint.join("snapshot").unwrap();
        prost_request(&self.http, url, req).await
    }

    pub async fn pull_offsets(
        &self,
        req: PullOffsetsRequest,
    ) -> Result<PullOffsetsResponse, ClientErr> {
        let url = self.endpoint.join("pull_offsets").unwrap();
        prost_request(&self.http, url, req).await
    }

    pub async fn pull_commits(
        &self,
        req: PullCommitsRequest,
    ) -> Result<PullCommitsResponse, ClientErr> {
        let url = self.endpoint.join("pull_commits").unwrap();
        prost_request(&self.http, url, req).await
    }

    pub async fn commit(&self, req: CommitRequest) -> Result<CommitResponse, ClientErr> {
        let url = self.endpoint.join("commit").unwrap();
        prost_request(&self.http, url, req).await
    }
}

pub struct PageStoreClient {
    endpoint: Url,
    http: reqwest::Client,
}

impl Default for PageStoreClient {
    fn default() -> Self {
        Self {
            endpoint: Url::parse("http://localhost:3030/pagestore/v1/").unwrap(),
            http: Default::default(),
        }
    }
}

impl PageStoreClient {
    pub fn new(endpoint: Url, http: reqwest::Client) -> Self {
        Self { endpoint, http }
    }

    pub async fn read_pages(&self, req: ReadPagesRequest) -> Result<ReadPagesResponse, ClientErr> {
        let url = self.endpoint.join("read_pages").unwrap();
        prost_request(&self.http, url, req).await
    }

    pub async fn write_pages(
        &self,
        req: WritePagesRequest,
    ) -> Result<WritePagesResponse, ClientErr> {
        let url = self.endpoint.join("write_pages").unwrap();
        prost_request(&self.http, url, req).await
    }
}
