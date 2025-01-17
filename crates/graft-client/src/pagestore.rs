use bytes::Bytes;
use culprit::Culprit;
use graft_core::lsn::LSN;
use graft_core::VolumeId;
use graft_proto::{
    common::v1::SegmentInfo,
    pagestore::v1::{
        PageAtOffset, ReadPagesRequest, ReadPagesResponse, WritePagesRequest, WritePagesResponse,
    },
};
use ureq::Agent;
use url::Url;

use crate::builder::ClientBuildErr;
use crate::builder::ClientBuilder;
use crate::net::prost_request;
use crate::ClientErr;

#[derive(Debug, Clone)]
pub struct PagestoreClient {
    endpoint: Url,
    agent: Agent,
}

impl TryFrom<ClientBuilder> for PagestoreClient {
    type Error = Culprit<ClientBuildErr>;

    fn try_from(builder: ClientBuilder) -> Result<Self, Self::Error> {
        let endpoint = builder.endpoint().join("pagestore/v1/")?;
        let agent = builder.agent();
        Ok(Self { endpoint, agent })
    }
}

impl PagestoreClient {
    pub fn read_pages(
        &self,
        vid: &VolumeId,
        lsn: LSN,
        offsets: Bytes,
    ) -> Result<Vec<PageAtOffset>, Culprit<ClientErr>> {
        let url = self.endpoint.join("read_pages").unwrap();
        let req = ReadPagesRequest {
            vid: vid.copy_to_bytes(),
            lsn: lsn.into(),
            offsets,
        };
        prost_request::<_, ReadPagesResponse>(&self.agent, url, req).map(|r| r.pages)
    }

    pub fn write_pages(
        &self,
        vid: &VolumeId,
        pages: Vec<PageAtOffset>,
    ) -> Result<Vec<SegmentInfo>, Culprit<ClientErr>> {
        let url = self.endpoint.join("write_pages").unwrap();
        let req = WritePagesRequest { vid: vid.copy_to_bytes(), pages };
        prost_request::<_, WritePagesResponse>(&self.agent, url, req).map(|r| r.segments)
    }
}
