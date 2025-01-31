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
use url::Url;

use crate::NetClient;
use crate::{net::EndpointBuilder, ClientErr};

#[derive(Debug, Clone)]
pub struct PagestoreClient {
    endpoint: EndpointBuilder,
    client: NetClient,
}

impl PagestoreClient {
    pub fn new(root: Url, client: NetClient) -> Self {
        Self { endpoint: root.into(), client }
    }

    pub fn read_pages(
        &self,
        vid: &VolumeId,
        lsn: LSN,
        offsets: Bytes,
    ) -> Result<Vec<PageAtOffset>, Culprit<ClientErr>> {
        let uri = self.endpoint.build("/pagestore/v1/read_pages")?;
        let req = ReadPagesRequest {
            vid: vid.copy_to_bytes(),
            lsn: lsn.into(),
            offsets,
        };
        self.client
            .send::<_, ReadPagesResponse>(uri, req)
            .map(|r| r.pages)
    }

    pub fn write_pages(
        &self,
        vid: &VolumeId,
        pages: Vec<PageAtOffset>,
    ) -> Result<Vec<SegmentInfo>, Culprit<ClientErr>> {
        let uri = self.endpoint.build("/pagestore/v1/write_pages")?;
        let req = WritePagesRequest { vid: vid.copy_to_bytes(), pages };
        self.client
            .send::<_, WritePagesResponse>(uri, req)
            .map(|r| r.segments)
    }
}
