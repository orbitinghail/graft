use bytes::Bytes;
use culprit::Culprit;
use graft_core::{VolumeId, page_count::PageCount};
use graft_core::lsn::LSN;
use graft_proto::{
    common::v1::SegmentInfo,
    pagestore::v1::{
        PageAtIdx, ReadPagesRequest, ReadPagesResponse, WritePagesRequest, WritePagesResponse,
    },
};
use std::sync::atomic::{AtomicU32, Ordering};
use url::Url;

use crate::NetClient;
use crate::{ClientErr, net::EndpointBuilder};

#[derive(Debug)]
pub struct PagestoreClient {
    endpoint: EndpointBuilder,
    client: NetClient,
    pages_read_count: AtomicU32,
}

impl PagestoreClient {
    pub fn new(root: Url, client: NetClient) -> Self {
        Self { 
            endpoint: root.into(), 
            client,
            pages_read_count: AtomicU32::new(0),
        }
    }

    pub fn read_pages(
        &self,
        vid: &VolumeId,
        lsn: LSN,
        graft: Bytes,
    ) -> Result<Vec<PageAtIdx>, Culprit<ClientErr>> {
        let uri = self.endpoint.build("/pagestore/v1/read_pages")?;
        let req = ReadPagesRequest {
            vid: vid.copy_to_bytes(),
            lsn: lsn.into(),
            graft,
        };
        let result = self.client
            .send::<_, ReadPagesResponse>(uri, req)
            .map(|r| r.pages);
        
        // Increment the counter with the number of pages read
        if let Ok(ref pages) = result {
            self.pages_read_count.fetch_add(pages.len() as u32, Ordering::Relaxed);
        }
        
        result
    }

    pub fn write_pages(
        &self,
        vid: &VolumeId,
        pages: Vec<PageAtIdx>,
    ) -> Result<Vec<SegmentInfo>, Culprit<ClientErr>> {
        let uri = self.endpoint.build("/pagestore/v1/write_pages")?;
        let req = WritePagesRequest { vid: vid.copy_to_bytes(), pages };
        self.client
            .send::<_, WritePagesResponse>(uri, req)
            .map(|r| r.segments)
    }

    /// Returns the total number of pages read by this client.
    pub fn pages_read(&self) -> PageCount {
        PageCount::new(self.pages_read_count.load(Ordering::Relaxed))
    }

    /// Resets the pages read counter to zero.
    pub fn reset_pages_read(&self) {
        self.pages_read_count.store(0, Ordering::Relaxed);
    }
}

impl Clone for PagestoreClient {
    fn clone(&self) -> Self {
        Self {
            endpoint: self.endpoint.clone(),
            client: self.client.clone(),
            pages_read_count: AtomicU32::new(0), // New counter for each clone
        }
    }
}
