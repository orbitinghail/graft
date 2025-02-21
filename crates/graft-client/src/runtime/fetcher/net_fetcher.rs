use culprit::{Result, ResultExt};
use graft_core::{
    lsn::LSN,
    page::{Page, EMPTY_PAGE},
    PageIdx, VolumeId,
};
use splinter::Splinter;

use crate::{runtime::storage::Storage, ClientErr, ClientPair};

use super::Fetcher;

#[derive(Debug, Clone)]
pub struct NetFetcher {
    clients: ClientPair,
}

impl NetFetcher {
    pub fn new(clients: ClientPair) -> Self {
        Self { clients }
    }
}

impl Fetcher for NetFetcher {
    fn fetch_page(
        &self,
        storage: &Storage,
        vid: &VolumeId,
        remote_lsn: LSN,
        local_lsn: LSN,
        pageidx: PageIdx,
    ) -> Result<Page, ClientErr> {
        let _span = tracing::trace_span!(
            "fetching page from pagestore",
            ?vid,
            ?remote_lsn,
            ?local_lsn,
            ?pageidx,
        )
        .entered();
        let graft = Splinter::from_iter([pageidx]).serialize_to_bytes();
        let pages = self
            .clients
            .pagestore()
            .read_pages(vid, remote_lsn, graft)?;
        if pages.is_empty() {
            return Ok(EMPTY_PAGE);
        }
        let page = pages[0].clone();
        assert!(
            page.pageidx().or_into_ctx()? == pageidx,
            "received page at wrong page index"
        );
        storage.receive_pages(vid, local_lsn, pages).or_into_ctx()?;
        Ok(page.page().expect("page has invalid size"))
    }
}
