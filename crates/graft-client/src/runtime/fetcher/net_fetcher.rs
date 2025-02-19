use culprit::{Result, ResultExt};
use graft_core::{
    lsn::LSN,
    page::{Page, EMPTY_PAGE},
    page_offset::PageOffset,
    VolumeId,
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
        offset: PageOffset,
    ) -> Result<Page, ClientErr> {
        let _span = tracing::trace_span!(
            "fetching page from pagestore",
            ?vid,
            ?remote_lsn,
            ?local_lsn,
            ?offset,
        )
        .entered();
        let offsets = Splinter::from_iter([offset]).serialize_to_bytes();
        let pages = self
            .clients
            .pagestore()
            .read_pages(vid, remote_lsn, offsets)?;
        if pages.is_empty() {
            return Ok(EMPTY_PAGE);
        }
        let page = pages[0].clone();
        assert!(
            page.offset().or_into_ctx()? == offset,
            "received page at wrong offset"
        );
        storage.receive_pages(vid, local_lsn, pages).or_into_ctx()?;
        Ok(page.page().expect("page has invalid size"))
    }
}
