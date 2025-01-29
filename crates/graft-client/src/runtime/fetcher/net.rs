use culprit::{Result, ResultExt};
use graft_core::{
    page::{Page, EMPTY_PAGE},
    page_offset::PageOffset,
    VolumeId,
};
use splinter::Splinter;

use crate::{
    runtime::storage::{snapshot::Snapshot, Storage},
    ClientErr, ClientPair,
};

use super::Fetcher;

#[derive(Debug)]
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
        snapshot: &Snapshot,
        offset: PageOffset,
    ) -> Result<Page, ClientErr> {
        if let Some(remote) = snapshot.remote() {
            let offsets = Splinter::from_iter([offset]).serialize_to_bytes();
            let pages = self.clients.pagestore().read_pages(vid, remote, offsets)?;
            if pages.is_empty() {
                return Ok(EMPTY_PAGE);
            }
            let page = pages[0].clone();
            assert!(page.offset() == offset, "received page at wrong offset");
            storage
                .receive_pages(vid, snapshot.local(), pages)
                .or_into_ctx()?;
            Ok(page.page().expect("page has invalid size"))
        } else {
            return Ok(EMPTY_PAGE);
        }
    }
}
