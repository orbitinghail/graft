use culprit::{Result, ResultExt};
use graft_core::{page::Page, page_offset::PageOffset, VolumeId};
use splinter::Splinter;

use crate::{
    runtime::storage::{snapshot::Snapshot, Storage},
    ClientErr, ClientPair,
};

use super::Fetcher;

pub struct NetFetcher {
    clients: ClientPair,
}

impl NetFetcher {
    pub fn new(clients: ClientPair) -> Self {
        Self { clients }
    }
}

impl Fetcher for NetFetcher {
    fn pull_snapshot(&self, storage: &Storage, vid: &VolumeId) -> Result<(), ClientErr> {
        if let Some(snapshot) = self.clients.metastore().snapshot(vid, None)? {
            let changed = Splinter::default().serialize_to_splinter_ref();
            storage
                .receive_remote_commit(vid, snapshot.is_checkpoint(), snapshot.into(), changed)
                .or_into_ctx()?;
        }
        Ok(())
    }

    fn fetch_page(
        &self,
        storage: &Storage,
        vid: &VolumeId,
        local: &Snapshot,
        remote: &Snapshot,
        offset: PageOffset,
    ) -> Result<Page, ClientErr> {
        let offsets = Splinter::from_iter([offset]).serialize_to_bytes();
        let pages = self
            .clients
            .pagestore()
            .read_pages(vid, remote.lsn(), offsets)?;
        assert!(!pages.is_empty(), "missing page");
        let page = pages[0].clone();
        assert!(page.offset() == offset, "received page at wrong offset");
        storage
            .receive_pages(vid, local.lsn(), pages)
            .or_into_ctx()?;
        Ok(page.page().expect("page has invalid size"))
    }
}
