use culprit::Result;
use graft_core::{
    page::{self, EMPTY_PAGE},
    page_offset::PageOffset,
    VolumeId,
};

use crate::{
    runtime::storage::{snapshot::Snapshot, Storage},
    ClientErr,
};

use super::Fetcher;

pub struct MockFetcher;

impl Fetcher for MockFetcher {
    fn pull_snapshot(&self, _storage: &Storage, _vid: &VolumeId) -> Result<(), ClientErr> {
        Ok(())
    }

    fn fetch_page(
        &self,
        _storage: &Storage,
        _vid: &VolumeId,
        _local: &Snapshot,
        _remote: &Snapshot,
        _offset: PageOffset,
    ) -> Result<page::Page, ClientErr> {
        Ok(EMPTY_PAGE)
    }
}
