use culprit::Result;
use graft_core::{
    lsn::LSN,
    page::{self, EMPTY_PAGE},
    page_offset::PageOffset,
    VolumeId,
};

use crate::{runtime::storage::Storage, ClientErr};

use super::Fetcher;

#[derive(Debug)]
pub struct MockFetcher;

impl Fetcher for MockFetcher {
    fn fetch_page(
        &self,
        _storage: &Storage,
        _vid: &VolumeId,
        _remote_lsn: LSN,
        _local_lsn: LSN,
        _offset: PageOffset,
    ) -> Result<page::Page, ClientErr> {
        Ok(EMPTY_PAGE)
    }
}
