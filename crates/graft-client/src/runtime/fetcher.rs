use std::fmt::Debug;

use culprit::Result;
use graft_core::{lsn::LSN, page::Page, PageIdx, VolumeId};

use crate::ClientErr;

use super::storage::Storage;

mod mock_fetcher;
mod net_fetcher;

pub use mock_fetcher::MockFetcher;
pub use net_fetcher::NetFetcher;

pub trait Fetcher: Debug + Send + Sync + 'static {
    /// Fetch a specific page, update storage, and return it.
    fn fetch_page(
        &self,
        storage: &Storage,
        vid: &VolumeId,
        remote_lsn: LSN,
        local_lsn: LSN,
        pageidx: PageIdx,
    ) -> Result<Page, ClientErr>;
}
