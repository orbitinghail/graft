use culprit::Result;
use graft_core::{page::Page, page_offset::PageOffset, VolumeId};

use crate::ClientErr;

use super::storage::{snapshot::Snapshot, Storage};

mod mock;
mod net;

pub use mock::MockFetcher;
pub use net::NetFetcher;

pub trait Fetcher: Send + Sync + 'static {
    /// Fetch a specific page, update storage, and return it.
    fn fetch_page(
        &self,
        storage: &Storage,
        vid: &VolumeId,
        snapshot: &Snapshot,
        offset: PageOffset,
    ) -> Result<Page, ClientErr>;
}
