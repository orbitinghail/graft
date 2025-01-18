use culprit::Result;
use graft_core::{page::Page, page_offset::PageOffset, VolumeId};

use crate::ClientErr;

use super::storage::{snapshot::Snapshot, Storage};

mod mock;
mod net;

pub use mock::MockFetcher;
pub use net::NetFetcher;

pub trait Fetcher: Send + Sync + 'static {
    /// Update storage with the latest snapshot of the specified Volume
    fn pull_snapshot(&self, storage: &Storage, vid: &VolumeId) -> Result<(), ClientErr>;

    /// Fetch a specific page, update storage, and return it.
    /// Snapshot refers to a valid remote Snapshot.
    fn fetch_page(
        &self,
        storage: &Storage,
        vid: &VolumeId,
        local: &Snapshot,
        remote: &Snapshot,
        offset: PageOffset,
    ) -> Result<Page, ClientErr>;
}
