use graft_core::{page_count::PageCount, VolumeId};
use serde::{Deserialize, Serialize};

use super::storage::snapshot::Snapshot;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VolumeSnapshot {
    vid: VolumeId,
    local: Snapshot,
    remote: Option<Snapshot>,
}

impl VolumeSnapshot {
    pub(crate) fn new(vid: VolumeId, local: Snapshot, remote: Option<Snapshot>) -> Self {
        Self { vid, local, remote }
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    pub fn local(&self) -> &Snapshot {
        &self.local
    }

    #[inline]
    pub fn remote(&self) -> Option<&Snapshot> {
        self.remote.as_ref()
    }

    /// Returns the number of pages in the volume at this snapshot
    #[inline]
    pub fn pages(&self) -> PageCount {
        self.local.pages()
    }

    pub(crate) fn with_local(self, local: Snapshot) -> Self {
        Self { local, ..self }
    }
}
