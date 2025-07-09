use bilrost::Message;

use crate::{PageCount, VolumeId, commit::Commit, lsn::LSN};

/// A Volume Snapshot.
#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct Snapshot {
    /// The Volume's ID
    #[bilrost(1)]
    vid: VolumeId,

    /// The Snapshot LSN
    #[bilrost(2)]
    lsn: LSN,

    /// The Volume's `PageCount` at this LSN.
    #[bilrost(3)]
    page_count: PageCount,
}

impl Snapshot {
    pub fn new(vid: VolumeId, lsn: LSN, page_count: PageCount) -> Self {
        Self { vid, lsn, page_count }
    }
}

impl From<Snapshot> for Commit {
    fn from(snapshot: Snapshot) -> Self {
        Commit::new(snapshot.vid, snapshot.lsn, snapshot.page_count)
    }
}
