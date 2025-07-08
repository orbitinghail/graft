use bilrost::Message;

use crate::{PageCount, VolumeId, lsn::LSN};

/// A Volume Snapshot.
#[derive(Debug, Clone, Message, PartialEq, Eq)]
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

impl Snapshot {}
