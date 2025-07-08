use bilrost::Message;

use crate::{VolumeId, lsn::LSN};

/// A reference to a Volume at a particular LSN.
#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct VolumeRef {
    /// The referenced Volume ID
    #[bilrost(1)]
    vid: VolumeId,

    /// The referenced LSN.
    #[bilrost(2)]
    lsn: LSN,
}

impl VolumeRef {
    pub fn new(vid: VolumeId, lsn: LSN) -> Self {
        Self { vid, lsn }
    }

    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    pub fn lsn(&self) -> LSN {
        self.lsn
    }
}
