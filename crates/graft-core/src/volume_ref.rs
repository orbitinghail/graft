use std::fmt::Display;

use bilrost::Message;

use crate::{VolumeId, lsn::LSN};

/// A reference to a Volume at a particular LSN.
#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct VolumeRef {
    /// The referenced Volume ID
    #[bilrost(1)]
    pub vid: VolumeId,

    /// The referenced LSN.
    #[bilrost(2)]
    pub lsn: LSN,
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

impl Display for VolumeRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.vid.short(), self.lsn)
    }
}
