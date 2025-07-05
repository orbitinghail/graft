use crate::{VolumeId, codec, lsn::LSN};

pub type VolumeRef = codec::v1::remote::VolumeRef;

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
