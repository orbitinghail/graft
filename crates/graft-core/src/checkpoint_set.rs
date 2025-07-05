use crate::{VolumeId, codec, lsn::LSN};

pub type CheckpointSet = codec::v1::remote::CheckpointSet;

impl CheckpointSet {
    pub fn new(vid: VolumeId, lsns: &[LSN]) -> Self {
        Self { vid, lsns: lsns.into() }
    }

    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    pub fn lsns(&self) -> &[LSN] {
        &self.lsns
    }
}
