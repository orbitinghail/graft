use std::ops::Range;

use graft_core::{lsn::LSN, VolumeId};

pub trait Job {}

pub struct PullJob {
    vid: VolumeId,
}

pub struct PushJob {
    vid: VolumeId,
    lsns: Range<LSN>,
}
