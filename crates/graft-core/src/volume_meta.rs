use bilrost::Message;
use bytes::Bytes;
use smallvec::SmallVec;

use crate::{VolumeId, lsn::LSN, volume_ref::VolumeRef};

#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct VolumeMeta {
    /// The Volume's ID
    #[bilrost(1)]
    vid: VolumeId,

    /// The parent reference if this Volume is a fork.
    #[bilrost(2)]
    parent: Option<VolumeRef>,

    /// The etag from the last time we pulled the `CheckpointSet`, used to only pull
    /// changed `CheckpointSets`
    #[bilrost(3)]
    etag: Bytes,

    /// The set of checkpoint LSNs.
    #[bilrost(4)]
    lsns: SmallVec<[LSN; 2]>,
}
