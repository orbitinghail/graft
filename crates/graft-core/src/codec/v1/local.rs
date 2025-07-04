use bilrost::Message;
use bytes::Bytes;
use smallvec::SmallVec;

use crate::{
    VolumeId, codec::v1::remote::VolumeRef, commit_hash::CommitHash, handle_id::HandleId, lsn::LSN,
};

#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct LocalControl {
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

#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct VolumeHandle {
    /// The Handle ID
    #[bilrost(1)]
    id: HandleId,

    /// Reference to the latest synchronization point for the local Volume.
    #[bilrost(2)]
    local: VolumeRef,

    /// Reference to the latest synchronization point for the remote Volume.
    #[bilrost(3)]
    remote: Option<VolumeRef>,

    /// Presence of the `pending_commit` field means that the Push operation is in
    /// the process of committing to the remote. If no such Push job is currently
    /// running (i.e. it was interrupted), this field must be used to resume or
    /// abort the commit process.
    #[bilrost(4)]
    pending_commit: Option<PendingCommit>,
}

#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct PendingCommit {
    /// The resulting remote LSN that the push job is attempting to create.
    #[bilrost(1)]
    remote_lsn: LSN,

    /// The associated commit hash. This is used to determine whether or not the
    /// commit has landed in the remote, in the case that we are interrupted
    /// while attempting to push.
    #[bilrost(2)]
    commit_hash: CommitHash,
}
