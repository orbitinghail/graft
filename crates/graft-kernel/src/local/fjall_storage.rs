use graft_core::{
    VolumeId,
    codec::{self, BilrostCodec, PageCodec},
    commit::Commit,
    handle_id::HandleId,
    volume_handle::VolumeHandle,
    volume_meta::VolumeMeta,
};

use crate::local::fjall_storage::{
    keys::{CommitKey, PageKey},
    typed_partition::TypedPartition,
};

pub mod keys;
mod typed_partition;

#[derive(Debug, thiserror::Error)]
pub enum FjallStorageErr {
    #[error("Fjall error: {0}")]
    FjallErr(#[from] fjall::Error),

    #[error("Fjall LSM Tree error: {0}")]
    LsmTreeErr(#[from] lsm_tree::Error),

    #[error("Codec error: {0}")]
    CodecDecodeErr(#[from] codec::DecodeErr),

    #[error("Key decode error: {0}")]
    KeyDecodeErr(#[from] keys::DecodeErr),
}

pub struct FjallStorage {
    keyspace: fjall::Keyspace,

    /// This partition maps `VolumeHandle` IDs to `VolumeHandles`
    /// {`HandleId`} -> `VolumeHandle`
    /// Keyed by `keys::HandleKey`
    handles: TypedPartition<HandleId, BilrostCodec<VolumeHandle>>,

    /// This partition stores metadata about each Volume
    /// {vid} -> VolumeMeta
    /// Keyed by `keys::VolumeKey`
    volumes: TypedPartition<VolumeId, BilrostCodec<VolumeMeta>>,

    /// This partition stores commits
    /// {vid} / {lsn} -> Commit
    /// Keyed by `keys::CommitKey`
    log: TypedPartition<CommitKey, BilrostCodec<Commit>>,

    /// This partition stores Pages
    /// {sid} / {pageidx} -> Page
    /// Keyed by `keys::PageKey`
    pages: TypedPartition<PageKey, PageCodec>,
}
