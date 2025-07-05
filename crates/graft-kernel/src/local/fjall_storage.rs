use graft_core::{
    VolumeId, commit::Commit, handle_id::HandleId, lsn::LSN, page::Page,
    volume_handle::VolumeHandle, volume_meta::VolumeMeta, volume_ref::VolumeRef,
};

use crate::local::fjall_storage::{
    keys::{CommitKey, PageKey},
    typed_partition::TypedPartition,
};

mod fjall_repr;
mod keys;
mod typed_partition;
mod values;

pub struct FjallStorage {
    keyspace: fjall::Keyspace,

    /// This partition maps `VolumeHandle` IDs to `VolumeHandles`
    /// {`HandleId`} -> `VolumeHandle`
    /// Keyed by `keys::HandleKey`
    handles: TypedPartition<HandleId, VolumeHandle>,

    /// This partition stores metadata about each Volume
    /// {vid} -> VolumeMeta
    /// Keyed by `keys::VolumeKey`
    volumes: TypedPartition<VolumeId, VolumeMeta>,

    /// This partition stores commits
    /// {vid} / {lsn} -> Commit
    /// Keyed by `keys::CommitKey`
    log: TypedPartition<CommitKey, Commit>,

    /// This partition stores Pages
    /// {sid} / {pageidx} -> Page
    /// Keyed by `keys::PageKey`
    pages: TypedPartition<PageKey, Page>,
}

fn foo() {
    let vr = VolumeRef::new(VolumeId::random(), LSN::FIRST);
    vr.lsn;
}
