use std::ops::RangeInclusive;

use culprit::ResultExt;
use graft_core::{
    VolumeId,
    lsn::{LSN, LSNRangeExt},
    volume_ref::VolumeRef,
};

use crate::{
    local::fjall_storage::FjallStorage, remote::Remote, rt::err::RuntimeErr, snapshot::Snapshot,
    volume_name::VolumeName,
};

/// Commits a Named Volume's local changes into its remote.
///
/// This process involves the following stages:
///
/// 1. prepare commit
/// 2. push segment
/// 3. commit
/// 4. update named volume (on success or failure)
pub struct Opts {
    pub name: VolumeName,
}

pub async fn run(
    storage: &FjallStorage,
    remote: &Remote,
    opts: Opts,
) -> culprit::Result<(), RuntimeErr> {
    todo!()
}

struct CommitState {
    /// the latest local snapshot
    local: Snapshot,

    /// the local lsns to commit to the remote
    local_lsns: RangeInclusive<LSN>,

    /// the latest remote snapshot
    remote: Option<Snapshot>,

    /// the VolumeRef of the resulting commit should this process be successful
    commit_ref: VolumeRef,
}

async fn prepare_commit(
    storage: &FjallStorage,
    name: &VolumeName,
) -> culprit::Result<CommitState, RuntimeErr> {
    let reader = storage.read();
    let Some(handle) = reader.named_volume(name).or_into_ctx()? else {
        return Err(RuntimeErr::NamedVolumeNotFound(name.clone()).into());
    };
    if handle.pending_commit().is_some() {
        return Err(RuntimeErr::NamedVolumeNeedsRecovery(name.clone()).into());
    }

    // load the latest local and remote snapshots
    let latest_local = reader.snapshot(handle.local().vid()).or_into_ctx()?;
    let latest_remote = if let Some(remote) = handle.remote() {
        Some(reader.snapshot(remote.vid()).or_into_ctx()?)
    } else {
        None
    };

    // check to see if we have any local changes to push
    let Some(local_lsns) = latest_local
        .lsn()
        .map(|latest| handle.local().lsn()..latest)
        .filter(|r| !r.is_empty())
    else {
        // nothing to push
        let status = handle.sync_status(&latest_local, latest_remote.as_ref());
        return Err(RuntimeErr::NamedVolumeNoChanges(name.clone(), status).into());
    };
    let local_lsns = local_lsns.as_inclusive();

    // we can only commit remotely if the remote hasn't changed since the last
    // time we synced or the remote is empty
    if latest_remote.as_ref().and_then(|r| r.lsn()) != handle.remote().map(|r| r.lsn()) {
        // the remote and local volumes have diverged
        let status = handle.sync_status(&latest_local, latest_remote.as_ref());
        return Err(RuntimeErr::NamedVolumeDiverged(name.clone(), status).into());
    }

    let commit_ref = if let Some(latest_remote) = latest_remote.as_ref() {
        let commit_lsn = latest_remote
            .lsn()
            .map_or(LSN::FIRST, |lsn| lsn.saturating_next());
        VolumeRef::new(latest_remote.vid().clone(), commit_lsn)
    } else {
        // there is no remote!
        VolumeRef::new(VolumeId::random(), LSN::FIRST)
    };

    Ok(CommitState {
        local: latest_local,
        local_lsns,
        remote: latest_remote,
        commit_ref: commit_ref,
    })
}
