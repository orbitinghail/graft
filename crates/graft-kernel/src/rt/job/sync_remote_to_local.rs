use std::fmt::Debug;

use culprit::ResultExt;
use graft_core::lsn::LSNRangeExt;
use tryiter::TryIteratorExt;

use crate::{
    local::fjall_storage::FjallStorage, rt::err::RuntimeErr, search_path::SearchPath,
    volume_err::VolumeErr, volume_name::VolumeName,
};

/// Fast-forwards the local volume to include any remote commits. Fails if
/// the local volume has unpushed commits.
pub struct Opts {
    /// Name of the volume to sync.
    pub name: VolumeName,
}

impl Debug for Opts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncRemoteToLocal")
            .field("name", &self.name.to_string())
            .finish()
    }
}

pub async fn run(storage: &FjallStorage, opts: Opts) -> culprit::Result<(), RuntimeErr> {
    // check that the named volume has a sync point
    let reader = storage.read();
    let Some(handle) = reader.named_volume(&opts.name).or_into_ctx()? else {
        return Err(VolumeErr::NamedVolumeNotFound(opts.name).into());
    };
    let Some(sync) = handle.sync() else {
        // nothing to sync
        return Ok(());
    };

    // check to see if we have any changes to sync
    let latest_remote = reader.snapshot(sync.remote().vid()).or_into_ctx()?;
    let Some(remote_changes) = sync.remote_changes(&latest_remote) else {
        // nothing to sync
        return Ok(());
    };

    // check that divergence hasn't happened
    let latest_local = reader.snapshot(sync.local().vid()).or_into_ctx()?;
    if sync.local_changes(&latest_local).is_some() {
        // the remote and local volumes have diverged
        let status = handle.sync_status(&latest_local, Some(&latest_remote));
        return Err(VolumeErr::NamedVolumeDiverged(opts.name, status).into());
    }

    tracing::debug!(
        local = ?latest_local.vid(),
        remote = ?latest_remote.vid(),
        lsns = %remote_changes.to_string(),
        "syncing commits from remote to local volume"
    );

    // iterate missing remote commits, and commit them to the local volume
    let mut latest_local_lsn = latest_local.lsn().expect("BUG: monotonicity violation");
    let search = SearchPath::new(sync.remote().vid().clone(), remote_changes);
    let mut batch = storage.batch();
    let mut commits = reader.commits(&search);
    while let Some(commit) = commits.try_next().or_into_ctx()? {
        // advance LSN
        latest_local_lsn = latest_local_lsn.next().expect("BUG: LSN overflow");
        // map the remote commit into the local volume
        batch.write_commit(
            commit
                .with_vid(latest_local.vid().clone())
                .with_lsn(latest_local_lsn),
        );
    }

    // finalize the batch, ensuring that neither volume changed in the process
    storage
        .batch_commit_precondition(batch, |reader| {
            Ok(reader.snapshot(latest_local.vid())? == latest_local
                && reader.snapshot(latest_remote.vid())? == latest_remote)
        })
        .or_into_ctx()
}
