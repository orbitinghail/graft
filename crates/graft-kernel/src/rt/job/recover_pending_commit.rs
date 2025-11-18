use std::fmt::Debug;

use culprit::ResultExt;
use graft_core::VolumeId;

use crate::{KernelErr, local::fjall_storage::FjallStorage, remote::Remote};

/// Resumes from an interrupted `Job::RemoteCommit`. This job should be
/// triggered when a `Graft` has a `pending_commit` and no `RemoteCommit`
/// operation is in progress.
pub struct Opts {
    pub graft: VolumeId,
}

impl Debug for Opts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoverPendingCommit")
            .field("graft", &self.graft)
            .finish()
    }
}

pub async fn run(
    storage: &FjallStorage,
    remote: &Remote,
    opts: Opts,
) -> culprit::Result<(), KernelErr> {
    // the graft must have a pending commit
    let reader = storage.read();
    let graft = reader.graft(&opts.graft).or_into_ctx()?;
    let Some(pending_commit) = graft.pending_commit() else {
        // nothing to recover
        return Ok(());
    };

    // to recover, we need to determine whether or not the pending commit made
    // it to the server. thus, there are three outcomes to this job:
    // 1. the commit made it (commit hash equal)
    // 2. the commit did not make it (commit hash not equal, or commit missing)
    // 3. an error occurs (retry later)

    let remote_commit = remote
        .get_commit(&graft.remote, pending_commit.commit)
        .await
        .or_into_ctx()?;

    match remote_commit {
        Some(commit) if commit.commit_hash() == Some(&pending_commit.commit_hash) => {
            // the commit made it! finish up the sync process
            storage
                .remote_commit_success(&graft.local, commit)
                .or_into_ctx()?;
        }
        Some(_) | None => {
            // the commit didn't make it, clear the pending commit.
            // the fetch_volume/sync_remote_to_local jobs will handle the new commit
            storage.drop_pending_commit(&graft.local).or_into_ctx()?;
        }
    }
    Ok(())
}
