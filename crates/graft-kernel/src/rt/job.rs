use culprit::{Culprit, ResultExt};
use graft_core::{VolumeId, lsn::LSN};
use tryiter::TryIteratorExt;

use crate::{
    local::fjall_storage::FjallStorage, remote::Remote, rt::err::RuntimeErr,
    volume_name::VolumeName,
};

mod pull_volume;
mod remote_commit;
mod resume_pending_commit;
mod sync_remote_to_local;

pub enum Job {
    /// Pulls commits and metadata from a remote.
    PullVolume(pull_volume::Opts),

    /// Commits a Named Volume's local changes into its remote.
    RemoteCommit(remote_commit::Opts),

    /// Commits a Named Volume's local changes into its remote.
    ResumePendingCommit(resume_pending_commit::Opts),

    /// Fast-forwards the local volume to include any remote commits. Fails if
    /// the local volume has unpushed commits, unless `force` is specified.
    SyncRemoteToLocal(sync_remote_to_local::Opts),
}

impl Job {
    pub fn pull_volume(vid: VolumeId, max_lsn: Option<LSN>) -> Self {
        Job::PullVolume(pull_volume::Opts { vid, max_lsn })
    }

    pub fn remote_commit(name: VolumeName) -> Self {
        Job::RemoteCommit(remote_commit::Opts { name })
    }

    pub fn resume_pending_commit(name: VolumeName) -> Self {
        Job::ResumePendingCommit(resume_pending_commit::Opts { name })
    }

    pub fn sync_remote_to_local(name: VolumeName, force: bool) -> Self {
        Job::SyncRemoteToLocal(sync_remote_to_local::Opts { name, force })
    }

    /// Inspects all named volumes to compute a list of outstanding jobs.
    pub fn collect(storage: &FjallStorage) -> Result<Vec<Self>, Culprit<RuntimeErr>> {
        let mut jobs = vec![];
        let reader = storage.read();
        let mut volumes = reader.named_volumes();
        while let Some(volume) = volumes.try_next().or_into_ctx()? {
            if volume.pending_commit().is_some() {
                jobs.push(Self::resume_pending_commit(volume.name().clone()));
            } else if let Some(remote_ref) = volume.remote() {
                let local_ref = volume.local();
                let local_snapshot = reader.snapshot(local_ref.vid()).or_into_ctx()?;
                let local_changes = local_snapshot.lsn() != Some(local_ref.lsn());

                let remote_snapshot = reader.snapshot(remote_ref.vid()).or_into_ctx()?;
                let remote_changes = remote_snapshot.lsn() != Some(remote_ref.lsn());

                if remote_changes && local_changes {
                    todo!("user needs to intervene")
                } else if remote_changes {
                    jobs.push(Self::sync_remote_to_local(volume.name().clone(), false))
                } else if local_changes {
                    jobs.push(Self::remote_commit(volume.name().clone()));
                } else {
                    jobs.push(Self::pull_volume(remote_ref.vid().clone(), None));
                    jobs.push(Self::sync_remote_to_local(volume.name().clone(), false))
                }
            }
        }
        Ok(jobs)
    }

    /// Run this job, potentially returning a job to run next
    pub async fn run(
        self,
        storage: &FjallStorage,
        remote: &Remote,
    ) -> culprit::Result<(), RuntimeErr> {
        match self {
            Job::PullVolume(opts) => pull_volume::run(storage, remote, opts).await,
            Job::RemoteCommit(opts) => remote_commit::run(storage, remote, opts).await,
            Job::ResumePendingCommit(opts) => {
                resume_pending_commit::run(storage, remote, opts).await
            }
            Job::SyncRemoteToLocal(opts) => sync_remote_to_local::run(storage, remote, opts).await,
        }
    }
}
