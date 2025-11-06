use std::fmt::Debug;

use culprit::{Culprit, ResultExt};
use graft_core::{SegmentId, VolumeId, commit::SegmentRangeRef, lsn::LSN};
use tryiter::TryIteratorExt;

use crate::{
    KernelErr, local::fjall_storage::FjallStorage, remote::Remote, volume_name::VolumeName,
};

mod fetch_segment;
mod hydrate_volume;
mod pull_volume;
mod recover_pending_commit;
mod remote_commit;
mod sync_remote_to_local;

pub enum Job {
    /// Pulls commits and metadata from a remote.
    PullVolume(pull_volume::Opts),

    /// Commits a Named Volume's local changes into its remote.
    RemoteCommit(remote_commit::Opts),

    /// Commits a Named Volume's local changes into its remote.
    RecoverPendingCommit(recover_pending_commit::Opts),

    /// Fast-forwards the local volume to include any remote commits. Fails if
    /// the local volume has unpushed commits, unless `force` is specified.
    SyncRemoteToLocal(sync_remote_to_local::Opts),

    /// Downloads all missing pages for a Volume up to an optional maximum LSN.
    HydrateVolume(hydrate_volume::Opts),

    /// Fetches one or more Segment frames and loads the pages into Storage.
    FetchSegment(fetch_segment::Opts),
}

impl Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PullVolume(opts) => opts.fmt(f),
            Self::RemoteCommit(opts) => opts.fmt(f),
            Self::RecoverPendingCommit(opts) => opts.fmt(f),
            Self::SyncRemoteToLocal(opts) => opts.fmt(f),
            Self::HydrateVolume(opts) => opts.fmt(f),
            Self::FetchSegment(opts) => opts.fmt(f),
        }
    }
}

impl Job {
    pub fn pull_volume(vid: VolumeId, max_lsn: Option<LSN>) -> Self {
        Job::PullVolume(pull_volume::Opts { vid, max_lsn })
    }

    pub fn remote_commit(name: VolumeName) -> Self {
        Job::RemoteCommit(remote_commit::Opts { name })
    }

    pub fn recover_pending_commit(name: VolumeName) -> Self {
        Job::RecoverPendingCommit(recover_pending_commit::Opts { name })
    }

    pub fn sync_remote_to_local(name: VolumeName) -> Self {
        Job::SyncRemoteToLocal(sync_remote_to_local::Opts { name })
    }

    pub fn hydrate_volume(vid: VolumeId, max_lsn: Option<LSN>) -> Self {
        Job::HydrateVolume(hydrate_volume::Opts { vid, max_lsn })
    }

    pub fn fetch_segment(sid: SegmentId, frame: SegmentRangeRef) -> Self {
        Job::FetchSegment(fetch_segment::Opts { sid, frame })
    }

    /// Inspects all named volumes to compute a list of outstanding jobs.
    pub fn collect(storage: &FjallStorage) -> Result<Vec<Self>, Culprit<KernelErr>> {
        let mut jobs = vec![];
        let reader = storage.read();
        let mut volumes = reader.named_volumes();
        while let Some(volume) = volumes.try_next().or_into_ctx()? {
            let name = volume.name.clone();

            if volume.pending_commit().is_some() {
                jobs.push(Self::recover_pending_commit(name));
            } else {
                let local_snapshot = reader.snapshot(&volume.local).or_into_ctx()?;
                let local_changes = volume.local_changes(&local_snapshot).is_some();
                let remote_snapshot = reader.snapshot(&volume.remote).or_into_ctx()?;
                let remote_changes = volume.remote_changes(&remote_snapshot).is_some();

                if remote_changes && local_changes {
                    todo!("user needs to intervene")
                } else if remote_changes {
                    jobs.push(Self::sync_remote_to_local(name))
                } else if local_changes {
                    jobs.push(Self::remote_commit(name));
                } else {
                    jobs.push(Self::pull_volume(volume.remote.clone(), None));
                    jobs.push(Self::sync_remote_to_local(name))
                }
            }
        }
        Ok(jobs)
    }

    /// Run this job, potentially returning a job to run next
    #[tracing::instrument("Job::run", level = "debug", skip(storage, remote))]
    pub async fn run(
        self,
        storage: &FjallStorage,
        remote: &Remote,
    ) -> culprit::Result<(), KernelErr> {
        match self {
            Job::PullVolume(opts) => pull_volume::run(storage, remote, opts).await,
            Job::RemoteCommit(opts) => remote_commit::run(storage, remote, opts).await,
            Job::RecoverPendingCommit(opts) => {
                recover_pending_commit::run(storage, remote, opts).await
            }
            Job::SyncRemoteToLocal(opts) => sync_remote_to_local::run(storage, opts).await,
            Job::HydrateVolume(opts) => hydrate_volume::run(storage, remote, opts).await,
            Job::FetchSegment(opts) => fetch_segment::run(storage, remote, opts).await,
        }
    }
}
