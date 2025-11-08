use std::fmt::Debug;

use culprit::{Culprit, ResultExt};
use graft_core::{SegmentId, VolumeId, commit::SegmentRangeRef, lsn::LSN};
use tryiter::TryIteratorExt;

use crate::{KernelErr, local::fjall_storage::FjallStorage, remote::Remote};

mod fetch_segment;
mod fetch_volume;
mod hydrate_volume;
mod recover_pending_commit;
mod remote_commit;
mod sync_remote_to_local;

pub enum Job {
    /// Pulls commits and metadata from a remote.
    FetchVolume(fetch_volume::Opts),

    /// Commits a Graft's local changes into its remote.
    RemoteCommit(remote_commit::Opts),

    /// Commits a Graft's local changes into its remote.
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
            Self::FetchVolume(opts) => opts.fmt(f),
            Self::RemoteCommit(opts) => opts.fmt(f),
            Self::RecoverPendingCommit(opts) => opts.fmt(f),
            Self::SyncRemoteToLocal(opts) => opts.fmt(f),
            Self::HydrateVolume(opts) => opts.fmt(f),
            Self::FetchSegment(opts) => opts.fmt(f),
        }
    }
}

impl Job {
    pub fn fetch_volume(vid: VolumeId, max_lsn: Option<LSN>) -> Self {
        Job::FetchVolume(fetch_volume::Opts { vid, max_lsn })
    }

    pub fn remote_commit(graft: VolumeId) -> Self {
        Job::RemoteCommit(remote_commit::Opts { graft })
    }

    pub fn recover_pending_commit(graft: VolumeId) -> Self {
        Job::RecoverPendingCommit(recover_pending_commit::Opts { graft })
    }

    pub fn sync_remote_to_local(graft: VolumeId) -> Self {
        Job::SyncRemoteToLocal(sync_remote_to_local::Opts { graft })
    }

    pub fn hydrate_volume(vid: VolumeId, max_lsn: Option<LSN>) -> Self {
        Job::HydrateVolume(hydrate_volume::Opts { vid, max_lsn })
    }

    pub fn fetch_segment(sid: SegmentId, frame: SegmentRangeRef) -> Self {
        Job::FetchSegment(fetch_segment::Opts { sid, frame })
    }

    /// Inspects all grafts to compute a list of outstanding jobs.
    pub fn collect(storage: &FjallStorage) -> Result<Vec<Self>, Culprit<KernelErr>> {
        let mut jobs = vec![];
        let reader = storage.read();
        let mut grafts = reader.iter_grafts();
        while let Some(graft) = grafts.try_next().or_into_ctx()? {
            if graft.pending_commit().is_some() {
                jobs.push(Self::recover_pending_commit(graft.local));
            } else {
                let latest_local = reader.latest_lsn(&graft.local).or_into_ctx()?;
                let latest_remote = reader.latest_lsn(&graft.remote).or_into_ctx()?;
                let local_changes = graft.local_changes(latest_local).is_some();
                let remote_changes = graft.remote_changes(latest_remote).is_some();

                if remote_changes && local_changes {
                    todo!("user needs to intervene")
                } else if remote_changes {
                    jobs.push(Self::sync_remote_to_local(graft.local))
                } else if local_changes {
                    jobs.push(Self::remote_commit(graft.local));
                } else {
                    jobs.push(Self::fetch_volume(graft.remote, None));
                    jobs.push(Self::sync_remote_to_local(graft.local))
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
            Job::FetchVolume(opts) => fetch_volume::run(storage, remote, opts).await,
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
