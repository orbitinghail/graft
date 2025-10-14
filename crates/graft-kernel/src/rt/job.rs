use culprit::{Culprit, ResultExt};
use graft_core::{VolumeId, lsn::LSN};
use tryiter::TryIteratorExt;

use crate::{
    local::fjall_storage::FjallStorage, remote::Remote, rt::err::RuntimeErr,
    volume_name::VolumeName,
};

pub enum Job {
    /// Pulls commits and metadata from a remote.
    PullVolume {
        /// The Volume to fetch.
        vid: VolumeId,

        /// An optional maximum LSN to fetch.
        max_lsn: Option<LSN>,
    },

    /// Commits a Named Volume's local changes into its remote.
    RemoteCommit { name: VolumeName },

    /// Fast-forwards the local volume to include any remote commits. Fails if
    /// the local volume has unpushed commits, unless `force` is specified.
    SyncRemoteToLocal {
        /// Name of the volume to sync.
        name: VolumeName,

        /// If true, discards any unpushed local commits before syncing.
        force: bool,
    },
}

impl Job {
    /// Inspects all named volumes to compute a list of outstanding jobs
    pub fn collect(storage: &FjallStorage) -> Result<Vec<Self>, Culprit<RuntimeErr>> {
        let mut jobs = vec![];
        let reader = storage.read();
        let mut volumes = reader.named_volumes();
        while let Some(volume) = volumes.try_next().or_into_ctx()? {
            if volume.pending_commit().is_some() {
                jobs.push(Job::RemoteCommit { name: volume.name().clone() })
            } else {
                let local_ref = volume.local();
                let local_snapshot = reader.snapshot(local_ref.vid()).or_into_ctx()?;
                let local_changes = local_snapshot.lsn() != Some(local_ref.lsn());

                let remote_ref = volume.remote();
                let remote_changes = if let Some(remote_ref) = remote_ref {
                    let remote_snapshot = reader.snapshot(remote_ref.vid()).or_into_ctx()?;
                    remote_snapshot.lsn() != Some(remote_ref.lsn())
                } else {
                    false
                };

                if remote_changes && local_changes {
                    todo!("user needs to intervene")
                } else if remote_changes {
                    todo!("SyncRemoteToLocal")
                } else if local_changes {
                    todo!("RemoteCommit")
                } else {
                    todo!("PullVolume + SyncRemoteToLocal")
                }
            }
        }
        Ok(jobs)
    }

    pub async fn run(
        self,
        storage: &FjallStorage,
        remote: &Remote,
    ) -> culprit::Result<(), RuntimeErr> {
        todo!()
    }
}
