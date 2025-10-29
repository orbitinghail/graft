use std::fmt::Display;

use bilrost::Message;
use culprit::Culprit;

use crate::{
    local::fjall_storage::FjallStorageErr, rt::runtime_handle::RuntimeHandle, snapshot::Snapshot,
    volume_name::VolumeName, volume_reader::VolumeReader, volume_writer::VolumeWriter,
};
use graft_core::{VolumeId, commit_hash::CommitHash, lsn::LSN, volume_ref::VolumeRef};

/// A `SyncPoint` is a pair of commits which represent the same logical Volume
/// state. The commits are tracked via two `VolumeRefs`, one for the local
/// volume, and one for the remote.
#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct SyncPoint {
    /// The local Volume reference
    #[bilrost(1)]
    local: VolumeRef,

    /// The remote Volume reference
    #[bilrost(2)]
    remote: VolumeRef,
}

impl SyncPoint {
    pub fn new(local: VolumeRef, remote: VolumeRef) -> Self {
        Self { local, remote }
    }

    pub fn local(&self) -> &VolumeRef {
        &self.local
    }

    pub fn remote(&self) -> &VolumeRef {
        &self.remote
    }
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

impl PendingCommit {
    pub fn new(remote_lsn: LSN, commit_hash: CommitHash) -> Self {
        Self { remote_lsn, commit_hash }
    }
}

#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct NamedVolumeState {
    /// The Volume name
    #[bilrost(1)]
    name: VolumeName,

    /// The local Volume backing this Named Volume
    #[bilrost(2)]
    local: VolumeId,

    /// The most recent successful sync point for this Named Volume
    #[bilrost(3)]
    sync: Option<SyncPoint>,

    /// Presence of the `pending_commit` field means that the Push operation is in
    /// the process of committing to the remote. If no such Push job is currently
    /// running (i.e. it was interrupted), this field must be used to resume or
    /// abort the commit process.
    #[bilrost(4)]
    pending_commit: Option<PendingCommit>,
}

impl NamedVolumeState {
    pub fn new(
        name: VolumeName,
        local: VolumeId,
        sync: Option<SyncPoint>,
        pending_commit: Option<PendingCommit>,
    ) -> Self {
        Self { name, local, sync, pending_commit }
    }

    pub fn name(&self) -> &VolumeName {
        &self.name
    }

    pub fn local(&self) -> &VolumeId {
        &self.local
    }

    pub fn with_sync(self, sync: Option<SyncPoint>) -> Self {
        Self { sync, ..self }
    }

    pub fn sync(&self) -> Option<&SyncPoint> {
        self.sync.as_ref()
    }

    pub fn with_pending_commit(self, pending_commit: Option<PendingCommit>) -> Self {
        Self { pending_commit, ..self }
    }

    pub fn pending_commit(&self) -> Option<&PendingCommit> {
        self.pending_commit.as_ref()
    }

    /// Given the latest local and remote snapshot, format a human readable
    /// concise description of the status of this named volume.
    ///
    /// # Output examples:
    ///  - `_`: empty volume
    ///  - `123`: never synced
    ///  - `123 r130`: remote and local in sync
    ///  - `123+3 r130`: local is 3 commits ahead
    ///  - `123 r130+3`: remote is 3 commits ahead
    ///  - `123+2 r130+3`: local and remote have diverged
    pub fn sync_status(&self, latest_local: &Snapshot, latest_remote: Option<&Snapshot>) -> String {
        assert_eq!(
            &self.local,
            latest_local.vid(),
            "BUG: local snapshot out of sync"
        );

        if let Some(sync) = self.sync() {
            let latest_local_lsn = latest_local
                .lsn()
                .expect("BUG: local snapshot behind sync point");
            let local_status = AheadStatus::new(latest_local_lsn, sync.local.lsn());

            let latest_remote = latest_remote.expect("BUG: remote snapshot missing");
            assert_eq!(
                sync.remote.vid(),
                latest_remote.vid(),
                "BUG: remote snapshot out of sync"
            );
            let latest_remote_lsn = latest_remote
                .lsn()
                .expect("BUG: remote snapshot behind sync point");
            let remote_status = AheadStatus::new(latest_remote_lsn, sync.remote.lsn());

            format!("{} {}", local_status, remote_status)
        } else {
            latest_local
                .lsn()
                .map_or(String::from("_"), |lsn| lsn.to_string())
        }
    }
}

pub struct NamedVolume {
    runtime: RuntimeHandle,
    name: VolumeName,
}

impl NamedVolume {
    pub(crate) fn new(runtime: RuntimeHandle, name: VolumeName) -> Self {
        Self { runtime, name }
    }

    pub fn reader(&self) -> Result<VolumeReader, Culprit<FjallStorageErr>> {
        let snapshot = self
            .runtime
            .storage()
            .read()
            .named_local_snapshot(&self.name)?
            .expect("BUG: NamedVolume missing local snapshot");
        Ok(VolumeReader::new(
            self.name.clone(),
            self.runtime.clone(),
            snapshot,
        ))
    }

    pub fn writer(&self) -> Result<VolumeWriter, Culprit<FjallStorageErr>> {
        let read = self.runtime.storage().read();
        let snapshot = read
            .named_local_snapshot(&self.name)?
            .expect("BUG: NamedVolume missing local snapshot");
        let page_count = read.page_count(&snapshot)?;
        Ok(VolumeWriter::new(
            self.name.clone(),
            self.runtime.clone(),
            snapshot,
            page_count,
        ))
    }
}

struct AheadStatus {
    head: LSN,
    base: LSN,
}

impl AheadStatus {
    fn new(head: LSN, base: LSN) -> Self {
        Self { head, base }
    }

    fn ahead(&self) -> u64 {
        self.head
            .since(self.base)
            .expect("BUG: monotonicity violation")
    }
}

impl Display for AheadStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ahead = self.ahead();
        if ahead > 0 {
            write!(f, "{}+{}", self.head, ahead)
        } else {
            write!(f, "{}", self.head)
        }
    }
}
