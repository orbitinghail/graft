use std::fmt::Display;

use bilrost::Message;
use culprit::Culprit;

use crate::{
    local::fjall_storage::FjallStorageErr, rt::runtime_handle::RuntimeHandle, snapshot::Snapshot,
    volume_name::VolumeName, volume_reader::VolumeReader, volume_writer::VolumeWriter,
};
use graft_core::{commit_hash::CommitHash, lsn::LSN, volume_ref::VolumeRef};

#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct NamedVolumeState {
    /// The Volume name
    #[bilrost(1)]
    name: VolumeName,

    /// Reference to the latest synchronization point for the local Volume.
    #[bilrost(2)]
    local: VolumeRef,

    /// Reference to the latest synchronization point for the remote Volume.
    #[bilrost(3)]
    remote: Option<VolumeRef>,

    /// Presence of the `pending_commit` field means that the Push operation is in
    /// the process of committing to the remote. If no such Push job is currently
    /// running (i.e. it was interrupted), this field must be used to resume or
    /// abort the commit process.
    #[bilrost(4)]
    pending_commit: Option<PendingCommit>,
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

impl NamedVolumeState {
    pub fn new(
        name: VolumeName,
        local: VolumeRef,
        remote: Option<VolumeRef>,
        pending_commit: Option<PendingCommit>,
    ) -> Self {
        Self { name, local, remote, pending_commit }
    }

    pub fn name(&self) -> &VolumeName {
        &self.name
    }

    pub fn local(&self) -> &VolumeRef {
        &self.local
    }

    pub fn remote(&self) -> Option<&VolumeRef> {
        self.remote.as_ref()
    }

    pub fn pending_commit(&self) -> Option<&PendingCommit> {
        self.pending_commit.as_ref()
    }

    /// Given the latest local and remote snapshot, format a human readable
    /// concise description of the status of this named volume.
    ///
    /// This function can return the following strings:
    ///  - `123` -> no remote, no changes, base LSN is 123
    ///  - `123+5` -> no remote, 5 local changes, base LSN is 123
    ///  - `123/456` -> remote base 456, no changes, base LSN is 123
    ///  - `123+5/456` -> remote base 456, 5 local changes, base LSN is 123
    ///  - `123/456+5` -> remote base 456, 5 remote changes, base LSN is 123
    ///  - `123+3/456+5` -> DIVERGED: remote base 456, 3 local changes, 5 remote
    ///    changes, base LSN is 123
    pub fn sync_status(&self, latest_local: &Snapshot, latest_remote: Option<&Snapshot>) -> String {
        // local invariants:
        // as self.local is always set, and resets are atomic, local_snapshot
        // must align and be equal or later to self.local
        assert_eq!(
            self.local.vid(),
            latest_local.vid(),
            "BUG: local snapshot out of sync"
        );
        let latest_local_lsn = latest_local.lsn().expect("BUG: local snapshot out of sync");
        assert!(
            latest_local_lsn >= self.local.lsn(),
            "BUG: monotonicity violation"
        );
        let local_status = AheadStatus::new(latest_local_lsn, self.local.lsn());

        // remote invariants:
        // if self.remote is set, then the same local invariants apply between
        // self.remote and latest_remote
        let remote_status = if let Some(remote) = self.remote() {
            let latest_remote = latest_remote.expect("BUG: remote snapshot out of sync");
            assert_eq!(
                remote.vid(),
                latest_remote.vid(),
                "BUG: remote snapshot out of sync"
            );
            let latest_remote_lsn = latest_remote
                .lsn()
                .expect("BUG: remote snapshot out of sync");
            assert!(
                latest_remote_lsn >= remote.lsn(),
                "BUG: monotonicity violation"
            );
            Some(AheadStatus::new(latest_remote_lsn, remote.lsn()))
        } else {
            None
        };

        if let Some(remote_status) = remote_status {
            format!("{local_status}/{remote_status}")
        } else {
            local_status.to_string()
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
            write!(f, "{}+{}", self.base, ahead)
        } else {
            write!(f, "{}", self.base)
        }
    }
}
