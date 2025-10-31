use std::{fmt::Display, ops::RangeInclusive};

use bilrost::Message;
use culprit::ResultExt;

use crate::{
    GraftErr, rt::runtime_handle::RuntimeHandle, snapshot::Snapshot, volume_name::VolumeName,
    volume_reader::VolumeReader, volume_writer::VolumeWriter,
};
use graft_core::{VolumeId, commit_hash::CommitHash, lsn::LSN};

type Result<T> = culprit::Result<T, GraftErr>;

#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct SyncPoint {
    /// The local LSN
    #[bilrost(1)]
    pub local: LSN,

    /// The remote LSN
    #[bilrost(2)]
    pub remote: LSN,
}

#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct PendingCommit {
    /// The LSN we are syncing from the local Volume
    #[bilrost(1)]
    pub local_lsn: LSN,

    /// The LSN we are creating in the remote Volume
    #[bilrost(2)]
    pub commit_lsn: LSN,

    /// The pending remote commit hash. This is used to determine whether or not
    /// the commit has landed in the remote, in the case that we are interrupted
    /// while attempting to push.
    #[bilrost(3)]
    pub commit_hash: CommitHash,
}

impl From<PendingCommit> for SyncPoint {
    fn from(value: PendingCommit) -> Self {
        Self {
            local: value.local_lsn,
            remote: value.commit_lsn,
        }
    }
}

#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct NamedVolumeState {
    /// The Volume name
    #[bilrost(1)]
    pub name: VolumeName,

    /// The local Volume backing this Named Volume
    #[bilrost(2)]
    pub local: VolumeId,

    /// The remote Volume backing this Named Volume.
    #[bilrost(3)]
    pub remote: VolumeId,

    /// The most recent successful sync point for this Named Volume
    #[bilrost(4)]
    pub sync: Option<SyncPoint>,

    /// Presence of the `pending_commit` field means that the Push operation is in
    /// the process of committing to the remote. If no such Push job is currently
    /// running (i.e. it was interrupted), this field must be used to resume or
    /// abort the commit process.
    #[bilrost(5)]
    pub pending_commit: Option<PendingCommit>,
}

impl NamedVolumeState {
    pub fn new(
        name: VolumeName,
        local: VolumeId,
        remote: VolumeId,
        sync: Option<SyncPoint>,
        pending_commit: Option<PendingCommit>,
    ) -> Self {
        Self {
            name,
            local,
            remote,
            sync,
            pending_commit,
        }
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

    pub fn local_changes(&self, snapshot: &Snapshot) -> Option<RangeInclusive<LSN>> {
        assert_eq!(&self.local, snapshot.vid());
        AheadStatus {
            head: snapshot.lsn(),
            base: self.sync().map(|s| s.local),
        }
        .changes()
    }

    pub fn remote_changes(&self, snapshot: &Snapshot) -> Option<RangeInclusive<LSN>> {
        assert_eq!(&self.remote, snapshot.vid());
        AheadStatus {
            head: snapshot.lsn(),
            base: self.sync().map(|s| s.remote),
        }
        .changes()
    }

    pub fn status(&self, latest_local: &Snapshot, latest_remote: &Snapshot) -> NamedVolumeStatus {
        assert_eq!(
            &self.local,
            latest_local.vid(),
            "BUG: local snapshot out of sync"
        );
        assert_eq!(
            &self.remote,
            latest_remote.vid(),
            "BUG: remote snapshot out of sync"
        );
        NamedVolumeStatus {
            local: latest_local.clone(),
            remote: latest_remote.clone(),
            sync: self.sync.clone(),
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

    pub fn status(&self) -> Result<NamedVolumeStatus> {
        let reader = self.runtime.storage().read();
        let state = reader
            .named_volume(&self.name)
            .or_into_ctx()?
            .expect("BUG: NamedVolume missing state");
        let latest_local = reader.snapshot(&state.local).or_into_ctx()?;
        let latest_remote = reader.snapshot(&state.remote).or_into_ctx()?;
        Ok(state.status(&latest_local, &latest_remote))
    }

    pub fn reader(&self) -> Result<VolumeReader> {
        let snapshot = self
            .runtime
            .storage()
            .read()
            .named_local_snapshot(&self.name)
            .or_into_ctx()?
            .expect("BUG: NamedVolume missing local snapshot");
        Ok(VolumeReader::new(
            self.name.clone(),
            self.runtime.clone(),
            snapshot,
        ))
    }

    pub fn writer(&self) -> Result<VolumeWriter> {
        let read = self.runtime.storage().read();
        let snapshot = read
            .named_local_snapshot(&self.name)
            .or_into_ctx()?
            .expect("BUG: NamedVolume missing local snapshot");
        let page_count = read.page_count(&snapshot).or_into_ctx()?;
        Ok(VolumeWriter::new(
            self.name.clone(),
            self.runtime.clone(),
            snapshot,
            page_count,
        ))
    }
}

struct AheadStatus {
    head: Option<LSN>,
    base: Option<LSN>,
}

impl AheadStatus {
    fn is_empty(&self) -> bool {
        self.head.is_none() && self.base.is_none()
    }

    fn changes(&self) -> Option<RangeInclusive<LSN>> {
        match (self.base, self.head) {
            (None, None) => None,
            (None, Some(head)) => Some(LSN::FIRST..=head),
            (Some(base), Some(head)) => (base < head).then(|| base..=head),

            (Some(_), None) => unreachable!("BUG: snapshot behind sync point"),
        }
    }
}

impl Display for AheadStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.base, self.head) {
            (Some(base), Some(head)) => {
                let ahead = head.since(base).expect("BUG: monotonicity violation");
                if ahead == 0 {
                    write!(f, "{head}")
                } else {
                    write!(f, "{head}+{ahead}")
                }
            }
            (None, Some(head)) => write!(f, "{head}"),
            (None, None) => write!(f, "_"),

            (Some(_), None) => unreachable!("BUG: snapshot behind sync point"),
        }
    }
}

#[derive(Debug)]
pub struct NamedVolumeStatus {
    pub local: Snapshot,
    pub remote: Snapshot,
    pub sync: Option<SyncPoint>,
}

impl NamedVolumeStatus {
    pub fn sync(&self) -> Option<&SyncPoint> {
        self.sync.as_ref()
    }
}

/// Output a human readable concise description of the status of this named
/// volume.
///
/// # Output examples:
///  - `_`: empty volume
///  - `123`: never synced
///  - `123 r130`: remote and local in sync
///  - `_ r130+130`: remote is 130 commits ahead, local is empty
///  - `123+3 r130`: local is 3 commits ahead
///  - `123 r130+3`: remote is 3 commits ahead
///  - `123+2 r130+3`: local and remote have diverged
impl Display for NamedVolumeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let local = AheadStatus {
            head: self.local.lsn(),
            base: self.sync().map(|s| s.local),
        };
        let remote = AheadStatus {
            head: self.remote.lsn(),
            base: self.sync().map(|s| s.remote),
        };
        if remote.is_empty() {
            write!(f, "{local}")
        } else {
            write!(f, "{local} r{remote}")
        }
    }
}
