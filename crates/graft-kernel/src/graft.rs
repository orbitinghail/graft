use std::{fmt::Display, ops::RangeInclusive};

use bilrost::Message;

use graft_core::{LogId, commit_hash::CommitHash, lsn::LSN};

#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct SyncPoint {
    /// This Graft is attached to the remote Log at this LSN
    #[bilrost(1)]
    pub remote: LSN,

    /// All commits up to this watermark in the local Log have been written
    /// to the remote.
    #[bilrost(2)]
    pub local_watermark: Option<LSN>,
}

#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct PendingCommit {
    /// The LSN we are syncing from the local Log
    #[bilrost(1)]
    pub local: LSN,

    /// The LSN we are creating in the remote Log
    #[bilrost(2)]
    pub commit: LSN,

    /// The pending remote commit hash. This is used to determine whether or not
    /// the commit has landed in the remote, in the case that we are interrupted
    /// while attempting to push.
    #[bilrost(3)]
    pub commit_hash: CommitHash,
}

impl From<PendingCommit> for SyncPoint {
    fn from(value: PendingCommit) -> Self {
        Self {
            remote: value.commit,
            local_watermark: Some(value.local),
        }
    }
}

#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct Graft {
    /// The local Log backing this Graft
    #[bilrost(1)]
    pub local: LogId,

    /// The remote Log backing this Graft.
    #[bilrost(2)]
    pub remote: LogId,

    /// Metadata keeping track of which portion of the local and remote log
    /// this Graft cares about.
    #[bilrost(3)]
    pub sync: Option<SyncPoint>,

    /// Presence of the `pending_commit` field means that the Push operation is in
    /// the process of committing to the remote. If no such Push job is currently
    /// running (i.e. it was interrupted), this field must be used to resume or
    /// abort the commit process.
    #[bilrost(4)]
    pub pending_commit: Option<PendingCommit>,
}

impl Graft {
    pub fn new(
        local: LogId,
        remote: LogId,
        sync: Option<SyncPoint>,
        pending_commit: Option<PendingCommit>,
    ) -> Self {
        Self { local, remote, sync, pending_commit }
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

    pub fn local_watermark(&self) -> Option<LSN> {
        self.sync().and_then(|s| s.local_watermark)
    }

    pub fn remote_commit(&self) -> Option<LSN> {
        self.sync().map(|s| s.remote)
    }

    pub fn local_changes(&self, head: Option<LSN>) -> Option<RangeInclusive<LSN>> {
        AheadStatus { head, base: self.local_watermark() }.changes()
    }

    pub fn remote_changes(&self, head: Option<LSN>) -> Option<RangeInclusive<LSN>> {
        AheadStatus {
            head,
            base: self.sync().map(|s| s.remote),
        }
        .changes()
    }

    pub fn status(&self, latest_local: Option<LSN>, latest_remote: Option<LSN>) -> GraftStatus {
        GraftStatus {
            local: self.local.clone(),
            local_status: AheadStatus {
                head: latest_local,
                base: self.local_watermark(),
            },
            remote: self.remote.clone(),
            remote_status: AheadStatus {
                head: latest_remote,
                base: self.sync().map(|s| s.remote),
            },
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct AheadStatus {
    pub head: Option<LSN>,
    pub base: Option<LSN>,
}

impl AheadStatus {
    pub fn new(head: Option<LSN>, base: Option<LSN>) -> Self {
        Self { head, base }
    }

    pub fn changes(&self) -> Option<RangeInclusive<LSN>> {
        match (self.base, self.head) {
            (None, None) => None,
            (None, Some(head)) => Some(LSN::FIRST..=head),
            (Some(base), Some(head)) => (base < head).then(|| base.next()..=head),

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
pub struct GraftStatus {
    pub local: LogId,
    pub local_status: AheadStatus,
    pub remote: LogId,
    pub remote_status: AheadStatus,
}

/// Output a human readable concise description of the status of this volume.
///
/// # Output examples:
///  - `_ r_`: empty volume
///  - `123 r_`: never synced
///  - `123 r130`: remote and local in sync
///  - `_ r130+7`: remote is 7 commits ahead, local is empty
///  - `123+3 r130`: local is 3 commits ahead
///  - `123 r130+3`: remote is 3 commits ahead
///  - `123+2 r130+3`: local and remote have diverged
impl Display for GraftStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} r{}", self.local_status, self.remote_status)
    }
}

impl GraftStatus {
    pub fn has_diverged(&self) -> bool {
        self.local_status.changes().is_some() && self.remote_status.changes().is_some()
    }
}
