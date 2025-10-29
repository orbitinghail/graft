use std::ops::RangeInclusive;

use bilrost::Message;

use graft_core::{lsn::LSN, volume_ref::VolumeRef};

use crate::snapshot::Snapshot;

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

    /// Returns the range of LSNs that represent local changes since the sync point.
    pub fn local_changes(&self, snapshot: &Snapshot) -> Option<RangeInclusive<LSN>> {
        changes("local", &self.local, snapshot)
    }

    /// Returns the range of LSNs that represent remote changes since the sync point.
    pub fn remote_changes(&self, snapshot: &Snapshot) -> Option<RangeInclusive<LSN>> {
        changes("remote", &self.remote, snapshot)
    }
}

fn changes(name: &str, base: &VolumeRef, snapshot: &Snapshot) -> Option<RangeInclusive<LSN>> {
    assert_eq!(
        base.vid(),
        snapshot.vid(),
        "BUG: snapshot is not from {name} volume"
    );
    // As SyncPoints represent a link between LSNs in two different volumes,
    // they also imply a causal relationship between the SyncPoint existing and
    // Snapshots always being non-empty for the SyncPoint Volumes.
    let latest = snapshot.lsn().expect("BUG: monotonicity violation");
    (base.lsn() < latest).then(|| base.lsn()..=latest)
}
