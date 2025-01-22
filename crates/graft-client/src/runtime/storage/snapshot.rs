use std::fmt::Debug;

use fjall::Slice;
use graft_core::{
    lsn::{MaybeLSN, LSN},
    page_count::PageCount,
    VolumeId,
};
use serde::{Deserialize, Serialize};
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[derive(
    Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned, Clone, Copy, PartialEq, Eq,
)]
#[repr(u8)]
pub enum SnapshotKind {
    /// The volume's local snapshot
    Local = 0b0001,

    /// The last local snapshot synced to the server
    Sync = 0b0010,

    /// The latest remote snapshot
    Remote = 0b0100,

    /// The latest remote checkpoint snapshot
    Checkpoint = 0b1000,
}

#[derive(Default, Clone, Copy)]
pub struct SnapshotKindMask(u8);

impl SnapshotKindMask {
    pub const ALL: SnapshotKindMask = SnapshotKindMask(!0);

    pub fn with(self, kind: SnapshotKind) -> Self {
        SnapshotKindMask(self.0 | kind as u8)
    }

    pub fn contains(&self, kind: SnapshotKind) -> bool {
        self.0 & kind as u8 != 0
    }
}

#[derive(
    Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned, Clone, PartialEq, Eq,
)]
#[repr(C)]
pub struct SnapshotKey {
    vid: VolumeId,
    kind: SnapshotKind,
}

impl SnapshotKey {
    #[inline]
    pub fn new(vid: VolumeId, kind: SnapshotKind) -> Self {
        Self { vid, kind }
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    pub fn kind(&self) -> SnapshotKind {
        self.kind
    }
}

impl Into<Slice> for SnapshotKey {
    fn into(self) -> Slice {
        self.as_bytes().into()
    }
}

impl AsRef<[u8]> for SnapshotKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[derive(
    KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone, PartialEq, Eq, Serialize, Deserialize,
)]
#[repr(C)]
pub struct Snapshots {
    local: LSN,
    remote: MaybeLSN,
    pages: PageCount,
    _padding: [u8; 4],
}

#[derive(
    KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone, PartialEq, Eq, Serialize, Deserialize,
)]
#[repr(C)]
pub struct Watermarks {
    pending_sync: LSN,
    last_sync: LSN,
    checkpoint: LSN,
}

#[derive(
    KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone, PartialEq, Eq, Serialize, Deserialize,
)]
#[repr(C)]
pub struct Snapshot {
    lsn: LSN,
    pages: PageCount,

    // Padding to align to 8 bytes
    #[serde(skip)]
    _padding: [u8; 4],
}

impl Snapshot {
    #[inline]
    pub fn new(lsn: LSN, pages: PageCount) -> Self {
        Self { lsn, pages, _padding: [0; 4] }
    }

    #[inline]
    pub fn lsn(&self) -> LSN {
        self.lsn
    }

    #[inline]
    pub fn pages(&self) -> PageCount {
        self.pages
    }
}

impl Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Snapshot")
            .field(&self.lsn())
            .field(&self.pages())
            .finish()
    }
}

impl AsRef<[u8]> for Snapshot {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Into<Slice> for Snapshot {
    fn into(self) -> Slice {
        self.as_bytes().into()
    }
}

impl From<graft_proto::Snapshot> for Snapshot {
    fn from(proto: graft_proto::Snapshot) -> Self {
        Self::new(proto.lsn().expect("invalid LSN"), proto.pages())
    }
}

#[derive(Default, Debug)]
pub struct SnapshotSet {
    local: Option<Snapshot>,
    sync: Option<Snapshot>,
    remote: Option<Snapshot>,
    checkpoint: Option<Snapshot>,
}

impl SnapshotSet {
    pub fn insert(&mut self, kind: SnapshotKind, snapshot: Snapshot) {
        match kind {
            SnapshotKind::Local => self.local = Some(snapshot),
            SnapshotKind::Sync => self.sync = Some(snapshot),
            SnapshotKind::Remote => self.remote = Some(snapshot),
            SnapshotKind::Checkpoint => self.checkpoint = Some(snapshot),
        }
    }

    pub fn take_local(&mut self) -> Option<Snapshot> {
        self.local.take()
    }

    pub fn local(&self) -> Option<&Snapshot> {
        self.local.as_ref()
    }

    pub fn take_sync(&mut self) -> Option<Snapshot> {
        self.sync.take()
    }

    pub fn sync(&self) -> Option<&Snapshot> {
        self.sync.as_ref()
    }

    pub fn take_remote(&mut self) -> Option<Snapshot> {
        self.remote.take()
    }

    pub fn remote(&self) -> Option<&Snapshot> {
        self.remote.as_ref()
    }

    pub fn take_checkpoint(&mut self) -> Option<Snapshot> {
        self.checkpoint.take()
    }

    pub fn checkpoint(&self) -> Option<&Snapshot> {
        self.checkpoint.as_ref()
    }
}
