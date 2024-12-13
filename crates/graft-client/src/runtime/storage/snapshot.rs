use std::fmt::Debug;

use graft_core::{lsn::LSN, page_count::PageCount, VolumeId};
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[derive(Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned, Clone, Copy)]
#[repr(u8)]
pub enum SnapshotKind {
    /// The volume's local snapshot
    Local = 1,

    /// The last local snapshot synced to the server
    Sync = 2,

    /// The latest remote snapshot
    Remote = 3,

    /// The latest remote checkpoint snapshot
    Checkpoint = 4,
}

#[derive(Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned, Clone)]
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
}

impl AsRef<[u8]> for SnapshotKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone)]
#[repr(C)]
pub struct Snapshot {
    lsn: LSN,
    page_count: PageCount,
    // Padding to align to 8 bytes
    _padding: [u8; 4],
}

impl Snapshot {
    #[inline]
    pub fn new(lsn: LSN, page_count: PageCount) -> Self {
        Self { lsn, page_count, _padding: [0; 4] }
    }

    #[inline]
    pub fn lsn(&self) -> LSN {
        self.lsn
    }

    #[inline]
    pub fn page_count(&self) -> PageCount {
        self.page_count
    }
}

impl Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot")
            .field("lsn", &self.lsn())
            .field("page_count", &self.page_count())
            .finish()
    }
}

impl From<Snapshot> for (LSN, PageCount) {
    fn from(snapshot: Snapshot) -> Self {
        (snapshot.lsn, snapshot.page_count)
    }
}

impl AsRef<[u8]> for Snapshot {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}
