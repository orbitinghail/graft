use std::fmt::Debug;

use graft_core::{lsn::LSN, page_count::PageCount, VolumeId};
use zerocopy::{
    little_endian::{U32, U64},
    Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned,
};

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

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    pub fn kind(&self) -> SnapshotKind {
        self.kind
    }
}

impl AsRef<[u8]> for SnapshotKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned)]
#[repr(C)]
pub struct Snapshot {
    lsn: U64,
    page_count: U32,
}

impl Snapshot {
    #[inline]
    pub fn new(lsn: LSN, page_count: PageCount) -> Self {
        Self {
            lsn: lsn.into(),
            page_count: page_count.into(),
        }
    }

    #[inline]
    pub fn lsn(&self) -> LSN {
        self.lsn.into()
    }

    #[inline]
    pub fn page_count(&self) -> PageCount {
        self.page_count.into()
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
