use graft_core::{lsn::LSN, VolumeId};
use zerocopy::{big_endian::U64, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[derive(Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone, Unaligned)]
#[repr(C)]
pub struct CommitKey {
    vid: VolumeId,
    lsn: U64,
}

impl CommitKey {
    #[inline]
    pub fn new(vid: VolumeId, lsn: LSN) -> Self {
        Self { vid, lsn: lsn.into() }
    }
}

impl AsRef<[u8]> for CommitKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}
