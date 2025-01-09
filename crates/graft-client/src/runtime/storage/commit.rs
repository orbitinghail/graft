use fjall::Slice;
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

    #[inline]
    pub fn lsn(&self) -> LSN {
        self.lsn.into()
    }

    #[inline]
    pub fn set_lsn(&mut self, lsn: LSN) {
        self.lsn = lsn.into();
    }
}

impl AsRef<[u8]> for CommitKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Into<Slice> for CommitKey {
    fn into(self) -> Slice {
        self.as_bytes().into()
    }
}
