use culprit::{Culprit, ResultExt};
use fjall::Slice;
use graft_core::{lsn::LSN, VolumeId};
use zerocopy::{BigEndian, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned, U64};

use super::StorageErr;

#[derive(Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone, Unaligned)]
#[repr(C)]
pub struct CommitKey {
    vid: VolumeId,
    lsn: U64<BigEndian>,
}

impl CommitKey {
    #[inline]
    pub fn new(vid: VolumeId, lsn: LSN) -> Self {
        Self { vid, lsn: lsn.into() }
    }

    #[track_caller]
    pub(crate) fn ref_from_bytes(bytes: &[u8]) -> Result<&Self, Culprit<StorageErr>> {
        Ok(Self::try_ref_from_bytes(&bytes).or_ctx(|e| StorageErr::CorruptKey(e.into()))?)
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    pub fn lsn(&self) -> LSN {
        self.lsn.try_into().expect("invalid LSN")
    }

    #[inline]
    pub fn with_lsn(self, lsn: LSN) -> Self {
        Self { lsn: lsn.into(), ..self }
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
