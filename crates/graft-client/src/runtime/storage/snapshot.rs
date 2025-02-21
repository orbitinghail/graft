use std::fmt::{Debug, Display};

use culprit::{Culprit, ResultExt};
use fjall::Slice;
use graft_core::{
    lsn::{MaybeLSN, LSN},
    page_count::PageCount,
};
use serde::Serialize;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

use super::{volume_state::VolumeStateTag, StorageErr};

#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone, PartialEq, Eq, Serialize)]
#[repr(C)]
pub struct Snapshot {
    local: LSN,
    remote: MaybeLSN,
    pages: PageCount,
    _padding: [u8; 4],
}

impl Snapshot {
    #[inline]
    pub fn new(local: LSN, remote: Option<LSN>, pages: PageCount) -> Self {
        Self {
            local,
            remote: remote.into(),
            pages,
            _padding: [0; 4],
        }
    }

    #[track_caller]
    pub(crate) fn try_from_bytes(bytes: &[u8]) -> Result<Self, Culprit<StorageErr>> {
        Self::try_read_from_bytes(bytes)
            .or_ctx(|e| StorageErr::CorruptVolumeState(VolumeStateTag::Snapshot, e.into()))
    }

    #[inline]
    pub fn local(&self) -> LSN {
        self.local
    }

    #[inline]
    pub fn remote(&self) -> Option<LSN> {
        self.remote.into()
    }

    #[inline]
    pub fn pages(&self) -> PageCount {
        self.pages
    }
}

impl Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Snapshot[{}/{};{}]",
            self.local(),
            match self.remote() {
                Some(lsn) => lsn.to_string(),
                None => "_".to_string(),
            },
            self.pages()
        )
    }
}

impl From<Snapshot> for Slice {
    fn from(snapshot: Snapshot) -> Slice {
        snapshot.as_bytes().into()
    }
}
