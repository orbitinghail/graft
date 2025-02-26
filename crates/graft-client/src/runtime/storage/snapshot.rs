// seems like rust analyzer has a bug that causes this warning to spuriously
// fire on camel case types that also use underscores which is what zerocopy
// generates for enum struct variants
#![allow(non_camel_case_types)]

use std::fmt::{Debug, Display};

use culprit::{Culprit, ResultExt};
use fjall::Slice;
use graft_core::{lsn::LSN, page_count::PageCount};
use serde::Serialize;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes};

use super::{volume_state::VolumeStateTag, StorageErr};

/// `RemoteLSN` tracks the relationship between the a server LSN and the local LSN
/// it maps to.
#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone, PartialEq, Eq, Serialize)]
#[repr(u8)]
pub enum RemoteMapping {
    Unmapped {
        #[serde(skip)]
        _padding: [u8; 23],
    },
    Mapped {
        #[serde(skip)]
        _padding: [u8; 7],

        /// the server LSN
        remote: LSN,
        /// the local LSN that maps to the remote LSN
        local: LSN,
    },
}

impl RemoteMapping {
    #[inline]
    pub fn new(remote: LSN, local: LSN) -> Self {
        Self::Mapped { _padding: [0; 7], remote, local }
    }

    #[inline]
    pub fn lsn(&self) -> Option<LSN> {
        match self {
            Self::Mapped { remote, .. } => Some(*remote),
            Self::Unmapped { .. } => None,
        }
    }

    #[inline]
    pub fn local(&self) -> Option<LSN> {
        match self {
            Self::Mapped { local, .. } => Some(*local),
            Self::Unmapped { .. } => None,
        }
    }

    /// returns the remote -> local LSN mapping as a single option tuple
    #[inline]
    pub fn splat(&self) -> Option<(LSN, LSN)> {
        match self {
            Self::Mapped { remote, local, .. } => Some((*remote, *local)),
            Self::Unmapped { .. } => None,
        }
    }
}

impl Default for RemoteMapping {
    #[inline]
    fn default() -> Self {
        Self::Unmapped { _padding: [0; 23] }
    }
}

impl Debug for RemoteMapping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for RemoteMapping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RemoteMapping::Unmapped { _padding } => write!(f, "_"),
            RemoteMapping::Mapped { _padding, remote, local } => write!(f, "{remote}({local})"),
        }
    }
}

#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone, PartialEq, Eq, Serialize)]
#[repr(C)]
pub struct Snapshot {
    /// resolve page reads at this local LSN
    local: LSN,
    /// the last known server LSN along with it's local LSN
    remote: RemoteMapping,
    /// the logical number of pages in this snapshot
    pages: PageCount,

    #[serde(skip)]
    _padding: [u8; 4],
}

impl Snapshot {
    #[inline]
    pub fn new(local: LSN, remote: RemoteMapping, pages: PageCount) -> Self {
        Self { local, remote, pages, _padding: [0; 4] }
    }

    #[track_caller]
    pub(crate) fn try_from_bytes(bytes: &[u8]) -> Result<Self, Culprit<StorageErr>> {
        Self::try_read_from_bytes(bytes)
            .or_ctx(|e| StorageErr::CorruptVolumeState(VolumeStateTag::Snapshot, e.into()))
    }

    /// the local LSN backing this snapshot
    #[inline]
    pub fn local(&self) -> LSN {
        self.local
    }

    /// the last known remote LSN as of this snapshot
    #[inline]
    pub fn remote(&self) -> Option<LSN> {
        self.remote.lsn()
    }

    /// the local LSN corresponding to the last known remote LSN as of this snapshot
    #[inline]
    pub fn remote_local(&self) -> Option<LSN> {
        self.remote.local()
    }

    /// Returns this snapshot's remote LSN along with the
    /// local LSN the remote LSN corresponds to
    #[inline]
    pub fn remote_mapping(&self) -> &RemoteMapping {
        &self.remote
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
            self.remote,
            self.pages()
        )
    }
}

impl From<Snapshot> for Slice {
    fn from(snapshot: Snapshot) -> Slice {
        snapshot.as_bytes().into()
    }
}
