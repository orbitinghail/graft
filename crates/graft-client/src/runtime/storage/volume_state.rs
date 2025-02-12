use culprit::{Culprit, ResultExt};
use fjall::{KvPair, Slice};
use graft_core::{
    lsn::{MaybeLSN, LSN},
    VolumeId,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Debug, Display},
    iter::FusedIterator,
};
use tryiter::TryIteratorExt;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use super::{snapshot::Snapshot, StorageErr};

#[derive(
    Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned, Clone, Copy, PartialEq, Eq,
)]
#[repr(u8)]
pub enum VolumeStateTag {
    Config = 1,
    Status = 2,
    Snapshot = 3,
    Watermarks = 4,
}

#[derive(
    Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned, Clone, PartialEq, Eq,
)]
#[repr(C)]
pub struct VolumeStateKey {
    vid: VolumeId,
    tag: VolumeStateTag,
}

impl Into<Slice> for VolumeStateKey {
    fn into(self) -> Slice {
        self.as_bytes().into()
    }
}

impl AsRef<[u8]> for VolumeStateKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl VolumeStateKey {
    #[inline]
    pub fn new(vid: VolumeId, tag: VolumeStateTag) -> Self {
        Self { vid, tag }
    }

    pub(crate) fn ref_from_bytes(bytes: &[u8]) -> Result<&Self, Culprit<StorageErr>> {
        Ok(Self::try_ref_from_bytes(&bytes).or_ctx(|e| StorageErr::CorruptKey(e.into()))?)
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    pub fn tag(&self) -> VolumeStateTag {
        self.tag
    }

    #[inline]
    pub fn with_tag(self, tag: VolumeStateTag) -> Self {
        Self { tag, ..self }
    }
}

#[derive(
    Default,
    Debug,
    KnownLayout,
    Immutable,
    TryFromBytes,
    IntoBytes,
    Unaligned,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
)]
#[repr(u8)]
pub enum SyncDirection {
    #[default]
    Disabled = 0,
    Push = 1,
    Pull = 2,
    Both = 3,
}

impl SyncDirection {
    pub fn matches(self, other: SyncDirection) -> bool {
        match (self, other) {
            (SyncDirection::Disabled, SyncDirection::Disabled) => true,
            (SyncDirection::Disabled, _) | (_, SyncDirection::Disabled) => false,
            (SyncDirection::Both, _) | (_, SyncDirection::Both) => true,
            (a, b) => a == b,
        }
    }
}

#[derive(
    KnownLayout, Immutable, TryFromBytes, IntoBytes, Clone, PartialEq, Eq, Debug, Default, Serialize,
)]
#[repr(C)]
pub struct VolumeConfig {
    sync: SyncDirection,
}

impl VolumeConfig {
    pub const DEFAULT: Self = Self { sync: SyncDirection::Disabled };

    pub fn new(sync: SyncDirection) -> Self {
        Self { sync }
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, Culprit<StorageErr>> {
        Ok(Self::try_read_from_bytes(&bytes)
            .or_ctx(|e| StorageErr::CorruptVolumeState(VolumeStateTag::Config, e.into()))?)
    }

    pub fn sync(&self) -> SyncDirection {
        self.sync
    }

    pub fn with_sync(self, sync: SyncDirection) -> Self {
        Self { sync, ..self }
    }
}

impl AsRef<[u8]> for VolumeConfig {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Into<Slice> for VolumeConfig {
    fn into(self) -> Slice {
        self.as_bytes().into()
    }
}

#[derive(
    KnownLayout,
    Immutable,
    TryFromBytes,
    IntoBytes,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Debug,
    Serialize,
    Default,
)]
#[repr(u8)]
pub enum VolumeStatus {
    #[default]
    Ok = 0,

    /// The last commit graft attempted to push to the server was rejected
    RejectedCommit = 1,

    /// The local and remote volume state have diverged
    Conflict = 2,

    /// The volume was interrupted in the middle of a push operation
    InterruptedPush = 3,
}

impl VolumeStatus {
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, Culprit<StorageErr>> {
        Ok(Self::try_read_from_bytes(&bytes)
            .or_ctx(|e| StorageErr::CorruptVolumeState(VolumeStateTag::Status, e.into()))?)
    }
}

impl AsRef<[u8]> for VolumeStatus {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Into<Slice> for VolumeStatus {
    fn into(self) -> Slice {
        self.as_bytes().into()
    }
}

impl Display for VolumeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VolumeStatus::Ok => write!(f, "ok"),
            VolumeStatus::RejectedCommit => write!(f, "rejected commit"),
            VolumeStatus::Conflict => write!(f, "conflict"),
            VolumeStatus::InterruptedPush => write!(f, "interrupted push"),
        }
    }
}

#[derive(
    Debug,
    KnownLayout,
    Immutable,
    TryFromBytes,
    IntoBytes,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Default,
)]
#[repr(C)]
pub struct Watermarks {
    /// last_sync is the last local LSN that successfully synced with the server
    last_sync: MaybeLSN,

    /// pending_sync is a local LSN that is in the process of syncing to the server
    /// when pending_sync > last_sync it means that we are actively syncing (last_sync..=pending_sync)
    pending_sync: MaybeLSN,

    /// checkpoint is the last local LSN that represents a volume checkpoint
    checkpoint: MaybeLSN,
}

impl Watermarks {
    pub const DEFAULT: Self = Self {
        last_sync: MaybeLSN::EMPTY,
        pending_sync: MaybeLSN::EMPTY,
        checkpoint: MaybeLSN::EMPTY,
    };

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self, Culprit<StorageErr>> {
        Ok(Self::try_read_from_bytes(&bytes)
            .or_ctx(|e| StorageErr::CorruptVolumeState(VolumeStateTag::Watermarks, e.into()))?)
    }

    #[inline]
    pub fn last_sync(&self) -> Option<LSN> {
        self.last_sync.into()
    }

    #[inline]
    pub fn with_last_sync(self, lsn: LSN) -> Self {
        Self { last_sync: MaybeLSN::some(lsn), ..self }
    }

    #[inline]
    pub fn pending_sync(&self) -> Option<LSN> {
        self.pending_sync.into()
    }

    #[inline]
    pub fn with_pending_sync(self, lsn: LSN) -> Self {
        Self {
            pending_sync: MaybeLSN::some(lsn),
            ..self
        }
    }

    #[inline]
    pub fn commit_pending_sync(self) -> Self {
        assert!(
            self.last_sync() <= self.pending_sync(),
            "refusing to rollback pending sync during commit"
        );
        Self { last_sync: self.pending_sync, ..self }
    }

    #[inline]
    pub fn rollback_pending_sync(self) -> Self {
        assert!(
            self.last_sync() <= self.pending_sync(),
            "expected pending sync to be ahead of or equal to last sync"
        );
        Self { pending_sync: self.last_sync, ..self }
    }

    pub fn is_syncing(&self) -> bool {
        let last_sync = self.last_sync();
        let pending_sync = self.pending_sync();
        debug_assert!(
            last_sync <= pending_sync,
            "invariant violation: last_sync should never be larger than pending_sync"
        );
        last_sync < pending_sync
    }

    #[inline]
    pub fn checkpoint(&self) -> Option<LSN> {
        self.checkpoint.into()
    }

    #[inline]
    pub fn with_checkpoint(self, lsn: LSN) -> Self {
        Self { checkpoint: MaybeLSN::some(lsn), ..self }
    }
}

impl From<Watermarks> for Slice {
    fn from(watermarks: Watermarks) -> Slice {
        watermarks.as_bytes().into()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct VolumeState {
    vid: VolumeId,
    config: Option<VolumeConfig>,
    status: Option<VolumeStatus>,
    snapshot: Option<Snapshot>,
    watermarks: Option<Watermarks>,
}

impl VolumeState {
    pub(crate) fn new(vid: VolumeId) -> Self {
        Self {
            vid,
            config: None,
            status: None,
            snapshot: None,
            watermarks: None,
        }
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    pub fn config(&self) -> &VolumeConfig {
        precept::expect_always_or_unreachable!(
            self.config.is_some(),
            "volume config should always be present",
            { "state": self }
        );
        debug_assert!(
            self.config.is_some(),
            "volume config should always be present; got {self:?}"
        );
        self.config.as_ref().unwrap_or(&VolumeConfig::DEFAULT)
    }

    #[inline]
    pub fn status(&self) -> VolumeStatus {
        self.status.unwrap_or(VolumeStatus::Ok)
    }

    #[inline]
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    #[inline]
    pub fn watermarks(&self) -> &Watermarks {
        self.watermarks.as_ref().unwrap_or(&Watermarks::DEFAULT)
    }

    pub fn is_syncing(&self) -> bool {
        self.watermarks().is_syncing()
    }

    pub fn has_pending_commits(&self) -> bool {
        let last_sync = self.watermarks().last_sync();
        let local = self.snapshot().map(|s| s.local());
        debug_assert!(
            last_sync <= local,
            "invariant violation: last_sync should never be larger than local"
        );
        last_sync < local
    }

    pub(crate) fn accumulate(
        &mut self,
        tag: VolumeStateTag,
        value: Slice,
    ) -> Result<(), Culprit<StorageErr>> {
        match tag {
            VolumeStateTag::Config => {
                self.config = Some(VolumeConfig::from_bytes(&value)?);
            }
            VolumeStateTag::Status => {
                self.status = Some(VolumeStatus::from_bytes(&value)?);
            }
            VolumeStateTag::Snapshot => {
                self.snapshot = Some(Snapshot::from_bytes(&value)?);
            }
            VolumeStateTag::Watermarks => {
                self.watermarks = Some(Watermarks::from_bytes(&value)?);
            }
        }
        Ok(())
    }
}

pub struct VolumeQueryIter<I> {
    current: Option<VolumeState>,
    inner: I,
}

impl<I> VolumeQueryIter<I> {
    pub fn new(inner: I) -> Self {
        Self { current: None, inner }
    }
}

impl<I> VolumeQueryIter<I>
where
    I: Iterator<Item = lsm_tree::Result<KvPair>>,
{
    fn next_inner(&mut self) -> Result<Option<VolumeState>, Culprit<StorageErr>> {
        // pull from our inner iterator until we see the next vid, then emit
        while let Some((key, value)) = self.inner.try_next().or_into_ctx()? {
            let key = VolumeStateKey::ref_from_bytes(&key)?;

            let current = self
                .current
                .get_or_insert_with(|| VolumeState::new(key.vid.clone()));

            if current.vid != key.vid {
                // this key corresponds to the next volume, so let's initialize
                // a new volume state and return the current state
                let mut next_state = VolumeState::new(key.vid.clone());
                next_state.accumulate(key.tag, value)?;
                let state = self.current.replace(next_state);
                return Ok(state);
            } else {
                // this key corresponds to the current volume, so let's
                // accumulate it into the state
                current.accumulate(key.tag, value)?;
            }
        }

        // we've exhausted the iterator, so return the current state if one
        // exists. this will also fuse the iterator.
        Ok(self.current.take())
    }
}

impl<I> Iterator for VolumeQueryIter<I>
where
    I: Iterator<Item = lsm_tree::Result<KvPair>>,
{
    type Item = Result<VolumeState, Culprit<StorageErr>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_inner().transpose()
    }
}

// VolumeQueryIter fuses
impl<I> FusedIterator for VolumeQueryIter<I> where I: Iterator<Item = lsm_tree::Result<KvPair>> {}
