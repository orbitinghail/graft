use std::{
    fmt::{Debug, Display},
    io,
    path::Path,
};

use fjall::{KvSeparationOptions, PartitionCreateOptions};
use graft_core::{lsn::LSN, offset::Offset, VolumeId};
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, BE, U32, U64};

pub trait Storage {
    type Error;
}

#[derive(KnownLayout, Immutable, TryFromBytes, IntoBytes)]
#[repr(C, packed)]
struct PageKey {
    vid: VolumeId,
    offset: U32<BE>,
    lsn: U64<BE>,
}

impl PageKey {
    #[inline]
    fn new(vid: VolumeId, offset: Offset, lsn: LSN) -> Self {
        Self {
            vid,
            offset: offset.into(),
            lsn: lsn.into(),
        }
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    pub fn offset(&self) -> Offset {
        self.offset.get()
    }

    #[inline]
    pub fn lsn(&self) -> LSN {
        self.lsn.get()
    }
}

impl AsRef<[u8]> for PageKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Display for PageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}@{}", self.vid.short(), self.offset, self.lsn)
    }
}

impl Debug for PageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl Clone for PageKey {
    fn clone(&self) -> Self {
        Self {
            vid: self.vid.clone(),
            offset: self.offset,
            lsn: self.lsn,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FjallStorageErr {
    #[error(transparent)]
    FjallErr(#[from] fjall::Error),

    #[error(transparent)]
    IoErr(#[from] io::Error),
}

#[derive(Clone)]
pub struct FjallStorage {
    keyspace: fjall::Keyspace,

    /// maps from VolumeId to (lsn, checkpoint_lsn, last_offset)
    volumes: fjall::Partition,

    /// maps from (VolumeId, Offset, LSN) to Page|MarkPending
    pages: fjall::Partition,

    /// maps from (VolumeId, Offset, Local LSN) to Page
    pending: fjall::Partition,
}

impl FjallStorage {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, FjallStorageErr> {
        Self::open_config(fjall::Config::new(path))
    }

    pub fn open_temporary() -> Result<Self, FjallStorageErr> {
        Self::open_config(fjall::Config::new(tempfile::tempdir()?.into_path()).temporary(true))
    }

    pub fn open_config(config: fjall::Config) -> Result<Self, FjallStorageErr> {
        let keyspace = config.open()?;
        let volumes = keyspace.open_partition("volumes", Default::default())?;
        let pages = keyspace.open_partition(
            "pages",
            PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
        )?;
        let pending = keyspace.open_partition(
            "pending",
            PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
        )?;
        Ok(FjallStorage { keyspace, volumes, pages, pending })
    }
}

impl Storage for FjallStorage {
    type Error = FjallStorageErr;
}
