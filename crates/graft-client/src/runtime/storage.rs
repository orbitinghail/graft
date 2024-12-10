use std::{io, path::Path};

use fjall::{KvSeparationOptions, PartitionCreateOptions};

pub trait Storage {
    type Error;
}

mod page;
mod snapshot;

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

    /// Used to store volume attributes
    /// maps from (VolumeId, SnapshotKind) to Snapshot
    volumes: fjall::Partition,

    /// Used to store page contents
    /// maps from (VolumeId, Offset, LSN) to PageValue
    pages: fjall::Partition,

    /// Used to track changes made by local commits.
    /// maps from (VolumeId, LSN) to Splinter of written offsets
    commits: fjall::Partition,
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
        let commits = keyspace.open_partition(
            "commits",
            PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
        )?;
        Ok(FjallStorage { keyspace, volumes, pages, commits })
    }
}

impl Storage for FjallStorage {
    type Error = FjallStorageErr;
}
