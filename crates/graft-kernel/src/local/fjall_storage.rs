use std::path::Path;

use fjall::PartitionCreateOptions;
use graft_core::{
    VolumeId,
    codec::{self, BilrostCodec, PageCodec},
    commit::Commit,
    handle_id::HandleId,
    volume_handle::VolumeHandle,
    volume_meta::VolumeMeta,
};

use crate::{
    local::fjall_storage::{
        keys::{CommitKey, PageKey},
        typed_partition::TypedPartition,
    },
    tracked_snapshot::TrackedSnapshot,
};

use culprit::Result;

pub mod keys;
mod typed_partition;

#[derive(Debug, thiserror::Error)]
pub enum FjallStorageErr {
    #[error("Fjall error: {0}")]
    FjallErr(#[from] fjall::Error),

    #[error("Fjall LSM Tree error: {0}")]
    LsmTreeErr(#[from] lsm_tree::Error),

    #[error("Codec error: {0}")]
    CodecDecodeErr(#[from] codec::DecodeErr),

    #[error("Key decode error: {0}")]
    KeyDecodeErr(#[from] keys::DecodeErr),

    #[error("I/O Error: {0}")]
    IoErr(#[from] std::io::Error),
}

pub struct FjallStorage {
    keyspace: fjall::Keyspace,

    /// This partition maps `VolumeHandle` IDs to `VolumeHandles`
    /// {`HandleId`} -> `VolumeHandle`
    /// Keyed by `keys::HandleKey`
    handles: TypedPartition<HandleId, BilrostCodec<VolumeHandle>>,

    /// This partition stores metadata about each Volume
    /// {vid} -> VolumeMeta
    /// Keyed by `keys::VolumeKey`
    volumes: TypedPartition<VolumeId, BilrostCodec<VolumeMeta>>,

    /// This partition stores commits
    /// {vid} / {lsn} -> Commit
    /// Keyed by `keys::CommitKey`
    log: TypedPartition<CommitKey, BilrostCodec<Commit>>,

    /// This partition stores Pages
    /// {sid} / {pageidx} -> Page
    /// Keyed by `keys::PageKey`
    pages: TypedPartition<PageKey, PageCodec>,
}

impl FjallStorage {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, FjallStorageErr> {
        tracing::debug!("opening Fjall storage at {}", path.as_ref().display());
        Self::open_config(fjall::Config::new(path))
    }

    pub fn open_temporary() -> Result<Self, FjallStorageErr> {
        let path = tempfile::tempdir()?.keep();
        tracing::debug!("opening temporary Fjall storage at {}", path.display());
        Self::open_config(fjall::Config::new(path).temporary(true))
    }

    fn open_config(config: fjall::Config) -> Result<Self, FjallStorageErr> {
        let keyspace = config.open()?;
        let handles = TypedPartition::open(&keyspace, "handles", Default::default())?;
        let volumes = TypedPartition::open(&keyspace, "volumes", Default::default())?;
        let log = TypedPartition::open(&keyspace, "log", Default::default())?;
        let pages = TypedPartition::open(
            &keyspace,
            "pages",
            PartitionCreateOptions::default().with_kv_separation(Default::default()),
        )?;

        Ok(Self { keyspace, handles, volumes, log, pages })
    }

    pub fn snapshot(&self, vid: &VolumeId) -> Result<Option<TrackedSnapshot>, FjallStorageErr> {
        let seqno = self.keyspace.instant();
        if let Some(commit) = TypedPartition::snapshot_at(&self.log, seqno).first(vid)? {
            todo!("need to compute the commit's SearchPath")
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use graft_core::VolumeId;

    use super::FjallStorage;

    #[graft_test::test]
    fn test_fjall_storage() {
        let storage = FjallStorage::open_temporary().unwrap();
        let _ = storage.snapshot(&VolumeId::random()).unwrap();
    }
}
