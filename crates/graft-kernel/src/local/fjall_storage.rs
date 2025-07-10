use std::{ops::RangeBounds, path::Path};

use fjall::PartitionCreateOptions;
use graft_core::{
    VolumeId, commit::Commit, handle_id::HandleId, lsn::LSN, page::Page,
    volume_handle::VolumeHandle, volume_meta::VolumeMeta, volume_ref::VolumeRef,
};
use lsm_tree::SeqNo;

use crate::{
    local::fjall_storage::{
        keys::{CommitKey, PageKey},
        typed_partition::TypedPartition,
    },
    search_path::SearchPath,
    snapshot::Snapshot,
};

use culprit::Result;

mod fjall_repr;
pub mod keys;
mod typed_partition;
mod values;

#[derive(Debug, thiserror::Error)]
pub enum FjallStorageErr {
    #[error("Fjall error: {0}")]
    FjallErr(#[from] fjall::Error),

    #[error("Fjall LSM Tree error: {0}")]
    LsmTreeErr(#[from] lsm_tree::Error),

    #[error("Failed to decode key: {0}")]
    DecodeErr(#[from] fjall_repr::DecodeErr),

    #[error("I/O Error: {0}")]
    IoErr(#[from] std::io::Error),
}

pub struct FjallStorage {
    keyspace: fjall::Keyspace,

    /// This partition maps `VolumeHandle` IDs to `VolumeHandles`
    /// {`HandleId`} -> `VolumeHandle`
    /// Keyed by `keys::HandleKey`
    handles: TypedPartition<HandleId, VolumeHandle>,

    /// This partition stores metadata about each Volume
    /// {vid} -> VolumeMeta
    /// Keyed by `keys::VolumeKey`
    volumes: TypedPartition<VolumeId, VolumeMeta>,

    /// This partition stores commits
    /// {vid} / {lsn} -> Commit
    /// Keyed by `keys::CommitKey`
    log: TypedPartition<CommitKey, Commit>,

    /// This partition stores Pages
    /// {sid} / {pageidx} -> Page
    /// Keyed by `keys::PageKey`
    pages: TypedPartition<PageKey, Page>,
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

    pub fn snapshot(&self, vid: &VolumeId) -> Result<Option<Snapshot>, FjallStorageErr> {
        let seqno = self.keyspace.instant();
        let log = self.log.snapshot_at(seqno);

        // compute the Snapshot's VolumeRef
        let vref = if let Some(commit) = log.first(vid)? {
            Some(commit.vref())
        } else {
            // no commit found in volume, search starting at the volume's parent
            self.volumes
                .snapshot_at(seqno)
                .get(vid)?
                .and_then(|meta| meta.parent().cloned())
        };

        // assuming we found a vref, compute the snapshots search path and return a new tracked snapshot
        if let Some(vref) = vref {
            let path = self.search_path(seqno, vref.clone())?;
            Ok(Some(Snapshot::new(vref, path)))
        } else {
            Ok(None)
        }
    }

    fn search_path(
        &self,
        seqno: SeqNo,
        mut vref: VolumeRef,
    ) -> Result<SearchPath, FjallStorageErr> {
        let volumes = self.volumes.snapshot_at(seqno);
        let mut path = SearchPath::default();

        while let Some(meta) = volumes.get(vref.vid())? {
            if let Some(checkpoint) = meta.checkpoints().checkpoint_for(vref.lsn()) {
                // found a checkpoint, we can terminate the path here
                path.push(meta.vid().clone(), vref.lsn(), checkpoint);
                return Ok(path);
            }

            // no checkpoint, scan to the beginning and recurse to the parent if possible
            path.push(meta.vid().clone(), vref.lsn(), LSN::FIRST);
            if let Some(parent) = meta.parent() {
                vref = parent.clone();
            } else {
                break; // no parent, we reached the root
            }
        }

        Ok(path)
    }

    /// Returns an iterator over all commits in the given LSN range for the specified volume ID.
    pub fn commits<R: RangeBounds<LSN>>(
        &self,
        vid: &VolumeId,
        range: R,
    ) -> impl DoubleEndedIterator<Item = Result<Commit, FjallStorageErr>> {
        // the input range is in the form `low..high`
        // but the commits partition orders LSNs in reverse
        // thus we need to flip the range when passing it down to the underlying
        // TypedPartition query
        let range = (
            range
                .end_bound()
                .map(|lsn| CommitKey::new(vid.clone(), *lsn)),
            range
                .start_bound()
                .map(|lsn| CommitKey::new(vid.clone(), *lsn)),
        );

        self.log
            .snapshot()
            .range(range)
            .map(|result| result.map(|(_, commit)| commit))
    }
}
