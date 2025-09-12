use std::{fmt::Debug, ops::RangeInclusive, path::Path};

use fjall::PartitionCreateOptions;
use graft_core::{
    PageCount, PageIdx, SegmentId, VolumeId, commit::Commit, handle_id::HandleId, lsn::LSN,
    page::Page, volume_handle::VolumeHandle, volume_meta::VolumeMeta, volume_ref::VolumeRef,
};
use lsm_tree::SeqNo;
use tryiter::TryIteratorExt;

use crate::{
    local::{
        fjall_storage::{
            keys::{CommitKey, PageKey},
            typed_partition::{FjallBatchExt, TypedPartition},
        },
        staged_segment::StagedSegment,
    },
    search_path::SearchPath,
    snapshot::Snapshot,
};

use culprit::{Result, ResultExt};

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

#[derive(Clone)]
pub struct FjallStorage {
    keyspace: fjall::Keyspace,

    /// This partition maps `VolumeHandle` IDs to `VolumeHandles`
    /// {`HandleId`} -> `VolumeHandle`
    /// Keyed by `keys::HandleKey`
    handles: TypedPartition<HandleId, VolumeHandle>,

    /// This partition stores metadata about each Volume
    /// {vid} -> `VolumeMeta`
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

impl Debug for FjallStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallStorage").finish()
    }
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

    pub fn snapshot(&self, vid: &VolumeId) -> Result<Snapshot, FjallStorageErr> {
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
            let (vid, _) = vref.into();
            Ok(Snapshot::new(vid, path))
        } else {
            Ok(Snapshot::new(vid.clone(), SearchPath::EMPTY))
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
                path.append(meta.vid().clone(), checkpoint..=vref.lsn());
                return Ok(path);
            }

            // no checkpoint, scan to the beginning and recurse to the parent if possible
            path.append(meta.vid().clone(), LSN::FIRST..=vref.lsn());
            if let Some(parent) = meta.parent() {
                vref = parent.clone();
            } else {
                break; // no parent, we reached the root
            }
        }

        Ok(path)
    }

    pub fn read_commit(&self, vid: &VolumeId, lsn: LSN) -> Result<Option<Commit>, FjallStorageErr> {
        self.log
            .snapshot()
            .get_owned(CommitKey::new(vid.clone(), lsn))
    }

    pub fn commits(
        &self,
        path: &SearchPath,
    ) -> impl Iterator<Item = Result<Commit, FjallStorageErr>> {
        let log = self.log.snapshot();

        path.iter().flat_map(move |entry| {
            // the entry range is in the form `low..high` but the commits
            // partition orders LSNs in reverse. thus we need to flip the range
            // when passing it down to the underlying scan.
            let low = CommitKey::new(entry.vid().clone(), *entry.lsns().start());
            let high = CommitKey::new(entry.vid().clone(), *entry.lsns().end());
            let range = high..=low;
            log.range(range).map_ok(|(_, commit)| Ok(commit))
        })
    }

    pub fn read_page(
        &self,
        sid: SegmentId,
        pageidx: PageIdx,
    ) -> Result<Option<Page>, FjallStorageErr> {
        self.pages
            .snapshot()
            .get_owned(PageKey::new(sid, pageidx))
            .or_into_ctx()
    }

    pub fn write_page(
        &self,
        sid: SegmentId,
        pageidx: PageIdx,
        page: Page,
    ) -> Result<(), FjallStorageErr> {
        self.pages
            .insert(PageKey::new(sid, pageidx), page)
            .or_into_ctx()
    }

    pub fn remove_page(&self, sid: SegmentId, pageidx: PageIdx) -> Result<(), FjallStorageErr> {
        self.pages.remove(PageKey::new(sid, pageidx)).or_into_ctx()
    }

    pub fn remove_page_range(
        &self,
        sid: &SegmentId,
        pages: RangeInclusive<PageIdx>,
    ) -> Result<(), FjallStorageErr> {
        // PageKeys are stored in descending order
        let keyrange =
            PageKey::new(sid.clone(), *pages.end())..=PageKey::new(sid.clone(), *pages.start());
        let mut batch = self.keyspace.batch();
        let mut iter = self.pages.snapshot().range(keyrange);
        while let Some((key, _)) = iter.try_next()? {
            batch.remove_typed(&self.pages, key);
        }
        batch.commit()?;
        Ok(())
    }

    pub fn remove_segment(&self, sid: &SegmentId) -> Result<(), FjallStorageErr> {
        let mut batch = self.keyspace.batch();
        let mut iter = self.pages.snapshot().prefix(sid);
        while let Some((key, _)) = iter.try_next()? {
            batch.remove_typed(&self.pages, key);
        }
        batch.commit()?;
        Ok(())
    }

    /// Attempt to execute a local commit on the volume pointed to by Snapshot.
    /// The resulting commit will claim the next LSN after the Snapshot.
    /// The `sid` must be a Segment containing all of the pages in this commit,
    /// which must match the provided `graft`.
    pub fn commit(
        &self,
        snapshot: Snapshot,
        page_count: PageCount,
        segment: StagedSegment,
    ) -> Result<(), FjallStorageErr> {
        let commit_lsn = snapshot
            .lsn()
            .unwrap_or_default()
            .next()
            .expect("LSN overflow");
        todo!()
    }
}
