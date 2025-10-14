use std::{collections::HashSet, fmt::Debug, ops::RangeInclusive, path::Path, sync::Arc};

use fjall::{Instant, PartitionCreateOptions};
use futures::Stream;
use graft_core::{
    PageCount, PageIdx, SegmentId, VolumeId,
    checkpoint_set::CheckpointSet,
    commit::{Commit, SegmentIdx},
    lsn::LSN,
    page::Page,
    volume_meta::VolumeMeta,
    volume_ref::VolumeRef,
};
use parking_lot::{Mutex, MutexGuard};
use tryiter::TryIteratorExt;

use crate::{
    changeset::ChangeSet,
    local::fjall_storage::{
        keys::{CommitKey, PageKey},
        typed_partition::{FjallBatchExt, TypedPartition, TypedPartitionSnapshot},
    },
    named_volume::NamedVolumeState,
    search_path::SearchPath,
    snapshot::Snapshot,
    volume_name::VolumeName,
};

use culprit::{Culprit, Result, ResultExt};

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

    #[error("Concurrent write to Volume {0} detected")]
    ConcurrentWrite(VolumeId),
}

pub struct FjallStorage {
    keyspace: fjall::Keyspace,

    /// This partition stores state regarding each NamedVolume
    /// {`VolumeName`} -> `NamedVolumeState`
    named: TypedPartition<VolumeName, NamedVolumeState>,

    /// This partition stores metadata about each Volume
    /// {vid} -> `VolumeMeta`
    volumes: TypedPartition<VolumeId, VolumeMeta>,

    /// This partition stores commits
    /// {vid} / {lsn} -> Commit
    log: TypedPartition<CommitKey, Commit>,

    /// This partition stores Pages
    /// {sid} / {pageidx} -> Page
    pages: TypedPartition<PageKey, Page>,

    /// Must be held while performing read+write transactions.
    /// Read-only and write-only transactions don't need to hold the lock as
    /// long as they are safe:
    /// To make read-only txns safe, use the same snapshot for all reads
    /// To make write-only txns safe, they must be monotonic
    lock: Arc<Mutex<()>>,

    /// The commits changeset is notified whenever a NamedVolume's local Volume
    /// receives a commit.
    commits: ChangeSet<VolumeName>,
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
        let named = TypedPartition::open(&keyspace, "named", Default::default())?;
        let volumes = TypedPartition::open(&keyspace, "volumes", Default::default())?;
        let log = TypedPartition::open(&keyspace, "log", Default::default())?;
        let pages = TypedPartition::open(
            &keyspace,
            "pages",
            PartitionCreateOptions::default().with_kv_separation(Default::default()),
        )?;

        Ok(Self {
            keyspace,
            named,
            volumes,
            log,
            pages,
            lock: Default::default(),
            commits: Default::default(),
        })
    }

    pub(crate) fn read(&self) -> ReadGuard<'_> {
        ReadGuard::open(self)
    }

    /// Open a read + write txn on storage.
    /// The returned object holds a lock, any subsequent calls to ReadWriteGuard
    /// will block.
    fn read_write(&self) -> ReadWriteGuard<'_> {
        ReadWriteGuard::open(self)
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

    /// Attempt to execute a local commit on the volume pointed to by Snapshot.
    /// The resulting commit will claim the next LSN after the Snapshot.
    /// The `sid` must be a Segment containing all of the pages in this commit,
    /// which must match the provided `graft`.
    pub fn commit(
        &self,
        name: VolumeName,
        snapshot: Snapshot,
        page_count: PageCount,
        segment: SegmentIdx,
    ) -> Result<(), FjallStorageErr> {
        self.read_write()
            .commit(&name, snapshot, page_count, segment)?;
        // notify downstream subscribers
        self.commits.mark_changed(&name);
        Ok(())
    }

    pub fn open_named_volume(&self, name: VolumeName) -> Result<NamedVolumeState, FjallStorageErr> {
        self.read_write().open_named_volume(name)
    }

    pub fn subscribe_commits(&self) -> impl Stream<Item = HashSet<VolumeName>> + use<> {
        self.commits.subscribe_all()
    }
}

pub struct ReadGuard<'a> {
    storage: &'a FjallStorage,
    seqno: Instant,
}

impl Drop for ReadGuard<'_> {
    fn drop(&mut self) {
        // IMPORTANT: Decrement snapshot count
        self.storage.keyspace.snapshot_tracker.close(self.seqno);
    }
}

impl<'a> ReadGuard<'a> {
    fn open(storage: &'a FjallStorage) -> ReadGuard<'a> {
        let seqno = storage.keyspace.instant();
        // IMPORTANT: Increment snapshot count
        storage.keyspace.snapshot_tracker.open(seqno);
        Self { storage, seqno }
    }

    fn named(&self) -> TypedPartitionSnapshot<VolumeName, NamedVolumeState> {
        self.storage.named.snapshot_at(self.seqno)
    }

    fn volumes(&self) -> TypedPartitionSnapshot<VolumeId, VolumeMeta> {
        self.storage.volumes.snapshot_at(self.seqno)
    }

    fn log(&self) -> TypedPartitionSnapshot<CommitKey, Commit> {
        self.storage.log.snapshot_at(self.seqno)
    }

    fn pages(&self) -> TypedPartitionSnapshot<PageKey, Page> {
        self.storage.pages.snapshot_at(self.seqno)
    }

    pub fn named_volumes(&self) -> impl Iterator<Item = Result<NamedVolumeState, FjallStorageErr>> {
        self.named().range(..).map_ok(|(_, v)| Ok(v))
    }

    /// Retrieve the latest `Snapshot` corresponding to the local Volume for the
    /// `NamedVolume` named `name`
    pub fn named_local_snapshot(
        &self,
        name: &VolumeName,
    ) -> Result<Option<Snapshot>, FjallStorageErr> {
        if let Some(handle) = self.named().get(name)? {
            Ok(Some(self.snapshot(handle.local().vid())?))
        } else {
            Ok(None)
        }
    }

    pub fn snapshot(&self, vid: &VolumeId) -> Result<Snapshot, FjallStorageErr> {
        // compute the Snapshot's VolumeRef
        let vref = if let Some(commit) = self.log().first(vid)? {
            Some(commit.vref())
        } else {
            // no commit found in volume, search starting at the volume's parent
            self.volumes()
                .get(vid)?
                .and_then(|meta| meta.parent().cloned())
        };

        // assuming we found a vref, compute the snapshots search path and return a new tracked snapshot
        if let Some(vref) = vref {
            let path = self.search_path(vref.clone())?;
            let (vid, _) = vref.into();
            Ok(Snapshot::new(vid, path))
        } else {
            Ok(Snapshot::new(vid.clone(), SearchPath::EMPTY))
        }
    }

    fn search_path(&self, mut vref: VolumeRef) -> Result<SearchPath, FjallStorageErr> {
        let volumes = self.volumes();
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

    fn get_commit(&self, vid: &VolumeId, lsn: LSN) -> Result<Option<Commit>, FjallStorageErr> {
        self.log().get_owned(CommitKey::new(vid.clone(), lsn))
    }

    pub fn commits(
        &self,
        path: &SearchPath,
    ) -> impl Iterator<Item = Result<Commit, FjallStorageErr>> {
        let log = self.log();

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

    pub fn search_page(
        &self,
        snapshot: &Snapshot,
        pageidx: PageIdx,
    ) -> Result<Option<Commit>, FjallStorageErr> {
        let mut commits = self.commits(snapshot.search_path());

        while let Some(commit) = commits.try_next()? {
            if !commit.page_count().contains(pageidx) {
                // the volume is smaller than the requested page idx.
                // this also handles the case that a volume is truncated and
                // then subsequently extended at a later time.
                break;
            }

            let Some(idx) = commit.segment_idx() else {
                // this commit contains no pages
                continue;
            };

            if !idx.contains(pageidx) {
                // this commit does not contain the requested pageidx
                continue;
            }

            return Ok(Some(commit));
        }
        Ok(None)
    }

    pub fn read_page(
        &self,
        sid: SegmentId,
        pageidx: PageIdx,
    ) -> Result<Option<Page>, FjallStorageErr> {
        self.pages()
            .get_owned(PageKey::new(sid, pageidx))
            .or_into_ctx()
    }

    pub fn page_count(&self, snapshot: &Snapshot) -> Result<PageCount, FjallStorageErr> {
        if let Some(lsn) = snapshot.lsn() {
            let commit = self
                .get_commit(snapshot.vid(), lsn)?
                .expect("no commit found for snapshot");
            Ok(commit.page_count())
        } else {
            Ok(PageCount::ZERO)
        }
    }
}

pub struct ReadWriteGuard<'a> {
    _permit: MutexGuard<'a, ()>,
    read: ReadGuard<'a>,
}

impl<'a> ReadWriteGuard<'a> {
    fn open(storage: &'a FjallStorage) -> Self {
        // TODO: consider adding a lock timeout for deadlock detection
        let _permit = storage.lock.lock();
        // IMPORTANT: take the read snapshot after taking the lock
        let read = storage.read();
        Self { _permit, read }
    }

    pub fn open_named_volume(&self, name: VolumeName) -> Result<NamedVolumeState, FjallStorageErr> {
        if let Some(state) = self.read.named().get(&name)? {
            Ok(state)
        } else {
            let mut batch = self.read.storage.keyspace.batch();

            // create a new local volume
            let vid = VolumeId::random();
            let local = VolumeMeta::new(vid.clone(), None, None, CheckpointSet::EMPTY);
            batch.insert_typed(&self.read.storage.volumes, vid.clone(), local);

            // write an empty initial commit
            let commit = Commit::new(vid.clone(), LSN::FIRST, PageCount::ZERO);
            batch.insert_typed(
                &self.read.storage.log,
                CommitKey::new(vid.clone(), LSN::FIRST),
                commit,
            );

            let localref = VolumeRef::new(vid, LSN::FIRST);

            // put it in a named volume
            let volume = NamedVolumeState::new(name.clone(), localref, None, None);
            batch.insert_typed(&self.read.storage.named, name, volume.clone());

            batch.commit()?;

            Ok(volume)
        }
    }

    pub fn commit(
        &self,
        name: &VolumeName,
        snapshot: Snapshot,
        page_count: PageCount,
        segment: SegmentIdx,
    ) -> Result<(), FjallStorageErr> {
        let commit_lsn = snapshot
            .lsn()
            .unwrap_or_default()
            .next()
            .expect("LSN overflow");

        let latest_snapshot = self
            .read
            .named_local_snapshot(name)?
            .expect("BUG: named volume is missing");

        // 1. We check the LSN rather than the whole path, as checkpoints may
        // cause the path to change without changing the logical representation of the snapshot.
        // 2. We check that the VolumeId is the same to handle the rare case of
        // a NamedVolume's local volume changing.
        if snapshot.lsn() != latest_snapshot.lsn() || snapshot.vid() != latest_snapshot.vid() {
            return Err(Culprit::from_err(FjallStorageErr::ConcurrentWrite(
                snapshot.vid().clone(),
            )));
        }

        let commit = Commit::new(snapshot.vid().clone(), commit_lsn, page_count)
            .with_segment_idx(Some(segment));

        self.read
            .storage
            .log
            .insert(CommitKey::new(snapshot.vid().clone(), commit_lsn), commit)
    }
}
