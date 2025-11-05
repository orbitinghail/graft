use std::{collections::HashSet, fmt::Debug, ops::RangeInclusive, path::Path, sync::Arc};

use fjall::{Batch, Instant, PartitionCreateOptions};
use futures::Stream;
use graft_core::{
    PageCount, PageIdx, SegmentId, VolumeId,
    checkpoints::CachedCheckpoints,
    commit::{Commit, SegmentIdx},
    commit_hash::CommitHash,
    lsn::{LSN, LSNRangeExt, LSNSet},
    page::Page,
    volume_control::VolumeControl,
    volume_meta::VolumeMeta,
    volume_ref::VolumeRef,
};
use parking_lot::{Mutex, MutexGuard};
use tryiter::TryIteratorExt;

use crate::{
    VolumeErr,
    changeset::ChangeSet,
    local::fjall_storage::{
        keys::PageKey,
        typed_partition::{TypedPartition, TypedPartitionSnapshot, fjall_batch_ext::FjallBatchExt},
    },
    named_volume::{NamedVolumeState, PendingCommit, SyncPoint},
    search_path::SearchPath,
    snapshot::Snapshot,
    volume_name::VolumeName,
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

    #[error("batch commit precondition failed")]
    BatchPreconditionErr,

    #[error(transparent)]
    VolumeErr(#[from] VolumeErr),
}

pub struct FjallStorage {
    keyspace: fjall::Keyspace,

    /// This partition stores state regarding each `NamedVolume`
    /// {`VolumeName`} -> `NamedVolumeState`
    named: TypedPartition<VolumeName, NamedVolumeState>,

    /// This partition stores metadata about each Volume
    /// {vid} -> `VolumeMeta`
    volumes: TypedPartition<VolumeId, VolumeMeta>,

    /// This partition stores commits
    /// {vid} / {lsn} -> Commit
    log: TypedPartition<VolumeRef, Commit>,

    /// This partition stores Pages
    /// {sid} / {pageidx} -> Page
    pages: TypedPartition<PageKey, Page>,

    /// Must be held while performing read+write transactions.
    /// Read-only and write-only transactions don't need to hold the lock as
    /// long as they are safe:
    /// To make read-only txns safe, use the same snapshot for all reads
    /// To make write-only txns safe, they must be monotonic
    lock: Arc<Mutex<()>>,

    /// The commits changeset is notified whenever a `NamedVolume`'s
    /// local Volume receives a commit.
    commits: ChangeSet<VolumeName>,
}

impl Debug for FjallStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FjallStorage").finish()
    }
}

impl FjallStorage {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, FjallStorageErr> {
        Self::open_config(fjall::Config::new(path))
    }

    pub fn open_temporary() -> Result<Self, FjallStorageErr> {
        let path = tempfile::tempdir()?.keep();
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

    pub fn subscribe_commits(&self) -> impl Stream<Item = HashSet<VolumeName>> + use<> {
        self.commits.subscribe_all()
    }

    pub(crate) fn read(&self) -> ReadGuard<'_> {
        ReadGuard::open(self)
    }

    pub(crate) fn batch(&self) -> WriteBatch<'_> {
        WriteBatch::open(self)
    }

    /// Open a read + write txn on storage.
    /// The returned object holds a lock, any subsequent calls to `ReadWriteGuard`
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
    ///
    /// Returns the resulting `VolumeRef` on success
    pub fn commit(
        &self,
        name: VolumeName,
        snapshot: Snapshot,
        page_count: PageCount,
        segment: SegmentIdx,
    ) -> Result<VolumeRef, FjallStorageErr> {
        let vref = self
            .read_write()
            .commit(&name, snapshot, page_count, segment)?;
        // notify downstream subscribers
        self.commits.mark_changed(&name);
        Ok(vref)
    }

    pub fn open_named_volume(
        &self,
        name: VolumeName,
        remote_vid: Option<VolumeId>,
    ) -> Result<NamedVolumeState, FjallStorageErr> {
        self.read_write().open_named_volume(name, remote_vid)
    }

    pub fn register_volume(
        &self,
        control: VolumeControl,
        checkpoints: CachedCheckpoints,
    ) -> Result<VolumeMeta, FjallStorageErr> {
        let meta = VolumeMeta::new(
            control.vid().clone(),
            control.parent().cloned(),
            checkpoints,
        );
        self.volumes.insert(control.vid().clone(), meta.clone())?;
        Ok(meta)
    }

    pub fn update_checkpoints(
        &self,
        vid: VolumeId,
        checkpoints: CachedCheckpoints,
    ) -> Result<VolumeMeta, FjallStorageErr> {
        self.read_write().update_checkpoints(vid, checkpoints)
    }

    /// Verify we are ready to make a remote commit and update the named volume
    /// with a `PendingCommit`
    pub fn remote_commit_prepare(
        &self,
        name: &VolumeName,
        pending_commit: PendingCommit,
    ) -> Result<(), FjallStorageErr> {
        self.read_write()
            .remote_commit_prepare(name, pending_commit)
    }

    /// Finish the remote commit process by writing out an updated named volume
    /// and recording the remote commit locally
    pub fn remote_commit_success(
        &self,
        name: &VolumeName,
        remote_commit: Commit,
    ) -> Result<(), FjallStorageErr> {
        self.read_write().remote_commit_success(name, remote_commit)
    }

    /// Drop a pending commit without applying it. This should only be called
    /// after receiving a rejection from the remote.
    pub fn drop_pending_commit(&self, name: &VolumeName) -> Result<(), FjallStorageErr> {
        self.read_write().drop_pending_commit(name)
    }

    /// Commit a batch with a precondition check.
    pub fn sync_remote_to_local(&self, name: VolumeName) -> Result<(), FjallStorageErr> {
        self.read_write().sync_remote_to_local(name)
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

    fn log(&self) -> TypedPartitionSnapshot<VolumeRef, Commit> {
        self.storage.log.snapshot_at(self.seqno)
    }

    fn pages(&self) -> TypedPartitionSnapshot<PageKey, Page> {
        self.storage.pages.snapshot_at(self.seqno)
    }

    pub fn named_volumes(&self) -> impl Iterator<Item = Result<NamedVolumeState, FjallStorageErr>> {
        self.named().values()
    }

    pub fn named_volume_exists(&self, name: &VolumeName) -> Result<bool, FjallStorageErr> {
        self.named().contains(name)
    }

    pub fn named_volume(&self, name: &VolumeName) -> Result<NamedVolumeState, FjallStorageErr> {
        self.named()
            .get(name)?
            .ok_or_else(|| VolumeErr::NamedVolumeNotFound(name.clone()))
            .or_into_ctx()
    }

    pub fn volume_meta(&self, vid: &VolumeId) -> Result<Option<VolumeMeta>, FjallStorageErr> {
        self.volumes().get(vid)
    }

    /// Load a Volume's latest snapshot
    pub fn snapshot(&self, vid: &VolumeId) -> Result<Snapshot, FjallStorageErr> {
        self.snapshot_at(vid, None)
    }

    /// Load the most recent Snapshot for a Volume as of the provided `max_lsn`.
    /// If `max_lsn` is None, loads the most recent Snapshot available.
    pub fn snapshot_at(
        &self,
        vid: &VolumeId,
        max_lsn: Option<LSN>,
    ) -> Result<Snapshot, FjallStorageErr> {
        // compute the Snapshot's VolumeRef at the search LSN (or latest)
        let search_range = VolumeRef::new(vid.clone(), max_lsn.unwrap_or(LSN::LAST))
            ..=VolumeRef::new(vid.clone(), LSN::FIRST);
        let vref = if let Some((_, commit)) = self.log().range(search_range).try_next()? {
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
            Ok(Snapshot::new(vref.vid, path))
        } else {
            Ok(Snapshot::new(vid.clone(), SearchPath::EMPTY))
        }
    }

    fn search_path(&self, mut vref: VolumeRef) -> Result<SearchPath, FjallStorageErr> {
        let volumes = self.volumes();
        let mut path = SearchPath::default();

        const MAX_HOPS: usize = 10;
        for hops in 0.. {
            assert!(
                hops <= MAX_HOPS,
                "Exceeded maximum parent recursion ({}) when building search path for volume {}",
                MAX_HOPS,
                vref.vid()
            );

            if let Some(meta) = volumes.get(vref.vid())? {
                if let Some(checkpoint) = meta.checkpoint_for(vref.lsn()) {
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
            } else {
                // There is no VolumeMeta for this VRef
                // this means there is no parent and no checkpoints
                // so scan to the beginning and stop searching
                path.append(vref.vid().clone(), LSN::FIRST..=vref.lsn());
                break;
            }
        }

        Ok(path)
    }

    /// Retrieve a specific commit
    fn get_commit(&self, vid: &VolumeId, lsn: LSN) -> Result<Option<Commit>, FjallStorageErr> {
        self.log().get_owned(VolumeRef::new(vid.clone(), lsn))
    }

    /// Iterates through all of the commits reachable by the provided `SearchPath`
    /// from the newest to oldest commit.
    pub fn commits(
        &self,
        path: &SearchPath,
    ) -> impl Iterator<Item = Result<Commit, FjallStorageErr>> {
        let log = self.log();

        path.iter().flat_map(move |entry| {
            // the entry range is in the form `low..=high` but the log orders
            // LSNs in reverse. thus we need to flip the range
            // when passing it down to the underlying scan.
            let low = entry.start_ref();
            let high = entry.end_ref();
            let range = high..=low;
            log.range(range).map_ok(|(_, commit)| Ok(commit))
        })
    }

    /// Given a range of LSNs for a particular volume, returns the set of LSNs
    /// we have
    pub fn lsns(
        &self,
        vid: &VolumeId,
        lsns: &RangeInclusive<LSN>,
    ) -> Result<LSNSet, FjallStorageErr> {
        // lsns is in the form `low..=high` but the log orders
        // LSNs in reverse. thus we need to flip the range
        let low = VolumeRef::new(vid.clone(), *lsns.start());
        let high = VolumeRef::new(vid.clone(), *lsns.end());
        let range = high..=low;
        self.log()
            .range_keys(range)
            .map_ok(|key| Ok(key.lsn()))
            .collect()
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

    pub fn has_page(&self, sid: SegmentId, pageidx: PageIdx) -> Result<bool, FjallStorageErr> {
        self.pages().contains(&PageKey::new(sid, pageidx))
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

pub struct WriteBatch<'a> {
    storage: &'a FjallStorage,
    batch: Batch,
}

impl<'a> WriteBatch<'a> {
    fn open(storage: &'a FjallStorage) -> Self {
        Self { storage, batch: storage.keyspace.batch() }
    }

    pub fn write_commit(&mut self, commit: Commit) {
        self.batch
            .insert_typed(&self.storage.log, commit.vref(), commit);
    }

    pub fn write_named_volume(&mut self, handle: NamedVolumeState) {
        self.batch
            .insert_typed(&self.storage.named, handle.name.clone(), handle);
    }

    pub fn write_page(&mut self, sid: SegmentId, pageidx: PageIdx, page: Page) {
        self.batch
            .insert_typed(&self.storage.pages, PageKey::new(sid, pageidx), page);
    }

    pub fn commit(self) -> Result<(), FjallStorageErr> {
        self.batch.commit().or_into_ctx()
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

    fn storage(&self) -> &'a FjallStorage {
        self.read.storage
    }

    pub fn open_named_volume(
        self,
        name: VolumeName,
        remote_vid: Option<VolumeId>,
    ) -> Result<NamedVolumeState, FjallStorageErr> {
        if let Some(state) = self.read.named().get(&name)? {
            if let Some(expected) = remote_vid
                && state.remote != expected
            {
                Err(
                    VolumeErr::NamedVolumeRemoteMismatch { name, expected, actual: state.remote }
                        .into(),
                )
            } else {
                Ok(state)
            }
        } else {
            // create volume ids for the local and remote volume
            let lvid = VolumeId::random();
            let rvid = remote_vid.unwrap_or_else(VolumeId::random);

            // create the named volume
            let volume = NamedVolumeState::new(name.clone(), lvid, rvid, None, None);
            self.storage().named.insert(name, volume.clone())?;

            tracing::debug!(
                name = %volume.name,
                local_vid = ?volume.local,
                remote_vid = ?volume.remote,
                "created named volume"
            );

            Ok(volume)
        }
    }

    pub fn commit(
        self,
        name: &VolumeName,
        snapshot: Snapshot,
        page_count: PageCount,
        segment: SegmentIdx,
    ) -> Result<VolumeRef, FjallStorageErr> {
        let handle = self.read.named_volume(name)?;

        let latest_snapshot = self.read.snapshot(&handle.local)?;

        // Verify that the commit was constructed using the latest snapshot for
        // the volume.
        if snapshot != latest_snapshot {
            return Err(VolumeErr::ConcurrentWrite(snapshot.vid().clone()).into());
        }

        let commit_lsn = latest_snapshot
            .lsn()
            .map(|lsn| lsn.next())
            .unwrap_or_default();

        let commit = Commit::new(snapshot.vid().clone(), commit_lsn, page_count)
            .with_segment_idx(Some(segment));

        let vref = commit.vref();
        tracing::debug!(%vref, "local commit");
        self.read.storage.log.insert(vref.clone(), commit)?;
        Ok(vref)
    }

    pub fn update_checkpoints(
        self,
        vid: VolumeId,
        checkpoints: CachedCheckpoints,
    ) -> Result<VolumeMeta, FjallStorageErr> {
        let meta = self
            .read
            .volume_meta(&vid)?
            .ok_or_else(|| VolumeErr::VolumeNotFound(vid.clone()))?
            .with_checkpoints(checkpoints);
        self.storage().volumes.insert(vid, meta.clone())?;
        Ok(meta)
    }

    pub fn remote_commit_prepare(
        self,
        name: &VolumeName,
        pending_commit: PendingCommit,
    ) -> Result<(), FjallStorageErr> {
        let handle = self.read.named_volume(name)?;

        assert!(
            handle.pending_commit().is_none(),
            "BUG: pending commit is present"
        );

        // ensure LSN monotonicity
        if let Some(sync) = handle.sync() {
            assert!(sync.local < pending_commit.local_lsn);
        }
        let latest_remote = self.read.snapshot(&handle.remote)?;
        assert_eq!(
            latest_remote.lsn(),
            pending_commit.commit_lsn.checked_prev()
        );

        // remember to set the commit hash
        assert!(pending_commit.commit_hash != CommitHash::ZERO);

        // save the new pending commit
        let handle = handle.with_pending_commit(Some(pending_commit));
        self.storage().named.insert(handle.name.clone(), handle)?;

        Ok(())
    }

    pub fn remote_commit_success(
        self,
        name: &VolumeName,
        remote_commit: Commit,
    ) -> Result<(), FjallStorageErr> {
        let handle = self.read.named_volume(name)?;

        // verify the pending commit matches the remote commit
        let pending_commit = handle.pending_commit.unwrap();
        assert_eq!(remote_commit.lsn(), pending_commit.commit_lsn);
        assert_eq!(
            remote_commit.commit_hash(),
            Some(&pending_commit.commit_hash)
        );

        // fail if we somehow already know about this commit locally
        assert!(
            !self.read.log().contains(&remote_commit.vref())?,
            "BUG: remote commit already exists"
        );

        // build a new handle with the updated sync points and no pending_commit
        let new_handle = NamedVolumeState {
            sync: Some(pending_commit.into()),
            pending_commit: None,
            ..handle
        };

        let mut batch = self.storage().batch();
        batch.write_commit(remote_commit);
        batch.write_named_volume(new_handle);
        batch.commit()
    }

    pub fn drop_pending_commit(self, name: &VolumeName) -> Result<(), FjallStorageErr> {
        let handle = self.read.named_volume(name)?;
        self.storage()
            .named
            .insert(handle.name.clone(), handle.with_pending_commit(None))
    }

    pub fn sync_remote_to_local(self, name: VolumeName) -> Result<(), FjallStorageErr> {
        let handle = self.read.named_volume(&name)?;

        // check to see if we have any changes to sync
        let latest_remote = self.read.snapshot(&handle.remote).or_into_ctx()?;
        let Some(remote_changes) = handle.remote_changes(&latest_remote) else {
            // nothing to sync
            return Ok(());
        };

        // check for divergence
        let latest_local = self.read.snapshot(&handle.local).or_into_ctx()?;
        if handle.local_changes(&latest_local).is_some() {
            // the remote and local volumes have diverged
            let status = handle.status(&latest_local, &latest_remote);
            tracing::debug!("named volume {name} has diverged; status=`{status}`");
            return Err(VolumeErr::NamedVolumeDiverged(name).into());
        }

        tracing::debug!(
            sync = ?handle.sync(),
            lsns = %remote_changes.to_string(),
            remote = ?latest_remote.vid(),
            local = ?latest_local.vid(),
            "syncing commits from remote to local volume"
        );

        // save the remote lsn for later
        let remote_lsn = *remote_changes.end();

        // construct an iterator of new local lsns
        // note: this iterator must return new local lsns in reverse as the
        // commits iterator returns commits from newest to oldest
        let num_commits = remote_changes.len();
        let local_first = latest_local.lsn().map_or(LSN::FIRST, |l| l.next());
        let local_last = local_first
            .checked_add(num_commits - 1)
            .expect("LSN overflow");
        let mut new_local_lsns = (local_first..=local_last).iter().rev();

        // iterate missing remote commits, and commit them to the local volume
        let search = SearchPath::new(handle.remote.clone(), remote_changes);
        let mut batch = self.storage().batch();
        let mut commits = self.read.commits(&search);
        while let Some(commit) = commits.try_next().or_into_ctx()? {
            let next_lsn = new_local_lsns
                .next()
                .expect("BUG: storage has more commits than expected");
            // map the remote commit into the local volume
            batch.write_commit(
                commit
                    .with_vid(latest_local.vid().clone())
                    .with_lsn(next_lsn),
            );
        }

        assert!(
            new_local_lsns.next().is_none(),
            "BUG: not all new local lsns were used"
        );

        // update the sync point
        batch.write_named_volume(
            handle.with_sync(Some(SyncPoint { local: local_last, remote: remote_lsn })),
        );

        // commit the batch
        batch.commit()
    }
}
