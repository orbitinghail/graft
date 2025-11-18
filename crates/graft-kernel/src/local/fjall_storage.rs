use std::{fmt::Debug, ops::RangeInclusive, path::Path, sync::Arc};

use bytestring::ByteString;
use fjall::{Batch, Instant, KvSeparationOptions, PartitionCreateOptions};
use graft_core::{
    PageCount, PageIdx, SegmentId, VolumeId,
    checkpoints::CachedCheckpoints,
    checksum::{Checksum, ChecksumBuilder},
    commit::{Commit, SegmentIdx, SegmentRangeRef},
    commit_hash::CommitHash,
    lsn::{LSN, LSNRangeExt, LSNSet},
    page::Page,
    pageset::PageSet,
    volume_ref::VolumeRef,
};
use parking_lot::{Mutex, MutexGuard};
use tryiter::TryIteratorExt;

use crate::{
    LogicalErr,
    graft::{Graft, PendingCommit, SyncPoint},
    local::fjall_storage::{
        keys::PageKey,
        typed_partition::{TypedPartition, TypedPartitionSnapshot, fjall_batch_ext::FjallBatchExt},
    },
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

    #[error("batch commit precondition failed")]
    BatchPreconditionErr,

    #[error(transparent)]
    LogicalErr(#[from] LogicalErr),
}

pub struct FjallStorage {
    keyspace: fjall::Keyspace,

    /// This partition allows grafts to be identified by a tag.
    /// The graft a tag points at can be changed.
    tags: TypedPartition<ByteString, VolumeId>,

    /// This partition stores state regarding each `Graft`
    /// keyed by its Local Volume ID
    /// {`VolumeId`} -> `GraftState`
    grafts: TypedPartition<VolumeId, Graft>,

    /// This partition stores `CachedCheckpoints` for each Volume
    /// {vid} -> `CachedCheckpoints`
    checkpoints: TypedPartition<VolumeId, CachedCheckpoints>,

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
        let tags = TypedPartition::open(&keyspace, "tags", Default::default())?;
        let grafts = TypedPartition::open(&keyspace, "grafts", Default::default())?;
        let checkpoints = TypedPartition::open(&keyspace, "checkpoints", Default::default())?;
        let log = TypedPartition::open(&keyspace, "log", Default::default())?;
        let pages = TypedPartition::open(
            &keyspace,
            "pages",
            PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
        )?;

        Ok(Self {
            keyspace,
            tags,
            grafts,
            checkpoints,
            log,
            pages,
            lock: Default::default(),
        })
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

    /// Switch a tag to point at the specified graft, creating it if it doesn't exist
    pub fn switch_graft(
        &self,
        tag: &str,
        graft_vid: VolumeId,
        remote: Option<VolumeId>,
    ) -> Result<Graft, FjallStorageErr> {
        self.read_write().switch_graft(tag, graft_vid, remote)
    }

    /// Clone the specified remote into a new graft. Assign the resulting
    /// graft to the specified tag.
    pub fn clone_remote(&self, tag: &str, remote: VolumeId) -> Result<Graft, FjallStorageErr> {
        self.read_write().new_graft(tag, VolumeId::random(), remote)
    }

    pub fn get_or_create_tag(&self, tag: &str) -> Result<Graft, FjallStorageErr> {
        self.read_write().get_or_create_tag(tag)
    }

    /// Attempt to execute a local commit to the specified Graft's local volume.
    ///
    /// Returns the resulting `VolumeRef` on success
    pub fn commit(
        &self,
        graft: VolumeId,
        snapshot: Snapshot,
        page_count: PageCount,
        segment: SegmentIdx,
    ) -> Result<Snapshot, FjallStorageErr> {
        self.read_write()
            .commit(&graft, snapshot, page_count, segment)
    }

    /// Verify we are ready to make a remote commit and update the graft
    /// with a `PendingCommit`
    pub fn remote_commit_prepare(
        &self,
        graft: &VolumeId,
        pending_commit: PendingCommit,
    ) -> Result<(), FjallStorageErr> {
        self.read_write()
            .remote_commit_prepare(graft, pending_commit)
    }

    /// Finish the remote commit process by writing out an updated graft
    /// and recording the remote commit locally
    pub fn remote_commit_success(
        &self,
        graft: &VolumeId,
        remote_commit: Commit,
    ) -> Result<(), FjallStorageErr> {
        self.read_write()
            .remote_commit_success(graft, remote_commit)
    }

    /// Drop a pending commit without applying it. This should only be called
    /// after receiving a rejection from the remote.
    pub fn drop_pending_commit(&self, graft: &VolumeId) -> Result<(), FjallStorageErr> {
        self.read_write().drop_pending_commit(graft)
    }

    /// Commit a batch with a precondition check.
    pub fn sync_remote_to_local(&self, graft: VolumeId) -> Result<(), FjallStorageErr> {
        self.read_write().sync_remote_to_local(graft)
    }

    pub fn write_checkpoints(
        &self,
        vid: VolumeId,
        checkpoints: CachedCheckpoints,
    ) -> Result<(), FjallStorageErr> {
        self.checkpoints.insert(vid, checkpoints)
    }

    pub fn fork_snapshot(&self, snapshot: &Snapshot) -> Result<Graft, FjallStorageErr> {
        let graft = Graft::new(VolumeId::random(), VolumeId::random(), None, None);
        let commits = self
            .read()
            .commits(snapshot)
            .collect::<Result<Vec<_>, _>>()?;
        let mut lsn = LSN::FIRST.checked_add(commits.len() as u64).unwrap();
        let mut batch = self.batch();
        for commit in commits {
            lsn = lsn.checked_prev().unwrap();
            batch.write_commit(commit.with_vid(graft.local.clone()).with_lsn(lsn));
        }
        batch.write_graft(graft.clone());
        batch.commit()?;
        Ok(graft)
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

    fn _tags(&self) -> TypedPartitionSnapshot<ByteString, VolumeId> {
        self.storage.tags.snapshot_at(self.seqno)
    }

    fn _grafts(&self) -> TypedPartitionSnapshot<VolumeId, Graft> {
        self.storage.grafts.snapshot_at(self.seqno)
    }

    fn _checkpoints(&self) -> TypedPartitionSnapshot<VolumeId, CachedCheckpoints> {
        self.storage.checkpoints.snapshot_at(self.seqno)
    }

    fn _log(&self) -> TypedPartitionSnapshot<VolumeRef, Commit> {
        self.storage.log.snapshot_at(self.seqno)
    }

    fn _pages(&self) -> TypedPartitionSnapshot<PageKey, Page> {
        self.storage.pages.snapshot_at(self.seqno)
    }

    pub fn iter_tags(
        &self,
    ) -> impl Iterator<Item = Result<(ByteString, VolumeId), FjallStorageErr>> + use<> {
        self._tags().range(..)
    }

    pub fn tag_exists(&self, name: &str) -> Result<bool, FjallStorageErr> {
        self._tags().contains(name)
    }

    pub fn get_tag(&self, name: &str) -> Result<Option<Graft>, FjallStorageErr> {
        if let Some(vid) = self._tags().get(name)? {
            self._grafts().get(&vid)
        } else {
            Ok(None)
        }
    }

    /// Lookup the latest LSN for a volume
    pub fn latest_lsn(&self, vid: &VolumeId) -> Result<Option<LSN>, FjallStorageErr> {
        Ok(self._log().first(vid)?.map(|(vref, _)| vref.lsn))
    }

    pub fn iter_grafts(&self) -> impl Iterator<Item = Result<Graft, FjallStorageErr>> + use<> {
        self._grafts().values()
    }

    pub fn graft(&self, vid: &VolumeId) -> Result<Graft, FjallStorageErr> {
        self._grafts()
            .get(vid)?
            .ok_or_else(|| LogicalErr::GraftNotFound(vid.clone()).into())
    }

    /// Check if the provided Snapshot is logically equal to the latest snapshot
    /// for the specified Graft.
    pub fn is_latest_snapshot(
        &self,
        graft: &VolumeId,
        snapshot: &Snapshot,
    ) -> Result<bool, FjallStorageErr> {
        let graft = self.graft(graft)?;
        let latest_local = self.latest_lsn(&graft.local)?;

        // The complexity here is that the snapshot may have been taken before
        // we pushed commits to a remote. When this happens, the snapshot will
        // be physically different but logically equivalent. We can use the
        // relationship setup by the SyncPoint to handle this case.
        Ok(match snapshot.head() {
            Some((vid, lsn)) if vid == &graft.local => Some(lsn) == latest_local,

            Some((vid, lsn)) if vid == &graft.remote => {
                if let Some(sync) = graft.sync {
                    lsn == sync.remote && sync.local_watermark == latest_local
                } else {
                    // if graft has no sync point, then a snapshot should not
                    // include a remote layer, thus this snapshot is from
                    // another graft
                    false
                }
            }

            // Snapshot from another graft
            Some(_) => false,

            // Snapshot is empty
            None => latest_local.is_none() && graft.sync().is_none(),
        })
    }

    /// Load the most recent Snapshot for a Graft.
    pub fn snapshot(&self, graft: &VolumeId) -> Result<Snapshot, FjallStorageErr> {
        let graft = self.graft(graft)?;

        let mut snapshot = Snapshot::EMPTY;

        if let Some(latest) = self.latest_lsn(&graft.local)? {
            if let Some(watermark) = graft.sync().and_then(|s| s.local_watermark) {
                if watermark < latest {
                    snapshot.append(graft.local, watermark..=latest);
                }
            } else {
                snapshot.append(graft.local, LSN::FIRST..=latest);
            }
        }

        if let Some(remote) = graft.sync.map(|s| s.remote) {
            snapshot.append(graft.remote, LSN::FIRST..=remote);
        }

        Ok(snapshot)
    }

    /// Retrieve a specific commit
    pub fn get_commit(&self, vid: &VolumeId, lsn: LSN) -> Result<Option<Commit>, FjallStorageErr> {
        self._log().get_owned(VolumeRef::new(vid.clone(), lsn))
    }

    /// Iterates through all of the commits reachable by the provided `Snapshot`
    /// from the newest to oldest commit.
    pub fn commits(
        &self,
        snapshot: &Snapshot,
    ) -> impl Iterator<Item = Result<Commit, FjallStorageErr>> {
        let log = self._log();

        snapshot.iter().flat_map(move |entry| {
            // the snapshot range is in the form `low..=high` but the log orders
            // LSNs in reverse. thus we need to flip the range when passing it
            // down to the underlying scan.
            let low = entry.start_ref();
            let high = entry.end_ref();
            let range = high..=low;
            log.range(range).map_ok(|(_, commit)| Ok(commit))
        })
    }

    /// Produce an iterator of `SegmentIdx`s along with the pages we need from the segment.
    /// Collectively provides full coverage of the pages visible to a snapshot.
    pub fn iter_visible_pages(
        &self,
        snapshot: &Snapshot,
    ) -> impl Iterator<Item = Result<(SegmentIdx, PageSet), FjallStorageErr>> {
        // the set of pages we are searching for.
        // we remove pages from this set as we iterate through commits.
        let mut pages = PageSet::FULL;
        // we keep track of the smallest page count as we iterate through commits
        let mut page_count = PageCount::MAX;

        self.commits(snapshot).try_filter_map(move |commit| {
            // if we have found all pages, we are done
            if pages.is_empty() {
                return Ok(None);
            }

            // if we encounter a smaller commit on our travels, we need to shrink
            // the page_count to ensure that truncation is respected
            if commit.page_count < page_count {
                page_count = commit.page_count;
                pages.truncate(page_count);
            }

            if let Some(idx) = commit.segment_idx {
                let mut commit_pages = idx.pageset.clone();

                if commit_pages.last().map(|idx| idx.pages()) > Some(page_count) {
                    // truncate any pages in this commit that extend beyond the page count
                    commit_pages.truncate(page_count);
                }

                // figure out which pages we need from this commit
                let outstanding = pages.cut(&commit_pages);

                if !outstanding.is_empty() {
                    return Ok(Some((idx, outstanding)));
                }
            }

            Ok(None)
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
        self._log()
            .range_keys(range)
            .map_ok(|key| Ok(key.lsn()))
            .collect()
    }

    pub fn search_page(
        &self,
        snapshot: &Snapshot,
        pageidx: PageIdx,
    ) -> Result<Option<Commit>, FjallStorageErr> {
        let mut commits = self.commits(snapshot);

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
        self._pages().contains(&PageKey::new(sid, pageidx))
    }

    pub fn read_page(
        &self,
        sid: SegmentId,
        pageidx: PageIdx,
    ) -> Result<Option<Page>, FjallStorageErr> {
        self._pages()
            .get_owned(PageKey::new(sid, pageidx))
            .or_into_ctx()
    }

    pub fn page_count(
        &self,
        vid: &VolumeId,
        lsn: LSN,
    ) -> Result<Option<PageCount>, FjallStorageErr> {
        Ok(self.get_commit(vid, lsn)?.map(|c| c.page_count()))
    }

    pub fn checkpoints(
        &self,
        vid: &VolumeId,
    ) -> Result<Option<CachedCheckpoints>, FjallStorageErr> {
        self._checkpoints().get(vid)
    }

    pub fn checksum(&self, snapshot: &Snapshot) -> Result<Checksum, FjallStorageErr> {
        let pages = self._pages();
        let mut builder = ChecksumBuilder::new();
        let mut iter = self.iter_visible_pages(snapshot);
        while let Some((idx, pageset)) = iter.try_next()? {
            for pageidx in pageset.iter() {
                let key = PageKey::new(idx.sid.clone(), pageidx);
                if let Some(page) = pages.get(&key)? {
                    builder.write(&page);
                }
            }
        }
        Ok(builder.build())
    }

    pub fn find_missing_frames(
        &self,
        snapshot: &Snapshot,
    ) -> Result<Vec<SegmentRangeRef>, FjallStorageErr> {
        let mut missing_frames = vec![];
        let pages = self._pages();
        let mut iter = self.iter_visible_pages(snapshot);
        while let Some((idx, pageset)) = iter.try_next()? {
            // find candidate frames (intersects with the visible pageset)
            let frames = idx.iter_frames(|pages| pageset.contains_any(pages));

            // find frames for which we are missing the first page.
            // since we always download entire segment frames, if we are missing
            // the first page, we are missing all the pages (in the frame)
            for frame in frames {
                if let Some(first_page) = frame.pageset.first()
                    && !pages.contains(&PageKey::new(frame.sid.clone(), first_page))?
                {
                    missing_frames.push(frame);
                }
            }
        }
        Ok(missing_frames)
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
    pub fn write_tag(&mut self, tag: &str, graft: VolumeId) {
        self.batch
            .insert_typed(&self.storage.tags, tag.into(), graft);
    }

    pub fn write_commit(&mut self, commit: Commit) {
        self.batch
            .insert_typed(&self.storage.log, commit.vref(), commit);
    }

    pub fn write_graft(&mut self, graft: Graft) {
        self.batch
            .insert_typed(&self.storage.grafts, graft.local.clone(), graft);
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

    /// Creates a new graft with the specified local and remote `VolumeId`'s and
    /// assigns the result to the specified tag.
    pub fn new_graft(
        self,
        tag: &str,
        local: VolumeId,
        remote: VolumeId,
    ) -> Result<Graft, FjallStorageErr> {
        // if the remote exists, set the sync point to start from the latest
        // remote lsn
        let sync = self
            .read
            .latest_lsn(&remote)?
            .map(|latest_remote| SyncPoint {
                remote: latest_remote,
                local_watermark: None,
            });

        let graft = Graft::new(local.clone(), remote, sync, None);

        let mut batch = self.storage().batch();
        batch.write_tag(tag, local);
        batch.write_graft(graft.clone());
        batch.commit()?;

        tracing::debug!(
            tag,
            local_vid = ?graft.local,
            remote_vid = ?graft.remote,
            "clone graft"
        );

        Ok(graft)
    }

    pub fn get_or_create_tag(self, tag: &str) -> Result<Graft, FjallStorageErr> {
        if let Some(state) = self.read.get_tag(tag)? {
            Ok(state)
        } else {
            self.new_graft(tag, VolumeId::random(), VolumeId::random())
        }
    }

    pub fn switch_graft(
        self,
        tag: &str,
        graft_vid: VolumeId,
        remote: Option<VolumeId>,
    ) -> Result<Graft, FjallStorageErr> {
        if let Some(graft) = self.read._grafts().get(&graft_vid)? {
            if let Some(remote) = remote
                && graft.remote != remote
            {
                return Err(LogicalErr::GraftRemoteMismatch {
                    graft: graft.local,
                    expected: remote,
                    actual: graft.remote,
                }
                .into());
            }
            self.storage().tags.insert(tag.into(), graft_vid)?;
            Ok(graft)
        } else {
            self.new_graft(tag, graft_vid, remote.unwrap_or_else(VolumeId::random))
        }
    }

    pub fn commit(
        self,
        graft: &VolumeId,
        snapshot: Snapshot,
        page_count: PageCount,
        segment: SegmentIdx,
    ) -> Result<Snapshot, FjallStorageErr> {
        // Verify that the commit was constructed using the latest snapshot for
        // the volume.
        if !self.read.is_latest_snapshot(graft, &snapshot)? {
            return Err(LogicalErr::GraftConcurrentWrite(graft.clone()).into());
        }

        let graft = self.read.graft(graft)?;

        // the commit_lsn is the next lsn for the graft's local volume
        let commit_lsn = self
            .read
            .latest_lsn(&graft.local)?
            .map_or(LSN::FIRST, |lsn| lsn.next());

        tracing::debug!(vid=?graft.local, %commit_lsn, "local commit");

        let commit = Commit::new(graft.local.clone(), commit_lsn, page_count)
            .with_segment_idx(Some(segment));

        // write the commit to storage
        self.read.storage.log.insert(commit.vref(), commit)?;

        // open a new ReadGuard to read an updated graft snapshot
        ReadGuard::open(self.storage()).snapshot(&graft.local)
    }

    pub fn remote_commit_prepare(
        self,
        graft: &VolumeId,
        pending_commit: PendingCommit,
    ) -> Result<(), FjallStorageErr> {
        let graft = self.read.graft(graft)?;

        assert!(
            graft.pending_commit().is_none(),
            "BUG: pending commit is present"
        );

        // ensure LSN monotonicity
        if let Some(local_watermark) = graft.local_watermark() {
            assert!(
                local_watermark < pending_commit.local,
                "BUG: local_watermark monotonicity violation"
            );
        }
        let latest_remote = self.read.latest_lsn(&graft.remote)?;
        assert_eq!(
            latest_remote,
            pending_commit.commit.checked_prev(),
            "BUG: remote lsn monotonicity violation"
        );

        // remember to set the commit hash
        assert!(pending_commit.commit_hash != CommitHash::ZERO);

        // save the new pending commit
        let graft = graft.with_pending_commit(Some(pending_commit));
        self.storage().grafts.insert(graft.local.clone(), graft)?;

        Ok(())
    }

    pub fn remote_commit_success(
        self,
        graft: &VolumeId,
        remote_commit: Commit,
    ) -> Result<(), FjallStorageErr> {
        let graft = self.read.graft(graft)?;

        // verify the pending commit matches the remote commit
        let pending_commit = graft.pending_commit.unwrap();
        assert_eq!(remote_commit.lsn(), pending_commit.commit);
        assert_eq!(
            remote_commit.commit_hash(),
            Some(&pending_commit.commit_hash)
        );

        // fail if we somehow already know about this commit locally
        assert!(
            !self.read._log().contains(&remote_commit.vref())?,
            "BUG: remote commit already exists"
        );

        // update the graft with the new sync points and no pending_commit
        let updated_graft = Graft {
            sync: Some(pending_commit.into()),
            pending_commit: None,
            ..graft
        };

        let mut batch = self.storage().batch();
        batch.write_commit(remote_commit);
        batch.write_graft(updated_graft);
        batch.commit()
    }

    pub fn drop_pending_commit(self, graft: &VolumeId) -> Result<(), FjallStorageErr> {
        let graft = self.read.graft(graft)?;
        self.storage()
            .grafts
            .insert(graft.local.clone(), graft.with_pending_commit(None))
    }

    pub fn sync_remote_to_local(self, graft: VolumeId) -> Result<(), FjallStorageErr> {
        let graft = self.read.graft(&graft)?;

        // check to see if we have any changes to sync
        let latest_remote = self.read.latest_lsn(&graft.remote).or_into_ctx()?;
        let Some(remote_changes) = graft.remote_changes(latest_remote) else {
            // nothing to sync
            return Ok(());
        };

        // check for divergence
        let latest_local = self.read.latest_lsn(&graft.local).or_into_ctx()?;
        if graft.local_changes(latest_local).is_some() {
            // the remote and local volumes have diverged
            let status = graft.status(latest_local, latest_remote);
            tracing::debug!("graft {} has diverged; status=`{status}`", graft.local);
            return Err(LogicalErr::GraftDiverged(graft.local).into());
        }

        tracing::debug!(
            sync = ?graft.sync(),
            lsns = %remote_changes.to_string(),
            remote = ?graft.remote,
            local = ?graft.local,
            "syncing commits from remote to local volume"
        );

        // save the remote lsn for later
        let remote_lsn = *remote_changes.end();

        // construct an iterator of new local lsns
        // note: this iterator must return new local lsns in reverse as the
        // commits iterator returns commits from newest to oldest
        let num_commits = remote_changes.len();
        let local_first = latest_local.map_or(LSN::FIRST, |l| l.next());
        let local_last = local_first
            .checked_add(num_commits - 1)
            .expect("LSN overflow");
        let mut new_local_lsns = (local_first..=local_last).iter().rev();

        // iterate missing remote commits, and commit them to the local volume
        let search = Snapshot::new(graft.remote.clone(), remote_changes);
        let mut batch = self.storage().batch();
        let mut commits = self.read.commits(&search);
        while let Some(commit) = commits.try_next().or_into_ctx()? {
            let next_lsn = new_local_lsns
                .next()
                .expect("BUG: storage has more commits than expected");
            // map the remote commit into the local volume
            batch.write_commit(commit.with_vid(graft.local.clone()).with_lsn(next_lsn));
        }

        assert!(
            new_local_lsns.next().is_none(),
            "BUG: not all new local lsns were used"
        );

        // update the sync point
        batch.write_graft(graft.with_sync(Some(SyncPoint {
            local_watermark: Some(local_last),
            remote: remote_lsn,
        })));

        // commit the batch
        batch.commit()
    }
}
