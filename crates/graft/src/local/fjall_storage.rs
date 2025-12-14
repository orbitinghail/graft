use std::{fmt::Debug, ops::RangeInclusive, path::Path, sync::Arc};

use crate::{
    core::{
        LogId, PageCount, PageIdx, SegmentId, VolumeId,
        checksum::{Checksum, ChecksumBuilder},
        commit::{Commit, SegmentIdx, SegmentRangeRef},
        commit_hash::CommitHash,
        logref::LogRef,
        lsn::{LSN, LSNRangeExt, LSNSet},
        page::Page,
        pageset::PageSet,
    },
    local::fjall_storage::{
        fjall_typed::{ReadableExt, TypedIter, TypedKeyspace, TypedValIter, WriteBatchExt},
        keys::PageVersion,
    },
};
use bytestring::ByteString;
use fjall::{Database, KeyspaceCreateOptions, KvSeparationOptions, OwnedWriteBatch};
use parking_lot::{Mutex, MutexGuard};
use thin_vec::thin_vec;
use tryiter::TryIteratorExt;

use crate::{
    LogicalErr,
    local::fjall_storage::keys::PageKey,
    snapshot::Snapshot,
    volume::{PendingCommit, SyncPoint, Volume},
};

mod fjall_repr;
mod fjall_typed;
mod keys;
mod values;

#[derive(Debug, thiserror::Error)]
pub enum FjallStorageErr {
    #[error("Fjall error: {0}")]
    FjallErr(#[from] fjall::Error),

    #[error("Failed to decode key: {0}")]
    DecodeErr(#[from] fjall_repr::DecodeErr),

    #[error("I/O Error: {0}")]
    IoErr(#[from] std::io::Error),

    #[error("batch commit precondition failed")]
    BatchPreconditionErr,

    #[error(transparent)]
    LogicalErr(#[from] LogicalErr),
}

struct Keyspaces {
    /// This keyspace maps tags to volumes
    tags: TypedKeyspace<ByteString, VolumeId>,

    /// This keyspace stores state regarding each `Volume`
    /// keyed by its `VolumeId`
    volumes: TypedKeyspace<VolumeId, Volume>,

    /// This keyspace is an index tracking which LSNs are checkpoints.
    checkpoints: TypedKeyspace<LogRef, ()>,

    /// This keyspace stores commits
    log: TypedKeyspace<LogRef, Commit>,

    /// This keyspace is an index mapping pages to the latest commit that
    /// modified said page.
    page_versions: TypedKeyspace<PageVersion, ()>,

    /// This keyspace stores Pages
    pages: TypedKeyspace<PageKey, Page>,
}

impl Keyspaces {
    fn open(db: &fjall::Database) -> Result<Self, FjallStorageErr> {
        Ok(Self {
            tags: TypedKeyspace::open(db, "tags", Default::default)?,
            volumes: TypedKeyspace::open(db, "volumes", Default::default)?,
            checkpoints: TypedKeyspace::open(db, "checkpoints", Default::default)?,
            log: TypedKeyspace::open(db, "log", Default::default)?,
            page_versions: TypedKeyspace::open(db, "page_versions", Default::default)?,
            pages: TypedKeyspace::open(db, "pages", || {
                KeyspaceCreateOptions::default()
                    .with_kv_separation(Some(KvSeparationOptions::default()))
            })?,
        })
    }
}

pub struct FjallStorage {
    db: fjall::Database,
    ks: Keyspaces,

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
        Self::open_from_builder(Database::builder(path))
    }

    pub fn open_temporary() -> Result<Self, FjallStorageErr> {
        let path = tempfile::tempdir()?.keep();
        Self::open_from_builder(Database::builder(path).temporary(true))
    }

    fn open_from_builder(
        builder: fjall::DatabaseBuilder<Database>,
    ) -> Result<Self, FjallStorageErr> {
        let db = builder.open()?;
        let ks = Keyspaces::open(&db)?;
        Ok(Self { db, ks, lock: Default::default() })
    }

    pub(crate) fn read(&self) -> ReadGuard<'_> {
        ReadGuard::open(self)
    }

    pub(crate) fn batch(&self) -> WriteBatch<'_> {
        WriteBatch::open(self)
    }

    /// Open a read + write txn on storage.
    /// The returned object holds a lock, any subsequent calls to `read_write`
    /// will block.
    pub(crate) fn read_write(&self) -> ReadWriteGuard<'_> {
        ReadWriteGuard::open(self)
    }

    pub fn write_page(
        &self,
        sid: SegmentId,
        pageidx: PageIdx,
        page: Page,
    ) -> Result<(), FjallStorageErr> {
        self.ks.pages.insert(PageKey::new(sid, pageidx), page)
    }

    pub fn remove_page(&self, sid: SegmentId, pageidx: PageIdx) -> Result<(), FjallStorageErr> {
        self.ks.pages.remove(PageKey::new(sid, pageidx))
    }

    pub fn remove_page_range(
        &self,
        sid: &SegmentId,
        pages: RangeInclusive<PageIdx>,
    ) -> Result<(), FjallStorageErr> {
        // PageKeys are stored in descending order
        let keyrange =
            PageKey::new(sid.clone(), *pages.end())..=PageKey::new(sid.clone(), *pages.start());
        let mut batch = self.db.batch();
        let mut iter = self.db.snapshot().range(&self.ks.pages, keyrange);
        while let Some((key, _)) = iter.try_next()? {
            batch.remove_typed(&self.ks.pages, key);
        }
        batch.commit()?;
        Ok(())
    }

    pub fn tag_delete(&self, tag: &str) -> Result<(), FjallStorageErr> {
        self.ks.tags.remove(tag.into())
    }

    pub fn volume_delete(&self, vid: &VolumeId) -> Result<(), FjallStorageErr> {
        self.ks.volumes.remove(vid.clone())
    }

    pub fn volume_from_snapshot(&self, snapshot: &Snapshot) -> Result<Volume, FjallStorageErr> {
        let volume = Volume::new_random();
        let commits = self
            .read()
            .commits(snapshot)
            .collect::<Result<Vec<_>, _>>()?;
        let mut lsn = LSN::FIRST.checked_add(commits.len() as u64).unwrap();
        let mut batch = self.batch();
        for commit in commits {
            lsn = lsn.checked_prev().unwrap();
            batch.write_commit(commit.with_log_id(volume.local.clone()).with_lsn(lsn));
        }
        batch.write_volume(volume.clone());
        batch.commit()?;
        Ok(volume)
    }
}

pub struct ReadGuard<'a> {
    storage: &'a FjallStorage,
    snapshot: fjall::Snapshot,
}

impl<'a> ReadGuard<'a> {
    fn open(storage: &'a FjallStorage) -> Self {
        let snapshot = storage.db.snapshot();
        Self { storage, snapshot }
    }

    fn ks(&self) -> &'a Keyspaces {
        &self.storage.ks
    }

    pub fn iter_tags(&self) -> TypedIter<ByteString, VolumeId> {
        self.snapshot.iter(&self.ks().tags)
    }

    pub fn tag_exists(&self, tag: &str) -> Result<bool, FjallStorageErr> {
        self.snapshot.contains_key(&self.ks().tags, tag)
    }

    pub fn get_tag(&self, tag: &str) -> Result<Option<VolumeId>, FjallStorageErr> {
        self.snapshot.get(&self.ks().tags, tag)
    }

    /// Lookup the latest LSN for a Log
    pub fn latest_lsn(&self, log: &LogId) -> Result<Option<LSN>, FjallStorageErr> {
        Ok(self
            .snapshot
            .prefix(&self.ks().log, log)
            .keys()
            .try_next()?
            .map(|logref| logref.lsn))
    }

    /// Retrieve the LSN of the most recent checkpoint as of the provided LSN.
    pub fn checkpoint_for(&self, log: &LogId, lsn: LSN) -> Result<Option<LSN>, FjallStorageErr> {
        // The checkpoint index orders LSNs in reverse, thus we need to search
        // from the provided LSN back to the first LSN
        let high = LogRef::new(log.clone(), lsn);
        let low = LogRef::new(log.clone(), LSN::FIRST);
        Ok(self
            .snapshot
            .range(&self.ks().checkpoints, high..=low)
            .keys()
            .try_next()?
            .map(|lr| lr.lsn))
    }

    pub fn iter_volumes(&self) -> TypedValIter<VolumeId, Volume> {
        self.snapshot.iter(&self.ks().volumes).values()
    }

    pub fn volume_exists(&self, vid: &VolumeId) -> Result<bool, FjallStorageErr> {
        self.snapshot.contains_key(&self.ks().volumes, vid)
    }

    pub fn volume(&self, vid: &VolumeId) -> Result<Volume, FjallStorageErr> {
        self.snapshot
            .get(&self.ks().volumes, vid)?
            .ok_or_else(|| LogicalErr::VolumeNotFound(vid.clone()).into())
    }

    /// Check if the provided Snapshot is logically equal to the latest snapshot
    /// for the specified Volume.
    pub fn is_latest_snapshot(
        &self,
        vid: &VolumeId,
        snapshot: &Snapshot,
    ) -> Result<bool, FjallStorageErr> {
        let volume = self.volume(vid)?;
        let latest_local = self.latest_lsn(&volume.local)?;

        // The complexity here is that the snapshot may have been taken before
        // we pushed commits to a remote. When this happens, the snapshot will
        // be physically different but logically equivalent. We can use the
        // relationship setup by the SyncPoint to handle this case.
        Ok(match snapshot.head() {
            Some((log, lsn)) if log == &volume.local => Some(lsn) == latest_local,

            Some((log, lsn)) if log == &volume.remote => {
                if let Some(sync) = volume.sync {
                    lsn == sync.remote && sync.local_watermark == latest_local
                } else {
                    // if volume has no sync point, then a snapshot should not
                    // include a remote layer, thus this snapshot is from
                    // another volume
                    false
                }
            }

            // Snapshot from another volume
            Some(_) => false,

            // Snapshot is empty
            None => latest_local.is_none() && volume.sync().is_none(),
        })
    }

    /// Load the most recent Snapshot for a Volume.
    pub fn snapshot(&self, vid: &VolumeId) -> Result<Snapshot, FjallStorageErr> {
        let volume = self.volume(vid)?;

        let mut snapshot = Snapshot::default();

        let local_range = if let Some(latest) = self.latest_lsn(&volume.local)? {
            if let Some(watermark) = volume.sync().and_then(|s| s.local_watermark)
                && watermark < latest
            {
                Some(watermark..=latest)
            } else {
                Some(LSN::FIRST..=latest)
            }
        } else {
            None
        };

        if let Some(range) = local_range {
            if let Some(checkpoint) = self.checkpoint_for(&volume.local, *range.end())?
                && range.contains(&checkpoint)
            {
                snapshot.append(volume.local, checkpoint..=*range.end());
                // checkpoint found, early exit
                return Ok(snapshot);
            } else {
                snapshot.append(volume.local, range);
            }
        }

        if let Some(remote) = volume.sync.map(|s| s.remote) {
            if let Some(checkpoint) = self.checkpoint_for(&volume.remote, remote)? {
                snapshot.append(volume.remote, checkpoint..=remote);
            } else {
                snapshot.append(volume.remote, LSN::FIRST..=remote);
            }
        }

        Ok(snapshot)
    }

    /// Retrieve a specific commit
    pub fn get_commit(&self, log: &LogId, lsn: LSN) -> Result<Option<Commit>, FjallStorageErr> {
        self.snapshot
            .get_owned(&self.ks().log, LogRef::new(log.clone(), lsn))
    }

    /// Iterates through all of the commits reachable by the provided `Snapshot`
    /// from the newest to oldest commit.
    pub fn commits(
        &self,
        snapshot: &Snapshot,
    ) -> impl Iterator<Item = Result<Commit, FjallStorageErr>> {
        snapshot.iter().flat_map(move |entry| {
            // the snapshot range is in the form `low..=high` but the log orders
            // LSNs in reverse. thus we need to flip the range when passing it
            // down to the underlying scan.
            let low = entry.start_ref();
            let high = entry.end_ref();
            let range = high..=low;
            self.snapshot.range(&self.ks().log, range).values()
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

    /// Given a range of LSNs for a particular Log, returns the set of LSNs we have
    pub fn lsns(&self, log: &LogId, lsns: &RangeInclusive<LSN>) -> Result<LSNSet, FjallStorageErr> {
        // lsns is in the form `low..=high` but the log orders
        // LSNs in reverse. thus we need to flip the range
        let low = LogRef::new(log.clone(), *lsns.start());
        let high = LogRef::new(log.clone(), *lsns.end());
        let range = high..=low;
        self.snapshot
            .range(&self.ks().log, range)
            .keys()
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
        self.snapshot
            .contains_key(&self.ks().pages, &PageKey::new(sid, pageidx))
    }

    pub fn read_page(
        &self,
        sid: SegmentId,
        pageidx: PageIdx,
    ) -> Result<Option<Page>, FjallStorageErr> {
        self.snapshot
            .get_owned(&self.ks().pages, PageKey::new(sid, pageidx))
    }

    /// Retrieve the `PageCount` of a Volume at a particular LSN.
    pub fn page_count(&self, log: &LogId, lsn: LSN) -> Result<Option<PageCount>, FjallStorageErr> {
        Ok(self.get_commit(log, lsn)?.map(|c| c.page_count()))
    }

    pub fn checksum(&self, snapshot: &Snapshot) -> Result<Checksum, FjallStorageErr> {
        let mut builder = ChecksumBuilder::new();
        let mut iter = self.iter_visible_pages(snapshot);
        while let Some((idx, pageset)) = iter.try_next()? {
            for pageidx in pageset.iter() {
                let key = PageKey::new(idx.sid.clone(), pageidx);
                if let Some(page) = self.snapshot.get(&self.ks().pages, &key)? {
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
        let mut iter = self.iter_visible_pages(snapshot);
        while let Some((idx, pageset)) = iter.try_next()? {
            // find candidate frames (intersects with the visible pageset)
            let frames = idx.iter_frames(|pages| pageset.contains_any(pages));

            // find frames for which we are missing the first page.
            // since we always download entire segment frames, if we are missing
            // the first page, we are missing all the pages (in the frame)
            for frame in frames {
                if let Some(first_page) = frame.pageset.first()
                    && !self.snapshot.contains_key(
                        &self.ks().pages,
                        &PageKey::new(frame.sid.clone(), first_page),
                    )?
                {
                    missing_frames.push(frame);
                }
            }
        }
        Ok(missing_frames)
    }
}

pub struct WriteBatch<'a> {
    ks: &'a Keyspaces,
    batch: OwnedWriteBatch,
}

impl<'a> WriteBatch<'a> {
    fn open(storage: &'a FjallStorage) -> Self {
        let ks = &storage.ks;
        let batch = storage.db.batch();
        Self { ks, batch }
    }

    pub fn write_tag(&mut self, tag: &str, vid: VolumeId) {
        self.batch.insert_typed(&self.ks.tags, tag.into(), vid);
    }

    pub fn write_commit(&mut self, commit: Commit) {
        // keep the checkpoint index up to date
        for &checkpoint in commit.checkpoints() {
            self.batch.insert_typed(
                &self.ks.checkpoints,
                LogRef::new(commit.log.clone(), checkpoint),
                (),
            );
        }

        // keep the page version index up to date
        if let Some(segment_idx) = commit.segment_idx() {
            for pageidx in segment_idx.pageset.iter() {
                self.batch.insert_typed(
                    &self.ks.page_versions,
                    PageVersion::new(commit.log.clone(), pageidx, commit.lsn),
                    (),
                );
            }
        }

        self.batch
            .insert_typed(&self.ks.log, commit.logref(), commit);
    }

    pub fn write_volume(&mut self, volume: Volume) {
        self.batch
            .insert_typed(&self.ks.volumes, volume.vid.clone(), volume);
    }

    pub fn write_page(&mut self, sid: SegmentId, pageidx: PageIdx, page: Page) {
        self.batch
            .insert_typed(&self.ks.pages, PageKey::new(sid, pageidx), page);
    }

    pub fn commit(self) -> Result<(), FjallStorageErr> {
        Ok(self.batch.commit()?)
    }
}

pub struct ReadWriteGuard<'a> {
    _permit: MutexGuard<'a, ()>,
    read: ReadGuard<'a>,
}

impl<'a> ReadWriteGuard<'a> {
    fn open(storage: &'a FjallStorage) -> Self {
        // TODO: consider adding some kind of deadlock detection
        let _permit = storage.lock.lock();
        // IMPORTANT: take the read snapshot after taking the lock
        let read = storage.read();
        Self { _permit, read }
    }

    fn ks(&self) -> &'a Keyspaces {
        self.read.ks()
    }

    pub fn tag_replace(
        self,
        tag: &str,
        vid: VolumeId,
    ) -> Result<Option<VolumeId>, FjallStorageErr> {
        let out = self.read.get_tag(tag)?;
        self.ks().tags.insert(tag.into(), vid)?;
        Ok(out)
    }

    /// opens a volume. if any id is missing, it will be randomly
    /// generated. If the volume already exists, this function will fail if its
    /// remote Log doesn't match.
    pub fn volume_open(
        self,
        vid: Option<VolumeId>,
        local: Option<LogId>,
        remote: Option<LogId>,
    ) -> Result<Volume, FjallStorageErr> {
        // generate the local LogId if it's not specified
        let vid = vid.unwrap_or_else(VolumeId::random);

        // lookup the volume if specified
        if let Some(volume) = self.read.snapshot.get(&self.ks().volumes, &vid)? {
            if let Some(remote) = remote
                && volume.remote != remote
            {
                return Err(LogicalErr::VolumeRemoteMismatch {
                    vid: volume.vid,
                    expected: remote,
                    actual: volume.remote,
                }
                .into());
            }
            return Ok(volume);
        }

        // determine the local and remote LogIds
        let local = local.unwrap_or_else(LogId::random);
        let remote = remote.unwrap_or_else(LogId::random);

        // if the remote exists, set the sync point to start from the latest
        // remote lsn
        let sync = self
            .read
            .latest_lsn(&remote)?
            .map(|latest_remote| SyncPoint {
                remote: latest_remote,
                local_watermark: None,
            });

        // create the new volume
        let volume = Volume::new(vid.clone(), local, remote, sync, None);
        self.ks().volumes.insert(vid, volume.clone())?;

        tracing::debug!(
            vid = ?volume.vid,
            local_log = ?volume.local,
            remote_log = ?volume.remote,
            "open volume"
        );

        Ok(volume)
    }

    /// Attempt to execute a local commit to the specified Volume's local Log.
    ///
    /// Returns the resulting `Snapshot` on success
    pub fn commit(
        self,
        vid: &VolumeId,
        snapshot: Snapshot,
        page_count: PageCount,
        segment: SegmentIdx,
    ) -> Result<Snapshot, FjallStorageErr> {
        // Verify that the commit was constructed using the latest snapshot for
        // the volume.
        if !self.read.is_latest_snapshot(vid, &snapshot)? {
            return Err(LogicalErr::VolumeConcurrentWrite(vid.clone()).into());
        }

        let volume = self.read.volume(vid)?;

        // the commit_lsn is the next lsn for the volume's local Log
        let commit_lsn = self
            .read
            .latest_lsn(&volume.local)?
            .map_or(LSN::FIRST, |lsn| lsn.next());

        let maybe_checkpoint = if page_count == segment.page_count() {
            thin_vec![commit_lsn]
        } else {
            thin_vec![]
        };

        tracing::debug!(vid=?volume.vid, log=?volume.local, %commit_lsn, "local commit");

        let commit = Commit::new(volume.local.clone(), commit_lsn, page_count)
            .with_checkpoints(maybe_checkpoint)
            .with_segment_idx(Some(segment));

        // write the commit to storage using a batch to
        // ensure indexes are updated
        let mut batch = self.read.storage.batch();
        batch.write_commit(commit);
        batch.commit()?;

        // open a new ReadGuard to read an updated snapshot
        // since we are holding a read_write lock, we know that no other thread
        // is concurrently committing to the volume, so we know this snapshot
        // will reflect the commit we just executed
        self.read.storage.read().snapshot(&volume.vid)
    }

    /// Verify we are ready to make a remote commit and update the volume
    /// with a `PendingCommit`
    pub fn remote_commit_prepare(
        self,
        vid: &VolumeId,
        pending_commit: PendingCommit,
    ) -> Result<(), FjallStorageErr> {
        let volume = self.read.volume(vid)?;

        assert!(
            volume.pending_commit().is_none(),
            "BUG: pending commit is present"
        );

        // ensure LSN monotonicity
        if let Some(local_watermark) = volume.local_watermark() {
            assert!(
                local_watermark < pending_commit.local,
                "BUG: local_watermark monotonicity violation"
            );
        }
        let latest_remote = self.read.latest_lsn(&volume.remote)?;
        assert_eq!(
            latest_remote,
            pending_commit.commit.checked_prev(),
            "BUG: remote lsn monotonicity violation"
        );

        // remember to set the commit hash
        assert!(pending_commit.commit_hash != CommitHash::ZERO);

        // save the new pending commit
        let volume = volume.with_pending_commit(Some(pending_commit));
        self.ks().volumes.insert(volume.vid.clone(), volume)?;

        Ok(())
    }

    /// Finish the remote commit process by writing out an updated volume
    /// and recording the remote commit locally
    pub fn remote_commit_success(
        &self,
        vid: &VolumeId,
        remote_commit: Commit,
    ) -> Result<(), FjallStorageErr> {
        let volume = self.read.volume(vid)?;

        // verify the pending commit matches the remote commit
        let pending_commit = volume.pending_commit.unwrap();
        assert_eq!(remote_commit.lsn(), pending_commit.commit);
        assert_eq!(
            remote_commit.commit_hash(),
            Some(&pending_commit.commit_hash)
        );

        // if we already know about this remote commit (which can happen during
        // recovery), verify that the commit we have is the same as the remote
        if let Some(existing_remote) = self
            .read
            .snapshot
            .get_owned(&self.ks().log, remote_commit.logref())?
        {
            assert_eq!(
                existing_remote.commit_hash, remote_commit.commit_hash,
                "BUG: remote commit mismatch"
            );
        }

        // update the volume with the new sync points and no pending_commit
        let volume = Volume {
            sync: Some(pending_commit.into()),
            pending_commit: None,
            ..volume
        };

        let mut batch = self.read.storage.batch();
        batch.write_commit(remote_commit);
        batch.write_volume(volume);
        batch.commit()
    }

    /// Drop a pending commit without applying it. This should only be called
    /// after receiving a rejection from the remote.
    pub fn drop_pending_commit(&self, vid: &VolumeId) -> Result<(), FjallStorageErr> {
        let volume = self.read.volume(vid)?;
        self.ks()
            .volumes
            .insert(volume.vid.clone(), volume.with_pending_commit(None))
    }

    /// Attempt to recover a pending commit by checking to see if it's included in the remote log.
    /// There are three outcomes:
    /// 1. the remote log contains a commit with the pending LSN and commit hash -> `remote_commit_success`
    /// 2. the remote log contains a commit with the pending LSN and different commit hash -> `drop_pending_commit`
    /// 3. the remote log doesn't contain a commit with the pending LSN -> `drop_pending_commit`
    ///
    /// Notably, this function ALWAYS drops the pending commit. So make sure you fetch the log before calling this function
    pub fn recover_pending_commit(self, vid: &VolumeId) -> Result<(), FjallStorageErr> {
        let volume = self.read.volume(vid)?;
        if let Some(pending) = volume.pending_commit {
            tracing::debug!(?pending, "attempting to recover pending commit");

            match self.read.get_commit(&volume.remote, pending.commit)? {
                Some(commit) if commit.commit_hash() == Some(&pending.commit_hash) => {
                    // case 1: remote contains the commit
                    #[cfg(feature = "precept")]
                    precept::expect_reachable!("recover pending commit: success", { "vid": vid });
                    self.remote_commit_success(&volume.vid, commit)?;
                    tracing::debug!("recovery success");
                    Ok(())
                }
                Some(commit) => {
                    // Case 2: remote contains a different commit
                    #[cfg(feature = "precept")]
                    precept::expect_reachable!("recover pending commit: diverged", { "vid": vid });
                    self.drop_pending_commit(&volume.vid)?;
                    tracing::warn!(
                        "pending commit recovery failed for volume {}, commit {}/{} already exists with different hash: {:?}",
                        volume.vid,
                        volume.remote,
                        pending.commit,
                        commit.commit_hash
                    );
                    Err(LogicalErr::VolumeDiverged(volume.vid).into())
                }
                None => {
                    // Case 3: remote doesn't contain the commit
                    #[cfg(feature = "precept")]
                    precept::expect_reachable!("recover pending commit: push failed", { "vid": vid });
                    self.drop_pending_commit(&volume.vid)?;
                    tracing::debug!(
                        "recovered from failed push; dropped uncommitted pending commit"
                    );
                    Ok(())
                }
            }
        } else {
            // recovery not needed
            Ok(())
        }
    }

    pub fn sync_remote_to_local(self, vid: VolumeId) -> Result<(), FjallStorageErr> {
        let volume = self.read.volume(&vid)?;

        // check to see if we have any changes to sync
        let latest_remote = self.read.latest_lsn(&volume.remote)?;
        let Some(remote_changes) = volume.remote_changes(latest_remote) else {
            // nothing to sync
            return Ok(());
        };

        // check for divergence
        let latest_local = self.read.latest_lsn(&volume.local)?;
        if volume.local_changes(latest_local).is_some() {
            // the remote and local logs have diverged
            let status = volume.status(latest_local, latest_remote);
            tracing::debug!("volume {} has diverged; status=`{status}`", volume.vid);
            return Err(LogicalErr::VolumeDiverged(volume.vid).into());
        }

        tracing::debug!(
            vid = ?volume.vid,
            sync = ?volume.sync(),
            lsns = %remote_changes.to_string(),
            local = ?volume.local,
            remote = ?volume.remote,
            "fast-forwarding volume"
        );

        // to perform the sync, we simply need to update the volume's SyncPoint
        // to reference the latest remote_lsn
        let remote_lsn = *remote_changes.end();

        let new_sync = match volume.sync() {
            Some(sync) => {
                assert!(
                    remote_lsn > sync.remote,
                    "BUG: attempt to sync volume to older version of the remote"
                );
                SyncPoint {
                    remote: remote_lsn,
                    local_watermark: sync.local_watermark,
                }
            }
            None => SyncPoint {
                remote: remote_lsn,
                local_watermark: None,
            },
        };

        // update the sync point
        self.ks()
            .volumes
            .insert(volume.vid.clone(), volume.with_sync(Some(new_sync)))
    }
}
