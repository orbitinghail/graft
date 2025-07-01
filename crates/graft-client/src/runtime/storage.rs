use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    io,
    ops::RangeInclusive,
    path::Path,
    sync::Arc,
};

use bytes::Bytes;
use changeset::ChangeSet;
use commit::CommitKey;
use culprit::{Culprit, ResultExt};
use fjall::{KvSeparationOptions, PartitionCreateOptions, Slice};
use graft_core::{
    PageIdx, VolumeId,
    byte_unit::ByteUnit,
    lsn::{LSN, LSNRangeExt},
    page::PageSizeErr,
    page_count::PageCount,
    page_idx::ConvertToPageIdxErr,
    zerocopy_ext::ZerocopyErr,
};
use memtable::Memtable;
use page::{PageKey, PageValue, PageValueConversionErr};
use parking_lot::{Mutex, MutexGuard};
use snapshot::{RemoteMapping, Snapshot};
use splinter_rs::{DecodeErr, Splinter, SplinterRead, SplinterRef, SplinterWrite};
use tracing::field;
use tryiter::{TryIterator, TryIteratorExt};
use volume_state::{
    SyncDirection, VolumeConfig, VolumeQueryIter, VolumeState, VolumeStateKey, VolumeStateTag,
    VolumeStatus, Watermark, Watermarks,
};
use zerocopy::IntoBytes;

pub mod changeset;
pub(crate) mod commit;
pub(crate) mod memtable;
pub mod page;
pub mod snapshot;
pub mod volume_state;

type Result<T> = std::result::Result<T, Culprit<StorageErr>>;

#[derive(Debug, thiserror::Error)]
pub enum StorageErr {
    #[error("fjall error: {0}")]
    FjallErr(#[from] fjall::Error),

    #[error("io error: {0}")]
    IoErr(io::ErrorKind),

    #[error("Corrupt key: {0}")]
    CorruptKey(ZerocopyErr),

    #[error("Corrupt snapshot: {0}")]
    CorruptSnapshot(ZerocopyErr),

    #[error("Corrupt volume config: {0}")]
    CorruptVolumeConfig(ZerocopyErr),

    #[error("Volume state {0:?} is corrupt: {1}")]
    CorruptVolumeState(VolumeStateTag, ZerocopyErr),

    #[error("Corrupt page: {0}")]
    CorruptPage(#[from] PageValueConversionErr),

    #[error("Corrupt commit: {0}")]
    CorruptCommit(#[from] DecodeErr),

    #[error("Illegal concurrent write to volume")]
    ConcurrentWrite,

    #[error("Volume needs recovery")]
    VolumeIsSyncing,

    #[error(
        "The local Volume state is ahead of the remote state, refusing to accept remote changes"
    )]
    RemoteConflict,

    #[error("invalid page index")]
    ConvertToPageIdxErr(#[from] ConvertToPageIdxErr),
}

impl From<io::Error> for StorageErr {
    fn from(err: io::Error) -> Self {
        StorageErr::IoErr(err.kind())
    }
}

impl From<lsm_tree::Error> for StorageErr {
    fn from(err: lsm_tree::Error) -> Self {
        StorageErr::FjallErr(err.into())
    }
}

impl From<PageSizeErr> for StorageErr {
    fn from(err: PageSizeErr) -> Self {
        StorageErr::CorruptPage(err.into())
    }
}

pub struct Storage {
    keyspace: fjall::Keyspace,

    /// Used to store volume state broken out by tag.
    /// Keyed by `VolumeStateKey`.
    ///
    /// ```text
    /// {vid}/VolumeStateTag::Config -> VolumeConfig
    /// {vid}/VolumeStateTag::Status -> VolumeStatus
    /// {vid}/VolumeStateTag::Snapshot -> Snapshot
    /// {vid}/VolumeStateTag::Watermarks -> Watermarks
    /// ```
    volumes: fjall::Partition,

    /// Used to store page contents
    /// maps from (`VolumeId`, `PageIdx`, LSN) to `PageValue`
    pages: fjall::Partition,

    /// Used to track changes made by local commits.
    /// maps from (`VolumeId`, LSN) to Graft (Splinter of changed `PageIdxs`)
    commits: fjall::Partition,

    /// Must be held while performing read+write transactions.
    /// Read-only and write-only transactions don't need to hold the lock as
    /// long as they are safe:
    /// To make read-only txns safe, always use fjall snapshots
    /// To make write-only txns safe, they must be monotonic
    commit_lock: Arc<Mutex<()>>,

    /// Used to notify subscribers of new local commits
    local_changeset: ChangeSet<VolumeId>,

    /// Used to notify subscribers of new remote commits
    remote_changeset: ChangeSet<VolumeId>,
}

impl Storage {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        tracing::debug!("opening runtime storage at {}", path.as_ref().display());
        Self::open_config(fjall::Config::new(path))
    }

    pub fn open_temporary() -> Result<Self> {
        let path = tempfile::tempdir()?.keep();
        tracing::debug!("opening temporary runtime storage at {}", path.display());
        Self::open_config(fjall::Config::new(path).temporary(true))
    }

    fn open_config(config: fjall::Config) -> Result<Self> {
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
        let storage = Storage {
            keyspace,
            volumes,
            pages,
            commits,
            commit_lock: Default::default(),
            local_changeset: Default::default(),
            remote_changeset: Default::default(),
        };
        storage.check_for_interrupted_push()?;
        Ok(storage)
    }

    fn check_for_interrupted_push(&self) -> Result<()> {
        let _permit = self.commit_lock.lock();
        let mut batch = self.keyspace.batch();
        batch = batch.durability(Some(fjall::PersistMode::SyncAll));

        let iter = self.volumes.snapshot().iter().err_into();
        let mut iter = VolumeQueryIter::new(iter);
        while let Some(state) = iter.try_next()? {
            if state.is_syncing() {
                tracing::warn!(?state, "detected interrupted push for volume");
                self.set_volume_status(&mut batch, state.vid(), VolumeStatus::InterruptedPush);
            }
        }
        Ok(batch.commit()?)
    }

    /// Access the local commit changeset. This `ChangeSet` is updated whenever a
    /// Volume receives a local commit.
    pub fn local_changeset(&self) -> &ChangeSet<VolumeId> {
        &self.local_changeset
    }

    /// Access the remote commit changeset. This `ChangeSet` is updated whenever a
    /// Volume receives a remote commit.
    pub fn remote_changeset(&self) -> &ChangeSet<VolumeId> {
        &self.remote_changeset
    }

    /// Set the specified Volume's config
    pub fn set_volume_config(&self, vid: &VolumeId, config: VolumeConfig) -> Result<()> {
        let key = VolumeStateKey::new(vid.clone(), VolumeStateTag::Config);
        Ok(self.volumes.insert(key, config)?)
    }

    /// Update a Volume's config
    pub fn update_volume_config<F>(&self, vid: &VolumeId, mut f: F) -> Result<()>
    where
        F: FnMut(VolumeConfig) -> VolumeConfig,
    {
        let _permit = self.commit_lock.lock();
        let key = VolumeStateKey::new(vid.clone(), VolumeStateTag::Config);
        let config = self
            .volumes
            .get(&key)?
            .map(|c| VolumeConfig::from_bytes(&c))
            .transpose()?
            .unwrap_or_default();
        Ok(self.volumes.insert(key, f(config))?)
    }

    fn set_volume_status(&self, batch: &mut fjall::Batch, vid: &VolumeId, status: VolumeStatus) {
        let key = VolumeStateKey::new(vid.clone(), VolumeStateTag::Status);
        batch.insert(&self.volumes, key, status)
    }

    pub fn get_volume_status(&self, vid: &VolumeId) -> Result<VolumeStatus> {
        let key = VolumeStateKey::new(vid.clone(), VolumeStateTag::Status);
        if let Some(value) = self.volumes.get(key)? {
            Ok(VolumeStatus::from_bytes(&value)?)
        } else {
            Ok(VolumeStatus::Ok)
        }
    }

    pub fn volume_state(&self, vid: &VolumeId) -> Result<VolumeState> {
        let mut state = VolumeState::new(vid.clone());
        let mut iter = self.volumes.snapshot().prefix(vid);
        while let Some((key, value)) = iter.try_next()? {
            let key = VolumeStateKey::ref_from_bytes(&key)?;
            debug_assert_eq!(key.vid(), vid, "vid mismatch");
            state.accumulate(key.tag(), value)?;
        }
        Ok(state)
    }

    pub fn snapshot(&self, vid: &VolumeId) -> Result<Option<Snapshot>> {
        let key = VolumeStateKey::new(vid.clone(), VolumeStateTag::Snapshot);
        if let Some(snapshot) = self.volumes.get(key)? {
            Ok(Some(Snapshot::try_from_bytes(&snapshot)?))
        } else {
            Ok(None)
        }
    }

    pub fn iter_volumes(&self) -> impl TryIterator<Ok = VolumeState, Err = Culprit<StorageErr>> {
        let iter = self.volumes.snapshot().iter().err_into();
        VolumeQueryIter::new(iter)
    }

    pub fn volume_exists(&self, vid: VolumeId) -> Result<bool> {
        let key = VolumeStateKey::new(vid, VolumeStateTag::Config);
        Ok(self.volumes.contains_key(key)?)
    }

    pub fn query_volumes(
        &self,
        sync: SyncDirection,
        vids: Option<HashSet<VolumeId>>,
    ) -> impl TryIterator<Ok = VolumeState, Err = Culprit<StorageErr>> {
        let iter = self.volumes.snapshot().iter().err_into();
        let iter = VolumeQueryIter::new(iter);
        iter.try_filter(move |state| {
            let matches_vid = vids.as_ref().is_none_or(|s| s.contains(state.vid()));
            let matches_dir = state.config().sync().matches(sync);
            Ok(matches_vid && matches_dir)
        })
    }

    /// Returns an iterator of `PageValue`'s at an exact LSN for a volume.
    /// Notably, this function will not return a page at an earlier LSN that is
    /// shadowed by this LSN.
    pub fn query_pages<'a, I>(
        &self,
        vid: &'a VolumeId,
        lsn: LSN,
        pages: I,
    ) -> impl TryIterator<Ok = (PageIdx, Option<PageValue>), Err = Culprit<StorageErr>> + 'a
    where
        I: TryIterator<Ok = PageIdx, Err = Culprit<StorageErr>> + 'a,
    {
        let snapshot = self.pages.snapshot();
        pages.map_ok(move |pageidx| {
            let key = PageKey::new(vid.clone(), pageidx, lsn);
            if let Some(page) = snapshot.get(key)? {
                Ok((pageidx, Some(PageValue::try_from(page).or_into_ctx()?)))
            } else {
                Ok((pageidx, None))
            }
        })
    }

    /// Returns the most recent visible page in a volume by LSN at a particular
    /// `PageIdx`. Notably, this will return a page from an earlier LSN if the page
    /// hasn't changed since then.
    pub fn read(&self, vid: &VolumeId, lsn: LSN, pageidx: PageIdx) -> Result<(LSN, PageValue)> {
        let first_key = PageKey::new(vid.clone(), pageidx, LSN::FIRST);
        let key = PageKey::new(vid.clone(), pageidx, lsn);
        let range = first_key..=key;

        // Search for the latest page between LSN(0) and the requested LSN,
        // returning PageValue::Pending if none found.
        if let Some((key, page)) = self.pages.snapshot().range(range).next_back().transpose()? {
            let lsn = PageKey::try_ref_from_bytes(&key)?.lsn();
            let bytes: Bytes = page.into();
            Ok((lsn, PageValue::try_from(bytes).or_into_ctx()?))
        } else {
            Ok((lsn, PageValue::Pending))
        }
    }

    pub fn commit(
        &self,
        vid: &VolumeId,
        snapshot: Option<Snapshot>,
        pages: impl Into<PageCount>,
        memtable: Memtable,
    ) -> Result<Snapshot> {
        let pages = pages.into();
        let span = tracing::debug_span!(
            "volume_commit",
            ?vid,
            ?snapshot,
            %pages,
            result = field::Empty
        )
        .entered();

        let mut batch = self.keyspace.batch();
        batch = batch.durability(Some(fjall::PersistMode::SyncAll));

        let read_lsn = snapshot.as_ref().map(|s| s.local());
        let commit_lsn = read_lsn.map_or(LSN::FIRST, |lsn| lsn.next().expect("lsn overflow"));

        // this Splinter will contain all of the PageIdxs this commit changed
        let mut graft = Splinter::default();

        // persist the memtable
        let mut page_key = PageKey::new(vid.clone(), PageIdx::FIRST, commit_lsn);
        for (pageidx, page) in memtable {
            page_key = page_key.with_index(pageidx);
            graft.insert(pageidx.into());
            batch.insert(&self.pages, page_key.as_bytes(), PageValue::from(page));
        }

        // persist the new commit
        let commit_key = CommitKey::new(vid.clone(), commit_lsn);
        batch.insert(&self.commits, commit_key, graft.serialize_to_bytes());

        // acquire the commit lock
        let _permit = self.commit_lock.lock();

        // check to see if the read snapshot is the latest local snapshot while
        // holding the commit lock
        let latest = self.snapshot(vid)?;
        if latest.as_ref().map(|l| l.local()) != read_lsn {
            precept::expect_reachable!(
                "concurrent write to volume",
                {
                    "vid": vid,
                    "snapshot": snapshot,
                    "latest": latest,
                }
            );

            return Err(Culprit::new_with_note(
                StorageErr::ConcurrentWrite,
                format!("Illegal concurrent write to Volume {vid}"),
            ));
        }

        // persist the new volume snapshot
        let snapshot_key = VolumeStateKey::new(vid.clone(), VolumeStateTag::Snapshot);
        let snapshot = Snapshot::new(
            commit_lsn,
            // don't change the remote mapping during a local commit
            latest
                .map(|l| l.remote_mapping().clone())
                .unwrap_or_default(),
            pages,
        );
        batch.insert(&self.volumes, snapshot_key, snapshot.as_bytes());

        // commit the changes
        batch.commit()?;

        // notify listeners of the new local commit
        self.local_changeset.mark_changed(vid);

        // log the result
        span.record("result", snapshot.to_string());

        // return the new snapshot
        Ok(snapshot)
    }

    /// Replicate a remote commit to local storage.
    pub fn receive_remote_commit(
        &self,
        vid: &VolumeId,
        remote_snapshot: graft_proto::Snapshot,
        changed: SplinterRef<Bytes>,
    ) -> Result<()> {
        self.receive_remote_commit_holding_lock(
            self.commit_lock.lock(),
            vid,
            remote_snapshot,
            changed,
        )
    }

    /// Receive a remote commit into storage; it's only safe to call this
    /// function while holding the commit lock
    fn receive_remote_commit_holding_lock(
        &self,
        _permit: MutexGuard<'_, ()>,
        vid: &VolumeId,
        remote_snapshot: graft_proto::Snapshot,
        graft: SplinterRef<Bytes>,
    ) -> Result<()> {
        // resolve the remote lsn and page count
        let remote_lsn = remote_snapshot.lsn().expect("invalid remote LSN");
        let remote_pages = remote_snapshot.pages();

        let span = tracing::debug_span!(
            "receive_remote_commit",
            ?vid,
            ?remote_lsn,
            result = field::Empty,
        )
        .entered();

        let mut batch = self.keyspace.batch();
        batch = batch.durability(Some(fjall::PersistMode::SyncAll));

        // retrieve the current volume state
        let state = self.volume_state(vid)?;
        let snapshot = state.snapshot();
        let watermarks = state.watermarks();

        // ensure that we can accept this remote commit
        if state.is_syncing() {
            return Err(Culprit::new_with_note(
                StorageErr::VolumeIsSyncing,
                format!("Volume {vid} is syncing, refusing to accept remote changes"),
            ));
        }
        if state.has_pending_commits() {
            precept::expect_reachable!(
                "volume has pending commits while receiving remote commit",
                { "vid": vid, "state": state }
            );

            // mark the volume as having a remote conflict
            self.set_volume_status(&mut batch, vid, VolumeStatus::Conflict);

            return Err(Culprit::new_with_note(
                StorageErr::RemoteConflict,
                format!("Volume {vid:?} has pending commits, refusing to accept remote changes"),
            ));
        }

        // compute the next local lsn
        let commit_lsn = snapshot.map_or(LSN::FIRST, |s| s.local().next().expect("lsn overflow"));
        let remote_mapping = RemoteMapping::new(remote_lsn, commit_lsn);

        // persist the new volume snapshot
        let new_snapshot = Snapshot::new(commit_lsn, remote_mapping, remote_pages);
        batch.insert(
            &self.volumes,
            VolumeStateKey::new(vid.clone(), VolumeStateTag::Snapshot),
            new_snapshot.as_bytes(),
        );

        // fast forward the pending sync watermark to ensure we don't roundtrip this
        // commit back to the server
        batch.insert(
            &self.volumes,
            VolumeStateKey::new(vid.clone(), VolumeStateTag::Watermarks),
            watermarks
                .clone()
                .with_pending_sync(Watermark::new(commit_lsn, remote_pages)),
        );

        // mark changed pages
        let mut key = PageKey::new(vid.clone(), PageIdx::FIRST, commit_lsn);
        let pending = Bytes::from(PageValue::Pending);
        for pageidx in graft.iter() {
            key = key.with_index(pageidx.try_into()?);
            batch.insert(&self.pages, key.as_ref(), pending.clone());
        }

        batch.commit()?;

        // notify listeners of the new remote commit
        self.remote_changeset.mark_changed(vid);

        // log the result
        span.record("result", new_snapshot.to_string());

        Ok(())
    }

    /// Write a set of `PageValue`'s to storage.
    pub fn receive_pages(
        &self,
        vid: &VolumeId,
        pages: HashMap<PageIdx, (LSN, PageValue)>,
    ) -> Result<()> {
        let mut batch = self.keyspace.batch();
        batch = batch.durability(Some(fjall::PersistMode::SyncAll));

        for (pageidx, (lsn, pagevalue)) in pages {
            tracing::trace!("caching page {pageidx} into lsn {lsn} with value {pagevalue:?}");
            let key = PageKey::new(vid.clone(), pageidx, lsn);
            batch.insert(&self.pages, key.as_ref(), pagevalue);
        }
        Ok(batch.commit()?)
    }

    /// Prepare to sync a volume to the remote.
    /// Returns:
    /// - the last known remote LSN
    /// - the local page count we are syncing
    /// - the range of LSNs to sync
    /// - an iterator of commits to sync
    #[allow(clippy::type_complexity)]
    pub fn prepare_sync_to_remote(
        &self,
        vid: &VolumeId,
    ) -> Result<(
        Option<LSN>,
        PageCount,
        RangeInclusive<LSN>,
        impl TryIterator<Ok = (LSN, SplinterRef<Slice>), Err = Culprit<StorageErr>>,
    )> {
        // acquire the commit lock
        let _permit = self.commit_lock.lock();

        // retrieve the current volume state
        let state = self.volume_state(vid)?;

        // ensure that we only run this job when we actually have commits to sync
        precept::expect_always_or_unreachable!(
            state.has_pending_commits(),
            "the sync push job only runs when we have local commits to push",
            { "vid": vid, "state": state }
        );

        // resolve the snapshot; we can expect it to be available because this
        // function should only run when we have local commits to sync
        let snapshot = state.snapshot().expect("volume snapshot missing").clone();
        let local_lsn = snapshot.local();

        // calculate the end of the sync range
        let (end_lsn, page_count) = if state.is_syncing() {
            // if we are resuming a previously interrupted sync, use the
            // existing pending_sync watermark
            let pending_sync = state.watermarks().pending_sync();
            tracing::debug!(
                ?vid,
                ?pending_sync,
                %snapshot,
                "resuming previously interrupted sync"
            );
            precept::expect_reachable!("resuming previously interrupted sync", state);
            pending_sync.splat().expect("pending sync must be mapped")
        } else {
            // update pending_sync to the local LSN
            self.volumes.insert(
                VolumeStateKey::new(vid.clone(), VolumeStateTag::Watermarks),
                state
                    .watermarks()
                    .clone()
                    .with_pending_sync(Watermark::new(local_lsn, snapshot.pages())),
            )?;
            (local_lsn, snapshot.pages())
        };

        // calculate the LSN range of commits to sync
        let start_lsn = state
            .snapshot()
            .and_then(|s| s.remote_local())
            .map_or(LSN::FIRST, |s| s.next().expect("LSN overflow"));
        let lsns = start_lsn..=end_lsn;

        // create a commit iterator
        let commit_start = CommitKey::new(vid.clone(), *lsns.start());
        let commit_end = CommitKey::new(vid.clone(), *lsns.end());
        let mut cursor = commit_start.lsn();
        let commits = self
            .commits
            .snapshot()
            .range(commit_start..=commit_end)
            .err_into()
            .map_ok(move |(k, v)| {
                let lsn = CommitKey::ref_from_bytes(&k)?.lsn();

                // detect missing commits
                assert_eq!(lsn, cursor, "missing commit detected");
                cursor = cursor.next().expect("lsn overflow");

                let splinter = SplinterRef::from_bytes(v).or_into_ctx()?;
                Ok((lsn, splinter))
            });

        Ok((snapshot.remote(), page_count, lsns, commits))
    }

    /// Update storage after a rejected sync
    pub fn rejected_sync_to_remote(&self, vid: &VolumeId) -> Result<()> {
        // acquire the commit lock
        let _permit = self.commit_lock.lock();
        let mut batch = self.keyspace.batch();
        batch = batch.durability(Some(fjall::PersistMode::SyncAll));

        // clear the pending sync watermark
        let watermarks_key = VolumeStateKey::new(vid.clone(), VolumeStateTag::Watermarks);
        let watermarks = self
            .volumes
            .get(&watermarks_key)?
            .map(|w| Watermarks::from_bytes(&w))
            .transpose()?
            .unwrap_or_default()
            .with_pending_sync(Watermark::default());
        batch.insert(&self.volumes, watermarks_key, watermarks);

        // update the volume status
        self.set_volume_status(&mut batch, vid, VolumeStatus::RejectedCommit);

        Ok(batch.commit()?)
    }

    /// Complete a push operation by updating the volume snapshot and removing
    /// all synced commits.
    pub fn complete_sync_to_remote(
        &self,
        vid: &VolumeId,
        remote_snapshot: graft_proto::Snapshot,
        synced_lsns: RangeInclusive<LSN>,
    ) -> Result<()> {
        // acquire the commit lock and start a new batch
        let _permit = self.commit_lock.lock();
        let mut batch = self.keyspace.batch();
        batch = batch.durability(Some(fjall::PersistMode::SyncAll));

        let state = self.volume_state(vid)?;

        // resolve the snapshot; we can expect it to be available because this
        // function should only run after we have synced a local commit
        let snapshot = state.snapshot().expect("volume snapshot missing");

        let local_lsn = snapshot.local();
        let pages = snapshot.pages();
        let remote_lsn = remote_snapshot.lsn().expect("invalid remote LSN");
        let remote_local_lsn = synced_lsns.try_end().expect("lsn range is RangeInclusive");

        // check invariants
        assert!(
            snapshot.remote() < Some(remote_lsn),
            "remote LSN should be monotonically increasing"
        );
        assert_eq!(
            state.watermarks().pending_sync().lsn(),
            Some(remote_local_lsn),
            "the pending_sync watermark doesn't match the synced LSN range"
        );

        // persist the new remote mapping to the snapshot
        let remote_mapping = RemoteMapping::new(remote_lsn, remote_local_lsn);
        let new_snapshot = Snapshot::new(local_lsn, remote_mapping, pages);
        batch.insert(
            &self.volumes,
            VolumeStateKey::new(vid.clone(), VolumeStateTag::Snapshot),
            new_snapshot.as_bytes(),
        );

        // clear the pending_sync watermark
        batch.insert(
            &self.volumes,
            VolumeStateKey::new(vid.clone(), VolumeStateTag::Watermarks),
            state
                .watermarks()
                .clone()
                .with_pending_sync(Watermark::default()),
        );

        // if the status is interrupted push, clear the status
        if state.status() == VolumeStatus::InterruptedPush {
            batch.remove(
                &self.volumes,
                VolumeStateKey::new(vid.clone(), VolumeStateTag::Status),
            );
        }

        // remove all commits in the synced range
        let mut key = CommitKey::new(vid.clone(), LSN::FIRST);
        for lsn in synced_lsns.iter() {
            key = key.with_lsn(lsn);
            batch.remove(&self.commits, key.as_ref());
        }

        batch.commit()?;

        tracing::debug!(?synced_lsns, %remote_lsn, %new_snapshot, "completed sync to remote");

        Ok(())
    }

    /// Reset the volume to the provided remote snapshot.
    /// This will cause all pending commits to be rolled back and the volume
    /// status to be cleared.
    pub fn reset_volume_to_remote(
        &self,
        vid: &VolumeId,
        remote_snapshot: graft_proto::Snapshot,
        remote_graft: SplinterRef<Bytes>,
    ) -> Result<()> {
        // acquire the commit lock and start a new batch
        let permit = self.commit_lock.lock();

        let span = tracing::debug_span!(
            "reset_volume_to_remote",
            ?vid,
            local_lsn = field::Empty,
            reset_lsn = field::Empty,
            remote_lsn = field::Empty,
            commit_lsn = field::Empty,
            result = field::Empty,
        )
        .entered();

        // retrieve the current volume state
        let state = self.volume_state(vid)?;
        let snapshot = state.snapshot();

        // the last local lsn
        let local_lsn = snapshot.map(|s| s.local());
        // the local lsn we are resetting to
        let reset_lsn = snapshot.and_then(|s| s.remote_local());
        // the remote lsn to receive
        let remote_lsn = remote_snapshot.lsn().expect("invalid remote LSN");
        // the new local lsn to commit the remote into
        let commit_lsn = reset_lsn.map_or(LSN::FIRST, |lsn| lsn.next().expect("lsn overflow"));

        span.record("local_lsn", format!("{local_lsn:?}"));
        span.record("reset_lsn", format!("{reset_lsn:?}"));
        span.record("remote_lsn", format!("{remote_lsn:?}"));
        span.record("commit_lsn", format!("{commit_lsn:?}"));

        if local_lsn == reset_lsn {
            // if the local and remote LSNs are the same, we can just receive the
            // remote commit normally
            assert!(
                !state.has_pending_commits(),
                "bug: local lsn == reset lsn but state has pending commits"
            );
            span.record("result", format!("{snapshot:?}"));
            drop(span);
            return self.receive_remote_commit_holding_lock(
                permit,
                vid,
                remote_snapshot,
                remote_graft,
            );
        }

        // ensure we never reset into the future
        assert!(
            reset_lsn < local_lsn,
            "refusing to reset to a LSN larger than the current LSN; local={local_lsn:?}, target={reset_lsn:?}"
        );

        let mut batch = self.keyspace.batch();
        batch = batch.durability(Some(fjall::PersistMode::SyncAll));

        // persist the new volume snapshot
        let remote_mapping = RemoteMapping::new(remote_lsn, commit_lsn);
        let new_snapshot = Snapshot::new(commit_lsn, remote_mapping, remote_snapshot.pages());
        batch.insert(
            &self.volumes,
            VolumeStateKey::new(vid.clone(), VolumeStateTag::Snapshot),
            new_snapshot.as_bytes(),
        );

        // clear the volume status
        batch.remove(
            &self.volumes,
            VolumeStateKey::new(vid.clone(), VolumeStateTag::Status),
        );

        // clear the pending_sync watermark
        batch.insert(
            &self.volumes,
            VolumeStateKey::new(vid.clone(), VolumeStateTag::Watermarks),
            state
                .watermarks()
                .clone()
                .with_pending_sync(Watermark::default()),
        );

        // remove all pending commits
        let mut commits = self.commits.snapshot().prefix(vid);
        while let Some((key, graft)) = commits.try_next().or_into_ctx()? {
            batch.remove(&self.commits, key.clone());

            let key = CommitKey::ref_from_bytes(&key)?;
            assert_eq!(
                key.vid(),
                vid,
                "refusing to remove commit from another volume"
            );
            assert!(
                Some(key.lsn()) > reset_lsn,
                "invariant violation: no commits should exist at or below reset_lsn"
            );

            // remove the commit's changed PageIdxs
            let graft = SplinterRef::from_bytes(graft).or_into_ctx()?;

            let mut key = PageKey::new(vid.clone(), PageIdx::FIRST, key.lsn());
            for pageidx in graft.iter() {
                key = key.with_index(pageidx.try_into()?);
                batch.remove(&self.pages, key.as_ref());
            }
        }

        // mark remotely changed pages
        let mut key = PageKey::new(vid.clone(), PageIdx::FIRST, commit_lsn);
        let pending = Bytes::from(PageValue::Pending);
        for pageidx in remote_graft.iter() {
            key = key.with_index(pageidx.try_into()?);
            batch.insert(&self.pages, key.as_ref(), pending.clone());
        }

        // commit the changes
        batch.commit()?;

        // post reset invariants
        // these are expensive so we only run them when precept is enabled
        if precept::ENABLED {
            // scan all of the pages in the volume to verify two invariants:
            // 1. all pages at commit_lsn must be pending
            // 2. no pages exist at an lsn > commit_lsn
            let mut iter = self.pages.snapshot().prefix(vid);
            while let Some((key, val)) = iter.try_next().or_into_ctx()? {
                let key = PageKey::try_ref_from_bytes(&key)?;
                if key.lsn() == commit_lsn {
                    // invariant 1: all pages at commit_lsn must be pending
                    assert!(
                        PageValue::is_pending(&val),
                        "all pages at commit_lsn must be pending after reset"
                    );
                } else {
                    // invariant 2: no pages exist at an lsn > commit_lsn
                    assert!(
                        key.lsn() < commit_lsn,
                        "no pages should exist at a lsn > commit_lsn after reset"
                    );
                }
            }
        }

        // notify listeners of the new remote commit
        self.remote_changeset.mark_changed(vid);

        // log the result
        span.record("result", new_snapshot.to_string());

        Ok(())
    }
}

impl Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage")
            .field("disk usage", &ByteUnit::new(self.keyspace.disk_space()))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use graft_core::{page::Page, pageidx};

    use super::*;

    #[graft_test::test]
    fn test_query_volumes() {
        let storage = Storage::open_temporary().unwrap();

        let mut memtable = Memtable::default();
        memtable.insert(pageidx!(1), Page::test_filled(0x42));

        let mut vids = [VolumeId::random(), VolumeId::random()];
        vids.sort();

        // first volume has two commits, and is configured to pull
        storage
            .set_volume_config(&vids[0], VolumeConfig::new(SyncDirection::Pull))
            .unwrap();
        let snapshot = storage.commit(&vids[0], None, 1, memtable.clone()).unwrap();
        storage
            .commit(&vids[0], Some(snapshot), 1, memtable.clone())
            .unwrap();

        // second volume has one commit, and is configured to push
        storage
            .set_volume_config(&vids[1], VolumeConfig::new(SyncDirection::Push))
            .unwrap();
        storage.commit(&vids[1], None, 1, memtable.clone()).unwrap();

        // ensure that we can query back out the snapshots
        let sync = SyncDirection::Both;
        let mut iter = storage.query_volumes(sync, None);

        // check the first volume
        let state = iter.try_next().unwrap().unwrap();
        assert_eq!(state.vid(), &vids[0]);
        assert_eq!(state.config().sync(), SyncDirection::Pull);
        let snapshot = state.snapshot().unwrap();
        assert_eq!(snapshot.local(), LSN::new(2));
        assert_eq!(snapshot.pages(), 1);

        // check the second volume
        let state = iter.try_next().unwrap().unwrap();
        assert_eq!(state.vid(), &vids[1]);
        assert_eq!(state.config().sync(), SyncDirection::Push);
        let snapshot = state.snapshot().unwrap();
        assert_eq!(snapshot.local(), LSN::new(1));
        assert_eq!(snapshot.pages(), 1);

        // iter is empty
        assert!(iter.next().is_none());

        // verify that the sync direction filter works
        let sync = SyncDirection::Push;
        let mut iter = storage.query_volumes(sync, None);

        // should be the second volume
        let state = iter.try_next().unwrap().unwrap();
        assert_eq!(state.vid(), &vids[1]);
        assert_eq!(state.config().sync(), SyncDirection::Push);
        let snapshot = state.snapshot().unwrap();
        assert_eq!(snapshot.local(), LSN::new(1));
        assert_eq!(snapshot.pages(), 1);

        // iter is empty
        assert!(iter.next().is_none());

        // verify that the volume id filter works
        let sync = SyncDirection::Both;
        let vid_set = HashSet::from_iter([vids[0].clone()]);
        let mut iter = storage.query_volumes(sync, Some(vid_set));

        // should be the first volume
        let state = iter.try_next().unwrap().unwrap();
        assert_eq!(state.vid(), &vids[0]);

        // iter is empty
        assert!(iter.next().is_none());
    }
}
