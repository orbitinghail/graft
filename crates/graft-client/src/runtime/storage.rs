use std::{collections::HashSet, fmt::Debug, io, ops::RangeBounds, path::Path, sync::Arc};

use bytes::Bytes;
use changeset::ChangeSet;
use commit::CommitKey;
use culprit::{Culprit, ResultExt};
use fjall::{KvSeparationOptions, PartitionCreateOptions, Slice};
use graft_core::{
    byte_unit::ByteUnit,
    lsn::{LSNRangeExt, LSN},
    page_count::PageCount,
    page_offset::PageOffset,
    zerocopy_err::ZerocopyErr,
    VolumeId,
};
use graft_proto::pagestore::v1::PageAtOffset;
use memtable::Memtable;
use page::{PageKey, PageValue, PageValueConversionErr};
use parking_lot::Mutex;
use snapshot::{Snapshot, SnapshotKey, SnapshotKind, SnapshotKindMask, SnapshotSet};
use splinter::{DecodeErr, Splinter, SplinterRef};
use tryiter::{TryIterator, TryIteratorExt};
use volume_config::{SyncDirection, VolumeConfig};
use zerocopy::{IntoBytes, TryFromBytes};

pub mod changeset;
pub(crate) mod commit;
pub(crate) mod memtable;
pub(crate) mod page;
pub mod snapshot;
pub mod volume_config;

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

    #[error("Corrupt page: {0}")]
    CorruptPage(#[from] PageValueConversionErr),

    #[error("Corrupt commit: {0}")]
    CorruptCommit(#[from] DecodeErr),

    #[error("Illegal concurrent write to volume")]
    ConcurrentWrite,
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

pub struct Storage {
    keyspace: fjall::Keyspace,

    /// Used to store volume state broken out by prefix.
    ///
    /// config/{vid} -> VolumeConfig
    /// snapshot/{vid} -> Snapshot
    /// watermarks/{vid} -> Watermarks
    volumes: fjall::Partition,

    /// Used to store volume snapshots
    /// maps from (VolumeId, SnapshotKind) to Snapshot
    snapshots: fjall::Partition,

    /// Used to store page contents
    /// maps from (VolumeId, Offset, LSN) to PageValue
    pages: fjall::Partition,

    /// Used to track changes made by local commits.
    /// maps from (VolumeId, LSN) to Splinter of written offsets
    commits: fjall::Partition,

    /// Used to serialize the Volume commit process
    commit_lock: Arc<Mutex<()>>,

    /// Used to notify subscribers of new local commits
    local_changeset: ChangeSet<VolumeId>,

    /// Used to notify subscribers of new remote commits
    remote_changeset: ChangeSet<VolumeId>,
}

impl Storage {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_config(fjall::Config::new(path))
    }

    pub fn open_temporary() -> Result<Self> {
        Self::open_config(fjall::Config::new(tempfile::tempdir()?.into_path()).temporary(true))
    }

    pub fn open_config(config: fjall::Config) -> Result<Self> {
        let keyspace = config.open()?;
        let volumes = keyspace.open_partition("volumes", Default::default())?;
        let snapshots = keyspace.open_partition("snapshots", Default::default())?;
        let pages = keyspace.open_partition(
            "pages",
            PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
        )?;
        let commits = keyspace.open_partition(
            "commits",
            PartitionCreateOptions::default().with_kv_separation(KvSeparationOptions::default()),
        )?;
        Ok(Storage {
            keyspace,
            volumes,
            snapshots,
            pages,
            commits,
            commit_lock: Default::default(),
            local_changeset: Default::default(),
            remote_changeset: Default::default(),
        })
    }

    /// Access the local commit changeset. This ChangeSet is updated whenever a
    /// Volume receives a local commit.
    pub fn local_changeset(&self) -> &ChangeSet<VolumeId> {
        &self.local_changeset
    }

    /// Access the remote commit changeset. This ChangeSet is updated whenever a
    /// Volume receives a remote commit.
    pub fn remote_changeset(&self) -> &ChangeSet<VolumeId> {
        &self.remote_changeset
    }

    /// Add a new volume to the storage. This function will overwrite any
    /// existing configuration for the volume.
    pub fn set_volume_config(&self, vid: &VolumeId, config: VolumeConfig) -> Result<()> {
        Ok(self.volumes.insert(vid.as_bytes(), config)?)
    }

    pub fn query_volumes(
        &self,
        sync: SyncDirection,
        kind_mask: SnapshotKindMask,
        vids: Option<HashSet<VolumeId>>,
    ) -> impl TryIterator<Ok = (VolumeId, VolumeConfig, SnapshotSet), Err = Culprit<StorageErr>> + '_
    {
        let seqno = self.keyspace.instant();
        let volumes = self.volumes.snapshot_at(seqno).iter().err_into();

        volumes.try_filter_map(move |(vid, config)| {
            let vid = VolumeId::try_read_from_bytes(&vid)
                .map_err(|e| StorageErr::CorruptKey(e.into()))?;
            let config = VolumeConfig::try_read_from_bytes(&config)
                .map_err(|e| StorageErr::CorruptVolumeConfig(e.into()))?;
            let matches_vid = vids.as_ref().map_or(true, |set| set.contains(&vid));
            if matches_vid && sync.matches(config.sync()) {
                let set = self.snapshots_with_seqno(seqno, &vid, kind_mask)?;
                Ok(Some((vid, config, set)))
            } else {
                Ok(None)
            }
        })
    }

    pub fn query_commits(
        &self,
        vid: &VolumeId,
        lsns: impl RangeBounds<LSN>,
    ) -> impl TryIterator<Ok = (LSN, SplinterRef<Slice>), Err = Culprit<StorageErr>> + '_ {
        let start = CommitKey::new(vid.clone(), lsns.try_start().unwrap_or_default());
        let end = CommitKey::new(vid.clone(), lsns.try_end().unwrap_or_default());
        self.commits
            .snapshot()
            .range(start..=end)
            .err_into()
            .map_ok(|(k, v)| {
                let lsn = CommitKey::try_ref_from_bytes(&k)
                    .map_err(|e| StorageErr::CorruptKey(e.into()))?
                    .lsn();
                let splinter = SplinterRef::from_bytes(v).or_into_ctx()?;
                Ok((lsn, splinter))
            })
    }

    /// Returns an iterator of PageValue's at an exact LSN for a volume.
    /// Notably, this function will not return a page at an earlier LSN that is
    /// shadowed by this LSN.
    pub fn query_pages<'a, T>(
        &'a self,
        vid: &'a VolumeId,
        lsn: LSN,
        offsets: &'a SplinterRef<T>,
    ) -> impl TryIterator<Ok = (PageOffset, Option<PageValue>), Err = Culprit<StorageErr>> + 'a
    where
        T: AsRef<[u8]> + 'a,
    {
        offsets
            .iter()
            .map(move |offset| {
                let offset: PageOffset = offset.into();
                let key = PageKey::new(vid.clone(), offset, lsn);
                Ok((offset, self.pages.get(key)?))
            })
            .map_ok(|(offset, page)| {
                if let Some(page) = page {
                    Ok((offset, Some(PageValue::try_from(page).or_into_ctx()?)))
                } else {
                    Ok((offset, None))
                }
            })
    }

    pub fn snapshots(&self, vid: &VolumeId, kind_mask: SnapshotKindMask) -> Result<SnapshotSet> {
        let seqno = self.keyspace.instant();
        self.snapshots_with_seqno(seqno, vid, kind_mask)
    }

    fn snapshots_with_seqno(
        &self,
        seqno: u64,
        vid: &VolumeId,
        kind_mask: SnapshotKindMask,
    ) -> Result<SnapshotSet> {
        let mut snapshots = self
            .snapshots
            .snapshot_at(seqno)
            .prefix(vid)
            .err_into::<StorageErr>()
            .try_filter_map(move |(k, v)| {
                let key = SnapshotKey::try_read_from_bytes(&k)
                    .map_err(|e| StorageErr::CorruptKey(e.into()))?;
                if kind_mask.contains(key.kind()) {
                    let val = Snapshot::try_read_from_bytes(&v)
                        .map_err(|e| StorageErr::CorruptSnapshot(e.into()))?;
                    Ok(Some((key, val)))
                } else {
                    Ok(None)
                }
            });

        let mut set = SnapshotSet::default();
        while let Some((key, snapshot)) = snapshots.try_next()? {
            assert_eq!(key.vid(), vid);
            set.insert(key.kind(), snapshot);
        }
        Ok(set)
    }

    pub fn snapshot(&self, vid: &VolumeId, kind: SnapshotKind) -> Result<Option<Snapshot>> {
        let key = snapshot::SnapshotKey::new(vid.clone(), kind);
        if let Some(snapshot) = self.snapshots.get(key)? {
            Ok(Some(
                Snapshot::try_read_from_bytes(&snapshot)
                    .map_err(|e| StorageErr::CorruptSnapshot(e.into()))?,
            ))
        } else {
            Ok(None)
        }
    }

    /// Returns the most recent visible page in a volume by LSN at a particular
    /// offset. Notably, this will return a page from an earlier LSN if the page
    /// hasn't changed since then.
    pub fn read(&self, vid: &VolumeId, lsn: LSN, offset: PageOffset) -> Result<(LSN, PageValue)> {
        let zero = PageKey::new(vid.clone(), offset, LSN::ZERO);
        let key = PageKey::new(vid.clone(), offset, lsn);
        let range = zero..=key;

        // Search for the latest page between LSN(0) and the requested LSN,
        // returning PageValue::Pending if none found.
        if let Some((key, page)) = self.pages.snapshot().range(range).next_back().transpose()? {
            let lsn = PageKey::try_ref_from_bytes(&key)
                .or_ctx(|e| StorageErr::CorruptKey(e.into()))?
                .lsn();
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
        memtable: Memtable,
    ) -> Result<Snapshot> {
        let mut batch = self.keyspace.batch();
        let mut pages = snapshot.as_ref().map_or(PageCount::ZERO, |s| s.pages());
        let read_lsn = snapshot.map(|s| s.lsn());
        let commit_lsn = read_lsn
            .map(|lsn| lsn.next().expect("lsn overflow"))
            .unwrap_or_default();

        // construct a changed offsets splinter
        let mut offsets = Splinter::default();

        // write out the memtable
        let mut page_key = PageKey::new(vid.clone(), PageOffset::ZERO, commit_lsn);
        for (offset, page) in memtable {
            page_key.set_offset(offset);
            pages = pages.max(offset.pages());
            offsets.insert(offset.into());
            batch.insert(&self.pages, page_key.as_bytes(), PageValue::from(page));
        }

        // write out a new volume snapshot
        let snapshot_key = SnapshotKey::new(vid.clone(), SnapshotKind::Local);
        let snapshot = Snapshot::new(commit_lsn, pages);
        batch.insert(&self.snapshots, snapshot_key, snapshot.as_bytes());

        // write out a new commit
        let commit_key = CommitKey::new(vid.clone(), commit_lsn);
        batch.insert(&self.commits, commit_key, offsets.serialize_to_bytes());

        // acquire the commit lock
        let _permit = self.commit_lock.lock();

        // check to see if the read snapshot is the latest local snapshot while
        // holding the commit lock
        let latest = self.snapshot(vid, SnapshotKind::Local)?;
        if latest.map(|l| l.lsn()) != read_lsn {
            return Err(Culprit::new_with_note(
                StorageErr::ConcurrentWrite,
                format!("Illegal concurrent write to Volume {vid}"),
            ));
        }

        // commit the changes
        batch.commit()?;

        // notify listeners of the new local commit
        self.local_changeset.mark_changed(&vid);

        // return the new snapshot
        Ok(snapshot)
    }

    /// Replicate a remote commit to local storage.
    pub fn receive_remote_commit(
        &self,
        vid: &VolumeId,
        is_checkpoint: bool,
        snapshot: Snapshot,
        changed: SplinterRef<Bytes>,
    ) -> Result<()> {
        log::trace!(
            "volume {:?} received remote commit with snapshot {:?}",
            vid,
            snapshot
        );

        // acquire the commit lock
        // TODO: reduce the scope of this lock
        let _permit = self.commit_lock.lock();

        let mut batch = self.keyspace.batch();

        // update the remote snapshot for the volume
        let snapshot_key = SnapshotKey::new(vid.clone(), SnapshotKind::Remote);
        batch.insert(&self.snapshots, snapshot_key, snapshot.as_ref());

        // update the checkpoint snapshot for the volume if needed
        if is_checkpoint {
            let snapshot_key = SnapshotKey::new(vid.clone(), SnapshotKind::Checkpoint);
            batch.insert(&self.snapshots, snapshot_key, snapshot.as_ref());
        }

        // compute the next local LSN
        let local_lsn = self
            .snapshot(&vid, SnapshotKind::Local)?
            .map_or(LSN::ZERO, |s| s.lsn().next().expect("lsn overflow"));

        // write out a local snapshot for the volume
        let snapshot_key = SnapshotKey::new(vid.clone(), SnapshotKind::Local);
        batch.insert(
            &self.snapshots,
            snapshot_key,
            Snapshot::new(local_lsn, snapshot.pages()),
        );

        // set the sync snapshot so we don't roundtrip this commit back to the
        // server
        let snapshot_key = SnapshotKey::new(vid.clone(), SnapshotKind::Sync);
        batch.insert(
            &self.snapshots,
            snapshot_key,
            Snapshot::new(local_lsn, snapshot.pages()),
        );

        // mark changed pages
        let mut key = PageKey::new(vid.clone(), PageOffset::ZERO, local_lsn);
        let pending = Bytes::from(PageValue::Pending);
        for offset in changed.iter() {
            key.set_offset(offset.into());
            batch.insert(&self.pages, key.as_ref(), pending.as_ref());
        }

        batch.commit()?;

        // notify listeners of the new remote commit
        self.remote_changeset.mark_changed(&vid);

        Ok(())
    }

    /// Write a set of pages to storage at a particular vid/lsn
    pub fn receive_pages(&self, vid: &VolumeId, lsn: LSN, pages: Vec<PageAtOffset>) -> Result<()> {
        let mut key = PageKey::new(vid.clone(), PageOffset::ZERO, lsn);
        let mut batch = self.keyspace.batch();
        for page in pages {
            key.set_offset(page.offset());
            batch.insert(
                &self.pages,
                key.as_ref(),
                PageValue::try_from(page.data).or_into_ctx()?,
            );
        }
        Ok(batch.commit()?)
    }

    /// Complete a sync operation by updating the remote snapshot for the volume
    /// and removing all synced commits.
    pub fn complete_sync(
        &self,
        vid: &VolumeId,
        is_checkpoint: bool,
        remote_snapshot: Snapshot,
        synced_lsns: impl RangeBounds<LSN>,
    ) -> Result<()> {
        let mut batch = self.keyspace.batch();

        // update the remote snapshot for the volume
        let snapshot_key = SnapshotKey::new(vid.clone(), SnapshotKind::Remote);
        batch.insert(&self.snapshots, snapshot_key, remote_snapshot.as_ref());

        // update the checkpoint snapshot for the volume if needed
        if is_checkpoint {
            let snapshot_key = SnapshotKey::new(vid.clone(), SnapshotKind::Checkpoint);
            batch.insert(&self.snapshots, snapshot_key, remote_snapshot.as_ref());
        }

        // remove all commits in the synced range
        let mut key = CommitKey::new(vid.clone(), LSN::ZERO);
        for lsn in synced_lsns.iter() {
            key.set_lsn(lsn);
            batch.remove(&self.commits, key.as_ref());
        }

        Ok(batch.commit()?)
    }

    pub fn set_snapshot(
        &self,
        vid: &VolumeId,
        kind: SnapshotKind,
        snapshot: Snapshot,
    ) -> Result<()> {
        let snapshot_key = SnapshotKey::new(vid.clone(), kind);
        Ok(self.snapshots.insert(snapshot_key, snapshot)?)
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
    use graft_core::page::Page;

    use super::*;

    #[test]
    fn test_query_volumes() {
        let storage = Storage::open_temporary().unwrap();

        let mut memtable = Memtable::default();
        memtable.insert(0.into(), Page::test_filled(0x42));

        let mut vids = [VolumeId::random(), VolumeId::random()];
        vids.sort();

        // first volume has two commits, and is configured to pull
        storage
            .set_volume_config(&vids[0], VolumeConfig::new(SyncDirection::Pull))
            .unwrap();
        let snapshot = storage.commit(&vids[0], None, memtable.clone()).unwrap();
        storage
            .commit(&vids[0], Some(snapshot), memtable.clone())
            .unwrap();

        // second volume has one commit, and is configured to push
        storage
            .set_volume_config(&vids[1], VolumeConfig::new(SyncDirection::Push))
            .unwrap();
        storage.commit(&vids[1], None, memtable.clone()).unwrap();

        // ensure that we can query back out the snapshots
        let sync = SyncDirection::Both;
        let mask = SnapshotKindMask::default().with(SnapshotKind::Local);
        let mut iter = storage.query_volumes(sync, mask, None);

        // check the first volume
        let (vid, config, set) = iter.try_next().unwrap().unwrap();
        assert_eq!(vid, vids[0]);
        assert_eq!(config.sync(), SyncDirection::Pull);
        let snapshot = set.local().unwrap();
        assert_eq!(snapshot.lsn(), LSN::new(1));
        assert_eq!(snapshot.pages(), 1);

        // check the second volume
        let (vid, config, set) = iter.try_next().unwrap().unwrap();
        assert_eq!(vid, vids[1]);
        assert_eq!(config.sync(), SyncDirection::Push);
        let snapshot = set.local().unwrap();
        assert_eq!(snapshot.lsn(), LSN::new(0));
        assert_eq!(snapshot.pages(), 1);

        // iter is empty
        assert!(iter.next().is_none());

        // verify that the sync direction filter works
        let sync = SyncDirection::Push;
        let mask = SnapshotKindMask::default().with(SnapshotKind::Local);
        let mut iter = storage.query_volumes(sync, mask, None);

        // should be the second volume
        let (vid, config, set) = iter.try_next().unwrap().unwrap();
        assert_eq!(vid, vids[1]);
        assert_eq!(config.sync(), SyncDirection::Push);
        let snapshot = set.local().unwrap();
        assert_eq!(snapshot.lsn(), LSN::new(0));
        assert_eq!(snapshot.pages(), 1);

        // iter is empty
        assert!(iter.next().is_none());

        // verify that the volume id filter works
        let sync = SyncDirection::Both;
        let mask = SnapshotKindMask::default().with(SnapshotKind::Local);
        let vid_set = HashSet::from_iter([vids[0].clone()]);
        let mut iter = storage.query_volumes(sync, mask, Some(vid_set));

        // should be the first volume
        let (vid, _, _) = iter.try_next().unwrap().unwrap();
        assert_eq!(vid, vids[0]);

        // iter is empty
        assert!(iter.next().is_none());
    }
}
