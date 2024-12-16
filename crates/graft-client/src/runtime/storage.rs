use std::{
    io,
    path::Path,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use commit::CommitKey;
use fjall::{KvSeparationOptions, PartitionCreateOptions};
use graft_core::{
    lsn::LSN,
    page::{PageSizeErr, EMPTY_PAGE},
    page_offset::PageOffset,
    zerocopy_err::ZerocopyErr,
    VolumeId,
};
use memtable::Memtable;
use page::{PageKey, PageValue};
use snapshot::{Snapshot, SnapshotKey, SnapshotKind};
use splinter::Splinter;
use zerocopy::{IntoBytes, TryFromBytes};

pub(crate) mod commit;
pub(crate) mod memtable;
pub(crate) mod page;
pub mod snapshot;

#[derive(Debug, thiserror::Error)]
pub enum StorageErr {
    #[error(transparent)]
    FjallErr(#[from] fjall::Error),

    #[error(transparent)]
    IoErr(#[from] io::Error),

    #[error("Corrupt snapshot: {0}")]
    CorruptSnapshot(ZerocopyErr),

    #[error("Corrupt page: {0}")]
    CorruptPage(#[from] PageSizeErr),

    #[error("Illegal concurrent write to volume {0}")]
    ConcurrentWrite(VolumeId),
}

#[derive(Clone)]
pub struct Storage {
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

    /// Used to serialize the Volume commit process
    commit_lock: Arc<Mutex<()>>,
}

impl Storage {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, StorageErr> {
        Self::open_config(fjall::Config::new(path))
    }

    pub fn open_temporary() -> Result<Self, StorageErr> {
        Self::open_config(fjall::Config::new(tempfile::tempdir()?.into_path()).temporary(true))
    }

    pub fn open_config(config: fjall::Config) -> Result<Self, StorageErr> {
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
        Ok(Storage {
            keyspace,
            volumes,
            pages,
            commits,
            commit_lock: Default::default(),
        })
    }

    pub fn snapshot(&self, vid: VolumeId) -> Result<Option<Snapshot>, StorageErr> {
        let key = snapshot::SnapshotKey::new(vid, SnapshotKind::Local);
        if let Some(snapshot) = self.volumes.get(key)? {
            Ok(Some(
                Snapshot::try_read_from_bytes(&snapshot)
                    .map_err(|e| StorageErr::CorruptSnapshot(e.into()))?,
            ))
        } else {
            Ok(None)
        }
    }

    pub fn read(
        &self,
        vid: VolumeId,
        offset: PageOffset,
        lsn: LSN,
    ) -> Result<PageValue, StorageErr> {
        let zero = PageKey::new(vid.clone(), offset, LSN::ZERO);
        let key = PageKey::new(vid, offset, lsn);
        let range = zero..=key;

        // Search for the latest page between LSN(0) and the requested LSN,
        // returning None if no page is found.
        if let Some((_, page)) = self.pages.range(range).next_back().transpose()? {
            let bytes: Bytes = page.into();
            Ok(bytes.try_into()?)
        } else {
            Ok(PageValue::Available(EMPTY_PAGE))
        }
    }

    pub fn commit(
        &self,
        vid: VolumeId,
        snapshot: Option<Snapshot>,
        memtable: Memtable,
    ) -> Result<Snapshot, StorageErr> {
        let mut batch = self.keyspace.batch();
        let read_lsn = snapshot.as_ref().map(|s| s.lsn());
        let mut max_offset = snapshot
            .and_then(|s| s.page_count().last_offset())
            .unwrap_or(PageOffset::ZERO);
        let commit_lsn = read_lsn
            .map(|lsn| lsn.next().expect("lsn overflow"))
            .unwrap_or_default();

        // construct a changed offsets splinter
        let mut offsets = Splinter::default();

        // write out the memtable
        let mut page_key = PageKey::new(vid.clone(), PageOffset::ZERO, commit_lsn);
        for (offset, page) in memtable {
            page_key.set_offset(offset);
            max_offset = max_offset.max(offset);
            offsets.insert(offset.into());
            batch.insert(&self.pages, page_key.as_bytes(), page);
        }

        // write out a new volume snapshot
        let snapshot_key = SnapshotKey::new(vid.clone(), SnapshotKind::Local);
        let snapshot = Snapshot::new(commit_lsn, max_offset.pages());
        batch.insert(&self.volumes, snapshot_key, snapshot.as_bytes());

        // write out a new commit
        let commit_key = CommitKey::new(vid.clone(), commit_lsn);
        batch.insert(&self.commits, commit_key, offsets.serialize_to_bytes());

        // acquire the commit lock
        let _permit = self.commit_lock.lock().expect("commit lock poisoned");

        // check to see if the read snapshot is the latest local snapshot while
        // holding the commit lock
        let latest = self.snapshot(vid.clone())?;
        if latest.map(|l| l.lsn()) != read_lsn {
            return Err(StorageErr::ConcurrentWrite(vid));
        }

        // commit the changes
        batch.commit()?;

        // return the new snapshot
        Ok(snapshot)
    }
}
