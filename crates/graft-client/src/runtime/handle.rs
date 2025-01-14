use culprit::{Result, ResultExt};
use std::{sync::Arc, time::Duration};
use tokio::sync::broadcast;

use graft_core::VolumeId;

use crate::{ClientErr, ClientPair};

use super::{
    storage::{
        snapshot::{Snapshot, SnapshotKind},
        volume::VolumeConfig,
        Storage,
    },
    sync::{SyncTask, SyncTaskHandle},
    txn::{ReadTxn, WriteTxn},
};

#[derive(Clone)]
pub struct RuntimeHandle {
    storage: Arc<Storage>,
}

impl RuntimeHandle {
    pub fn new(storage: Storage) -> Self {
        Self { storage: Arc::new(storage) }
    }

    pub fn spawn_sync_task(
        &self,
        clients: ClientPair,
        refresh_interval: Duration,
    ) -> SyncTaskHandle {
        SyncTask::spawn(self.storage.clone(), clients, refresh_interval)
    }

    pub fn add_volume(&self, vid: &VolumeId, config: VolumeConfig) -> Result<(), ClientErr> {
        Ok(self.storage.add_volume(vid, config).or_into_ctx()?)
    }

    /// Start a new read transaction at the latest local snapshot
    pub fn read_txn(&self, vid: &VolumeId) -> Result<ReadTxn, ClientErr> {
        let snapshot = self.snapshot(vid)?;
        Ok(ReadTxn::new(vid.clone(), snapshot, self.storage.clone()))
    }

    /// Start a new write transaction at the latest local snapshot
    pub fn write_txn(&self, vid: &VolumeId) -> Result<WriteTxn, ClientErr> {
        let snapshot = self
            .storage
            .snapshot(vid, SnapshotKind::Local)
            .or_into_ctx()?;
        Ok(WriteTxn::new(vid.clone(), snapshot, self.storage.clone()))
    }

    pub fn snapshot(&self, vid: &VolumeId) -> Result<Snapshot, ClientErr> {
        let snapshot = self
            .storage
            .snapshot(vid, SnapshotKind::Local)
            .or_into_ctx()?;
        match snapshot {
            Some(snapshot) => Ok(snapshot),
            None => todo!("fetch latest snapshot"),
        }
    }

    /// Subscribe to new local commits to volumes. This is a best effort
    /// channel, laggy consumers will receive RecvError::Lagged.
    pub fn subscribe_to_local_commits(&self) -> broadcast::Receiver<VolumeId> {
        self.storage.subscribe_to_local_commits()
    }

    /// Subscribe to new remote commits to volumes. This is a best effort
    /// channel, laggy consumers will receive RecvError::Lagged.
    pub fn subscribe_to_remote_commits(&self) -> broadcast::Receiver<VolumeId> {
        self.storage.subscribe_to_remote_commits()
    }
}

#[cfg(test)]
mod tests {
    use graft_core::{
        lsn::LSN,
        page::{Page, EMPTY_PAGE},
    };

    use crate::runtime::storage::StorageErr;

    use super::*;

    #[test]
    fn test_read_write_sanity() {
        let storage = Storage::open_temporary().unwrap();
        let handle = RuntimeHandle::new(storage);

        let vid = VolumeId::random();
        let page = Page::test_filled(0x42);
        let page2 = Page::test_filled(0x99);

        // open a read txn and verify that no pages are returned
        let txn = handle.read_txn(&vid).unwrap();
        assert_eq!(txn.read(0.into()).unwrap(), EMPTY_PAGE);

        // open a write txn and write a page, verify RYOW, then commit
        let mut txn = handle.write_txn(&vid).unwrap();
        txn.write(0.into(), page.clone());
        assert_eq!(txn.read(0.into()).unwrap(), page);
        let txn = txn.commit().unwrap();

        // verify the new read txn can read the page
        assert_eq!(txn.read(0.into()).unwrap(), page);

        // verify the snapshot
        let snapshot = txn.snapshot();
        assert_eq!(snapshot.lsn(), LSN::ZERO);
        assert_eq!(snapshot.page_count(), 1);

        // open a new write txn, verify it can read the page; write another page
        let mut txn = handle.write_txn(&vid).unwrap();
        assert_eq!(txn.read(0.into()).unwrap(), page);
        txn.write(1.into(), page2.clone());
        assert_eq!(txn.read(1.into()).unwrap(), page2);
        let txn = txn.commit().unwrap();

        // verify the new read txn can read both pages
        assert_eq!(txn.read(0.into()).unwrap(), page);
        assert_eq!(txn.read(1.into()).unwrap(), page2);

        // verify the snapshot
        let snapshot = txn.snapshot();
        assert_eq!(snapshot.lsn(), LSN::new(1));
        assert_eq!(snapshot.page_count(), 2);

        // upgrade to a write txn and overwrite the first page
        let mut txn = txn.upgrade();
        txn.write(0.into(), page2.clone());
        assert_eq!(txn.read(0.into()).unwrap(), page2);
        let txn = txn.commit().unwrap();

        // verify the new read txn can read the updated page
        assert_eq!(txn.read(0.into()).unwrap(), page2);

        // verify the snapshot
        let snapshot = txn.snapshot();
        assert_eq!(snapshot.lsn(), LSN::new(2));
        assert_eq!(snapshot.page_count(), 2);
    }

    #[test]
    fn test_concurrent_commit_err() {
        // open two write txns, commit the first, then commit the second

        let storage = Storage::open_temporary().unwrap();
        let handle = RuntimeHandle::new(storage);

        let vid = VolumeId::random();
        let page = Page::test_filled(0x42);

        let mut txn1 = handle.write_txn(&vid).unwrap();
        txn1.write(0.into(), page.clone());

        let mut txn2 = handle.write_txn(&vid).unwrap();
        txn2.write(0.into(), page.clone());

        let txn1 = txn1.commit().unwrap();
        assert_eq!(txn1.read(0.into()).unwrap(), page);

        // take a snapshot of the volume before committing txn2
        let pre_commit = handle.snapshot(&vid).unwrap();

        let txn2 = txn2.commit();
        assert!(matches!(
            txn2.expect_err("expected concurrent write error").ctx(),
            ClientErr::StorageErr(StorageErr::ConcurrentWrite)
        ));

        // ensure that txn2 did not commit by verifying the snapshot
        let snapshot = handle.snapshot(&vid).unwrap();
        assert_eq!(pre_commit, snapshot);
    }
}
