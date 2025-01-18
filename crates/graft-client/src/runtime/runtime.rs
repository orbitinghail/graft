use culprit::{Result, ResultExt};
use std::time::Duration;

use graft_core::VolumeId;

use crate::{runtime::storage::volume_config::SyncDirection, ClientErr, ClientPair};

use super::{
    fetcher::Fetcher,
    shared::Shared,
    storage::{snapshot::SnapshotKind, volume_config::VolumeConfig, Storage},
    sync::{SyncTask, SyncTaskHandle},
    volume::VolumeHandle,
};

pub struct Runtime<F> {
    shared: Shared<F>,
}

impl<F> Clone for Runtime<F> {
    fn clone(&self) -> Self {
        Self { shared: self.shared.clone() }
    }
}

impl<F: Fetcher> Runtime<F> {
    pub fn new(fetcher: F, storage: Storage) -> Self {
        Self { shared: Shared::new(fetcher, storage) }
    }

    pub fn start_sync_task(
        &self,
        clients: ClientPair,
        refresh_interval: Duration,
    ) -> SyncTaskHandle {
        SyncTask::spawn(self.shared.clone(), clients, refresh_interval)
    }

    pub fn open_volume(
        &self,
        vid: &VolumeId,
        config: VolumeConfig,
    ) -> Result<VolumeHandle<F>, ClientErr> {
        let storage = self.shared.storage();
        if config.sync().matches(SyncDirection::Pull) {
            self.shared
                .fetcher()
                .pull_snapshot(storage, vid)
                .or_into_ctx()?;
        }

        // if no local snapshot exists, create an empty one
        if storage
            .snapshot(&vid, SnapshotKind::Local)
            .or_into_ctx()?
            .is_none()
        {
            storage
                .commit(&vid, None, Default::default())
                .or_into_ctx()?;
        }

        storage.set_volume_config(&vid, config).or_into_ctx()?;

        Ok(VolumeHandle::new(vid.clone(), self.shared.clone()))
    }
}

#[cfg(test)]
mod tests {
    use graft_core::page::{Page, EMPTY_PAGE};

    use crate::runtime::{fetcher::MockFetcher, storage::StorageErr};

    use super::*;

    #[test]
    fn test_read_write_sanity() {
        let storage = Storage::open_temporary().unwrap();
        let runtime = Runtime::new(MockFetcher, storage);

        let vid = VolumeId::random();
        let page = Page::test_filled(0x42);
        let page2 = Page::test_filled(0x99);

        let handle = runtime
            .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
            .unwrap();

        // open a reader and verify that no pages are returned
        let reader = handle.reader().unwrap();
        assert_eq!(reader.read(0.into()).unwrap(), EMPTY_PAGE);

        // open a writer and write a page, verify RYOW, then commit
        let mut writer = handle.writer().unwrap();
        writer.write(0.into(), page.clone());
        assert_eq!(writer.read(0.into()).unwrap(), page);
        let reader = writer.commit().unwrap();

        // verify the new reader can read the page
        assert_eq!(reader.read(0.into()).unwrap(), page);

        // verify the snapshot
        let snapshot = reader.snapshot();
        assert_eq!(snapshot.local().lsn(), 1);
        assert_eq!(snapshot.local().page_count(), 1);

        // open a new writer, verify it can read the page; write another page
        let mut writer = handle.writer().unwrap();
        assert_eq!(writer.read(0.into()).unwrap(), page);
        writer.write(1.into(), page2.clone());
        assert_eq!(writer.read(1.into()).unwrap(), page2);
        let reader = writer.commit().unwrap();

        // verify the new reader can read both pages
        assert_eq!(reader.read(0.into()).unwrap(), page);
        assert_eq!(reader.read(1.into()).unwrap(), page2);

        // verify the snapshot
        let snapshot = reader.snapshot();
        assert_eq!(snapshot.local().lsn(), 2);
        assert_eq!(snapshot.local().page_count(), 2);

        // upgrade to a writer and overwrite the first page
        let mut writer = reader.upgrade();
        writer.write(0.into(), page2.clone());
        assert_eq!(writer.read(0.into()).unwrap(), page2);
        let reader = writer.commit().unwrap();

        // verify the new reader can read the updated page
        assert_eq!(reader.read(0.into()).unwrap(), page2);

        // verify the snapshot
        let snapshot = reader.snapshot();
        assert_eq!(snapshot.local().lsn(), 3);
        assert_eq!(snapshot.local().page_count(), 2);
    }

    #[test]
    fn test_concurrent_commit_err() {
        // open two writers, commit the first, then commit the second

        let storage = Storage::open_temporary().unwrap();
        let runtime = Runtime::new(MockFetcher, storage);

        let vid = VolumeId::random();
        let page = Page::test_filled(0x42);

        let handle = runtime
            .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
            .unwrap();

        let mut writer1 = handle.writer().unwrap();
        writer1.write(0.into(), page.clone());

        let mut writer2 = handle.writer().unwrap();
        writer2.write(0.into(), page.clone());

        let reader1 = writer1.commit().unwrap();
        assert_eq!(reader1.read(0.into()).unwrap(), page);

        // take a snapshot of the volume before committing txn2
        let pre_commit = handle.snapshot().unwrap();

        let result = writer2.commit();
        assert!(matches!(
            result.expect_err("expected concurrent write error").ctx(),
            ClientErr::StorageErr(StorageErr::ConcurrentWrite)
        ));

        // ensure that txn2 did not commit by verifying the snapshot
        let snapshot = handle.snapshot().unwrap();
        assert_eq!(pre_commit, snapshot);
    }
}
