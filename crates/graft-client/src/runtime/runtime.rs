use culprit::{Result, ResultExt};
use std::time::Duration;

use graft_core::{gid::ClientId, VolumeId};

use crate::{ClientErr, ClientPair};

use super::{
    fetcher::Fetcher,
    shared::Shared,
    storage::{volume_state::VolumeConfig, Storage},
    sync::{ShutdownErr, StartupErr, SyncTaskHandle},
    volume::VolumeHandle,
};

pub struct Runtime<F> {
    shared: Shared<F>,
    sync: SyncTaskHandle,
}

impl<F> Clone for Runtime<F> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            sync: self.sync.clone(),
        }
    }
}

impl<F: Fetcher> Runtime<F> {
    pub fn new(cid: ClientId, fetcher: F, storage: Storage) -> Self {
        Self {
            shared: Shared::new(cid, fetcher, storage),
            sync: SyncTaskHandle::default(),
        }
    }

    pub fn start_sync_task(
        &self,
        clients: ClientPair,
        refresh_interval: Duration,
        control_channel_size: usize,
    ) -> Result<(), StartupErr> {
        self.sync.spawn(
            self.shared.clone(),
            clients,
            refresh_interval,
            control_channel_size,
        )
    }

    pub fn shutdown_sync_task(&self, timeout: Duration) -> Result<(), ShutdownErr> {
        self.sync.shutdown_timeout(timeout)
    }

    pub fn open_volume(
        &self,
        vid: &VolumeId,
        config: VolumeConfig,
    ) -> Result<VolumeHandle<F>, ClientErr> {
        self.shared
            .storage()
            .set_volume_config(&vid, config)
            .or_into_ctx()?;

        Ok(VolumeHandle::new(
            vid.clone(),
            self.shared.clone(),
            self.sync.control(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use graft_core::page::{Page, EMPTY_PAGE};

    use crate::runtime::{
        fetcher::MockFetcher,
        storage::{volume_state::SyncDirection, StorageErr},
    };

    use super::*;

    #[test]
    fn test_read_write_sanity() {
        let cid = ClientId::random();
        let storage = Storage::open_temporary().unwrap();
        let runtime = Runtime::new(cid, MockFetcher, storage);

        let vid = VolumeId::random();
        let page = Page::test_filled(0x42);
        let page2 = Page::test_filled(0x99);

        let handle = runtime
            .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
            .unwrap();

        // open a reader and verify that no pages are returned
        let reader = handle.reader().unwrap();
        assert_eq!(reader.snapshot(), None);
        assert_eq!(reader.read(0.into()).unwrap(), EMPTY_PAGE);

        // open a writer and write a page, verify RYOW, then commit
        let mut writer = handle.writer().unwrap();
        writer.write(0.into(), page.clone());
        assert_eq!(writer.read(0.into()).unwrap(), page);
        let reader = writer.commit().unwrap();

        // verify the new reader can read the page
        assert_eq!(reader.read(0.into()).unwrap(), page);

        // verify the snapshot
        let snapshot = reader.snapshot().unwrap();
        assert_eq!(snapshot.local(), 1);
        assert_eq!(snapshot.pages(), 1);

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
        let snapshot = reader.snapshot().unwrap();
        assert_eq!(snapshot.local(), 2);
        assert_eq!(snapshot.pages(), 2);

        // upgrade to a writer and overwrite the first page
        let mut writer = reader.upgrade();
        writer.write(0.into(), page2.clone());
        assert_eq!(writer.read(0.into()).unwrap(), page2);
        let reader = writer.commit().unwrap();

        // verify the new reader can read the updated page
        assert_eq!(reader.read(0.into()).unwrap(), page2);

        // verify the snapshot
        let snapshot = reader.snapshot().unwrap();
        assert_eq!(snapshot.local(), 3);
        assert_eq!(snapshot.pages(), 2);
    }

    #[test]
    fn test_concurrent_commit_err() {
        // open two writers, commit the first, then commit the second

        let cid = ClientId::random();
        let storage = Storage::open_temporary().unwrap();
        let runtime = Runtime::new(cid, MockFetcher, storage);

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
