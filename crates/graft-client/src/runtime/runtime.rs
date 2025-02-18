use culprit::{Culprit, Result, ResultExt};
use std::time::Duration;
use tryiter::{TryIterator, TryIteratorExt};

use graft_core::{gid::ClientId, VolumeId};

use crate::{ClientErr, ClientPair};

use super::{
    fetcher::Fetcher,
    shared::Shared,
    storage::{
        volume_state::{VolumeConfig, VolumeState},
        Storage,
    },
    sync::{ShutdownErr, StartupErr, SyncTaskHandle},
    volume_handle::VolumeHandle,
};

#[derive(Clone)]
pub struct Runtime {
    shared: Shared,
    sync: SyncTaskHandle,
}

impl Runtime {
    pub fn new(cid: ClientId, fetcher: impl Fetcher, storage: Storage) -> Self {
        let fetcher = Box::new(fetcher);
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
        autosync: bool,
    ) -> Result<(), StartupErr> {
        self.sync.spawn(
            self.shared.clone(),
            clients,
            refresh_interval,
            control_channel_size,
            autosync,
        )
    }

    pub fn shutdown_sync_task(&self, timeout: Duration) -> Result<(), ShutdownErr> {
        self.sync.shutdown_timeout(timeout)
    }

    pub fn get_autosync(&self) -> bool {
        self.sync.rpc().get_autosync()
    }

    pub fn set_autosync(&self, autosync: bool) {
        self.sync.rpc().set_autosync(autosync)
    }

    pub fn iter_volumes(&self) -> impl TryIterator<Ok = VolumeState, Err = Culprit<ClientErr>> {
        self.shared
            .storage()
            .iter_volumes()
            .map_err(|e| e.map_ctx(|c| ClientErr::StorageErr(c)))
    }

    pub fn open_volume(
        &self,
        vid: &VolumeId,
        config: VolumeConfig,
    ) -> Result<VolumeHandle, ClientErr> {
        self.shared
            .storage()
            .set_volume_config(&vid, config)
            .or_into_ctx()?;

        Ok(VolumeHandle::new(
            vid.clone(),
            self.shared.clone(),
            self.sync.rpc(),
        ))
    }

    pub fn update_volume_config<U>(&self, vid: &VolumeId, f: U) -> Result<(), ClientErr>
    where
        U: FnMut(VolumeConfig) -> VolumeConfig,
    {
        self.shared
            .storage()
            .update_volume_config(&vid, f)
            .or_into_ctx()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use graft_core::page::{Page, EMPTY_PAGE};

    use crate::runtime::{
        fetcher::MockFetcher,
        storage::{volume_state::SyncDirection, StorageErr},
        volume_reader::VolumeRead,
        volume_writer::VolumeWrite,
    };

    use super::*;

    #[graft_test::test]
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
        assert_eq!(reader.read(0).unwrap(), EMPTY_PAGE);

        // open a writer and write a page, verify RYOW, then commit
        let mut writer = handle.writer().unwrap();
        writer.write(0, page.clone());
        assert_eq!(writer.read(0).unwrap(), page);
        let reader = writer.commit().unwrap();

        // verify the new reader can read the page
        assert_eq!(reader.read(0).unwrap(), page);

        // verify the snapshot
        let snapshot = reader.snapshot().unwrap();
        assert_eq!(snapshot.local(), 1);
        assert_eq!(snapshot.pages(), 1);

        // open a new writer, verify it can read the page; write another page
        let mut writer = handle.writer().unwrap();
        assert_eq!(writer.read(0).unwrap(), page);
        writer.write(1, page2.clone());
        assert_eq!(writer.read(1).unwrap(), page2);
        let reader = writer.commit().unwrap();

        // verify the new reader can read both pages
        assert_eq!(reader.read(0).unwrap(), page);
        assert_eq!(reader.read(1).unwrap(), page2);

        // verify the snapshot
        let snapshot = reader.snapshot().unwrap();
        assert_eq!(snapshot.local(), 2);
        assert_eq!(snapshot.pages(), 2);

        // upgrade to a writer and overwrite the first page
        let mut writer = reader.upgrade();
        writer.write(0, page2.clone());
        assert_eq!(writer.read(0).unwrap(), page2);
        let reader = writer.commit().unwrap();

        // verify the new reader can read the updated page
        assert_eq!(reader.read(0).unwrap(), page2);

        // verify the snapshot
        let snapshot = reader.snapshot().unwrap();
        assert_eq!(snapshot.local(), 3);
        assert_eq!(snapshot.pages(), 2);
    }

    #[graft_test::test]
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
        writer1.write(0, page.clone());

        let mut writer2 = handle.writer().unwrap();
        writer2.write(0, page.clone());

        let reader1 = writer1.commit().unwrap();
        assert_eq!(reader1.read(0).unwrap(), page);

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
