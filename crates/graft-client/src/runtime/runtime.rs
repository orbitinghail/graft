use culprit::{Culprit, Result, ResultExt};
use std::{sync::Arc, time::Duration};
use tryiter::{TryIterator, TryIteratorExt};

use graft_core::{VolumeId, gid::ClientId};

use crate::{ClientErr, ClientPair};

use super::{
    storage::{
        Storage,
        volume_state::{VolumeConfig, VolumeState},
    },
    sync::{ShutdownErr, StartupErr, SyncTaskHandle},
    volume_handle::VolumeHandle,
};

#[derive(Clone)]
pub struct Runtime {
    cid: ClientId,
    clients: Arc<ClientPair>,
    storage: Arc<Storage>,
    sync: SyncTaskHandle,
}

impl Runtime {
    pub fn new(cid: ClientId, clients: ClientPair, storage: Storage) -> Self {
        Self {
            cid,
            clients: Arc::new(clients),
            storage: Arc::new(storage),
            sync: SyncTaskHandle::default(),
        }
    }

    pub fn start_sync_task(
        &self,
        refresh_interval: Duration,
        control_channel_size: usize,
        autosync: bool,
        thread_name: &str,
    ) -> Result<(), StartupErr> {
        self.sync.spawn(
            self.cid.clone(),
            self.storage.clone(),
            self.clients.clone(),
            refresh_interval,
            control_channel_size,
            autosync,
            thread_name,
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
        self.storage
            .iter_volumes()
            .map_err(|e| e.map_ctx(ClientErr::StorageErr))
    }

    pub fn open_volume(
        &self,
        vid: &VolumeId,
        config: VolumeConfig,
    ) -> Result<VolumeHandle, ClientErr> {
        self.storage.set_volume_config(vid, config).or_into_ctx()?;

        Ok(VolumeHandle::new(
            vid.clone(),
            self.clients.clone(),
            self.storage.clone(),
            self.sync.rpc(),
        ))
    }

    pub fn update_volume_config<U>(&self, vid: &VolumeId, f: U) -> Result<(), ClientErr>
    where
        U: FnMut(VolumeConfig) -> VolumeConfig,
    {
        self.storage.update_volume_config(vid, f).or_into_ctx()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use graft_core::{
        page::{EMPTY_PAGE, Page},
        pageidx,
    };

    use crate::{
        oracle::NoopOracle,
        runtime::{
            storage::{StorageErr, volume_state::SyncDirection},
            volume_reader::VolumeRead,
            volume_writer::VolumeWrite,
        },
    };

    use super::*;

    #[graft_test::test]
    fn test_read_write_sanity() {
        let cid = ClientId::random();
        let storage = Storage::open_temporary().unwrap();
        let runtime = Runtime::new(cid, ClientPair::test_empty(), storage);
        let mut oracle = NoopOracle;

        let vid = VolumeId::random();
        let page = Page::test_filled(0x42);
        let page2 = Page::test_filled(0x99);

        let handle = runtime
            .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
            .unwrap();

        // open a reader and verify that no pages are returned
        let reader = handle.reader().unwrap();
        assert_eq!(reader.snapshot(), None);
        assert_eq!(reader.read(&mut oracle, pageidx!(1)).unwrap(), EMPTY_PAGE);

        // open a writer and write a page, verify RYOW, then commit
        let mut writer = handle.writer().unwrap();
        writer.write(pageidx!(1), page.clone());
        assert_eq!(writer.read(&mut oracle, pageidx!(1)).unwrap(), page);
        let reader = writer.commit().unwrap();

        // verify the new reader can read the page
        assert_eq!(reader.read(&mut oracle, pageidx!(1)).unwrap(), page);

        // verify the snapshot
        let snapshot = reader.snapshot().unwrap();
        assert_eq!(snapshot.local(), 1);
        assert_eq!(snapshot.pages(), 1);

        // open a new writer, verify it can read the page; write another page
        let mut writer = handle.writer().unwrap();
        assert_eq!(writer.read(&mut oracle, pageidx!(1)).unwrap(), page);
        writer.write(pageidx!(2), page2.clone());
        assert_eq!(writer.read(&mut oracle, pageidx!(2)).unwrap(), page2);
        let reader = writer.commit().unwrap();

        // verify the new reader can read both pages
        assert_eq!(reader.read(&mut oracle, pageidx!(1)).unwrap(), page);
        assert_eq!(reader.read(&mut oracle, pageidx!(2)).unwrap(), page2);

        // verify the snapshot
        let snapshot = reader.snapshot().unwrap();
        assert_eq!(snapshot.local(), 2);
        assert_eq!(snapshot.pages(), 2);

        // upgrade to a writer and overwrite the first page
        let mut writer = reader.upgrade();
        writer.write(pageidx!(1), page2.clone());
        assert_eq!(writer.read(&mut oracle, pageidx!(1)).unwrap(), page2);
        let reader = writer.commit().unwrap();

        // verify the new reader can read the updated page
        assert_eq!(reader.read(&mut oracle, pageidx!(1)).unwrap(), page2);

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
        let runtime = Runtime::new(cid, ClientPair::test_empty(), storage);
        let mut oracle = NoopOracle;

        let vid = VolumeId::random();
        let page = Page::test_filled(0x42);

        let handle = runtime
            .open_volume(&vid, VolumeConfig::new(SyncDirection::Both))
            .unwrap();

        let mut writer1 = handle.writer().unwrap();
        writer1.write(pageidx!(1), page.clone());

        let mut writer2 = handle.writer().unwrap();
        writer2.write(pageidx!(1), page.clone());

        let reader1 = writer1.commit().unwrap();
        assert_eq!(reader1.read(&mut oracle, pageidx!(1)).unwrap(), page);

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
