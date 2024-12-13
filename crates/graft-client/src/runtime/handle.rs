use std::{fmt::Debug, sync::Arc};

use graft_core::VolumeId;

use crate::ClientErr;

use super::{
    storage::Storage,
    txn::{ReadTxn, WriteTxn},
};

#[derive(Clone)]
pub struct RuntimeHandle {
    rt: Arc<RuntimeInner>,
}

impl RuntimeHandle {
    pub fn new(storage: Storage) -> Self {
        Self { rt: Arc::new(RuntimeInner::new(storage)) }
    }

    /// Start a new read transaction at the latest snapshot
    pub fn read_txn(&self, vid: VolumeId) -> Result<ReadTxn, ClientErr> {
        let snapshot = self.rt.storage.snapshot(vid.clone())?;
        Ok(ReadTxn::new(vid, snapshot, self.rt.clone()))
    }

    /// Start a new write transaction at the latest snapshot
    pub fn write_txn(&self, vid: VolumeId) -> Result<WriteTxn, ClientErr> {
        let read_tx = self.read_txn(vid)?;
        Ok(WriteTxn::new(read_tx))
    }
}

pub(crate) struct RuntimeInner {
    storage: Storage,
}

impl RuntimeInner {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }

    pub fn storage(&self) -> &Storage {
        &self.storage
    }
}

impl Debug for RuntimeInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Runtime")
    }
}

#[cfg(test)]
mod tests {
    use graft_core::page::{Page, EMPTY_PAGE};

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
        let txn = handle.read_txn(vid.clone()).unwrap();
        assert_eq!(txn.read(0.into()).unwrap(), EMPTY_PAGE);

        // open a write txn and write a page, verify RYOW, then commit
        let mut txn = handle.write_txn(vid.clone()).unwrap();
        txn.write(0.into(), page.clone());
        assert_eq!(txn.read(0.into()).unwrap(), page);
        let txn = txn.commit().unwrap();

        // verify the new read txn can read the page
        assert_eq!(txn.read(0.into()).unwrap(), page);

        // open a new write txn, verify it can read the page; write another page
        let mut txn = handle.write_txn(vid.clone()).unwrap();
        assert_eq!(txn.read(0.into()).unwrap(), page);
        txn.write(1.into(), page2.clone());
        assert_eq!(txn.read(1.into()).unwrap(), page2);
        let txn = txn.commit().unwrap();

        // verify the new read txn can read both pages
        assert_eq!(txn.read(0.into()).unwrap(), page);
        assert_eq!(txn.read(1.into()).unwrap(), page2);

        // upgrade to a write txn and overwrite the first page
        let mut txn = txn.upgrade().unwrap();
        txn.write(0.into(), page2.clone());
        assert_eq!(txn.read(0.into()).unwrap(), page2);
        let txn = txn.commit().unwrap();

        // verify the new read txn can read the updated page
        assert_eq!(txn.read(0.into()).unwrap(), page2);
    }

    #[test]
    fn test_concurrent_commit_err() {
        // open two write txns, commit the first, then commit the second

        let storage = Storage::open_temporary().unwrap();
        let handle = RuntimeHandle::new(storage);

        let vid = VolumeId::random();
        let page = Page::test_filled(0x42);

        let mut txn1 = handle.write_txn(vid.clone()).unwrap();
        txn1.write(0.into(), page.clone());

        let mut txn2 = handle.write_txn(vid.clone()).unwrap();
        txn2.write(0.into(), page.clone());

        let txn1 = txn1.commit().unwrap();
        assert_eq!(txn1.read(0.into()).unwrap(), page);

        let txn2 = txn2.commit();
        assert!(matches!(
            txn2.expect_err("expected concurrent write error"),
            ClientErr::StorageErr(StorageErr::ConcurrentWrite(_))
        ));
    }
}
