use std::{collections::BTreeMap, sync::Arc};

use graft_core::{lsn::LSN, page_offset::PageOffset, page::Page, VolumeId};

use crate::ClientErr;

use super::storage::Storage;

pub type Result<T> = std::result::Result<T, ClientErr>;

pub struct Snapshot {
    lsn: LSN,
    checkpoint_lsn: LSN,
    page_count: u32,
}

#[derive(Clone)]
pub struct RuntimeHandle<S> {
    inner: Arc<RuntimeInner<S>>,
}

struct RuntimeInner<S> {
    storage: S,
}

impl<S: Storage> RuntimeHandle<S> {
    pub fn new(storage: S) -> Self {
        Self {
            inner: Arc::new(RuntimeInner { storage }),
        }
    }

    /// Return the latest snapshot for a volume
    pub fn snapshot(&self, vid: VolumeId) -> Result<Option<Snapshot>> {
        todo!()
    }

    /// Return a specific page at a specific LSN
    pub fn read(&self, vid: VolumeId, lsn: LSN, offset: PageOffset) -> Result<Page> {
        todo!()
    }

    /// Start a new write transaction at the Volume's last known snapshot
    pub fn transaction(&self, vid: VolumeId) -> Result<Transaction<S>> {
        Ok(Transaction {
            snapshot: self.snapshot(vid)?,
            memtable: Default::default(),
            inner: self.inner.clone(),
        })
    }
}

pub struct Transaction<S> {
    snapshot: Option<Snapshot>,
    memtable: BTreeMap<PageOffset, Page>,
    inner: Arc<RuntimeInner<S>>,
}

impl<S: Storage> Transaction<S> {
    /// Read a page
    pub fn read(&self, offset: PageOffset) -> Result<Page> {
        if let Some(page) = self.memtable.get(&offset) {
            return Ok(page.clone());
        }
        todo!()
    }

    /// Write a page
    pub fn write(&mut self, offset: PageOffset, page: Page) {
        self.memtable.insert(offset, page);
    }

    /// Commit the transaction
    pub fn commit(self) -> Result<Snapshot> {
        todo!()
    }
}
