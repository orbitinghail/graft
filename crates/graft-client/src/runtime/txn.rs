use culprit::{Result, ResultExt};
use std::sync::Arc;

use graft_core::{
    page::{Page, EMPTY_PAGE},
    page_offset::PageOffset,
    VolumeId,
};

use crate::ClientErr;

use super::storage::{memtable::Memtable, page::PageValue, snapshot::Snapshot, Storage};

#[derive(Clone, Debug)]
pub struct ReadTxn {
    vid: VolumeId,
    snapshot: Snapshot,
    storage: Arc<Storage>,
}

impl ReadTxn {
    pub(crate) fn new(vid: VolumeId, snapshot: Snapshot, storage: Arc<Storage>) -> Self {
        Self { vid, snapshot, storage }
    }

    /// Return the volume ID for this transaction
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    /// Return the snapshot for this transaction
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }

    /// Read a page from the snapshot
    pub fn read(&self, offset: PageOffset) -> Result<Page, ClientErr> {
        // TODO:
        // return None if offset is out of range OR we don't have a snapshot

        match self
            .storage
            .read(&self.vid, self.snapshot.lsn(), offset)
            .or_into_ctx()?
        {
            (_, PageValue::Available(page)) => Ok(page),
            (_, PageValue::Empty) => Ok(EMPTY_PAGE),
            (_, PageValue::Pending) => {
                // When this is fixed, update the test:
                // graft-test/tests/sync.rs
                todo!("download page from remote")
            }
        }
    }

    // Upgrade this read transaction into a write transaction.
    pub fn upgrade(self) -> WriteTxn {
        self.into()
    }
}

#[derive(Debug)]
pub struct WriteTxn {
    read_txn: ReadTxn,
    memtable: Memtable,
}

impl From<ReadTxn> for WriteTxn {
    #[inline]
    fn from(read_txn: ReadTxn) -> Self {
        WriteTxn { read_txn, memtable: Default::default() }
    }
}

impl WriteTxn {
    /// Returns the volume ID for this transaction
    #[inline]
    pub fn vid(&self) -> &VolumeId {
        self.read_txn.vid()
    }

    /// Returns the snapshot backing this transaction
    #[inline]
    pub fn snapshot(&self) -> &Snapshot {
        self.read_txn.snapshot()
    }

    /// Read a page; supports read your own writes (RYOW)
    pub fn read(&self, offset: PageOffset) -> Result<Page, ClientErr> {
        if let Some(page) = self.memtable.get(offset) {
            return Ok(page.clone());
        }
        self.read_txn.read(offset)
    }

    /// Write a page
    pub fn write(&mut self, offset: PageOffset, page: Page) {
        self.memtable.insert(offset, page);
    }

    /// Commit the transaction
    pub fn commit(self) -> Result<ReadTxn, ClientErr> {
        let Self {
            read_txn: ReadTxn { vid, snapshot, storage },
            memtable,
        } = self;
        let snapshot = storage
            .commit(&vid, Some(snapshot), memtable)
            .or_into_ctx()?;
        Ok(ReadTxn::new(vid, snapshot, storage))
    }
}
