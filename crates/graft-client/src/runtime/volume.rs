use std::sync::Arc;

use culprit::{Result, ResultExt};
use graft_core::VolumeId;

use crate::ClientErr;

use super::{
    storage::{
        changeset::Subscriber,
        snapshot::{Snapshot, SnapshotKind},
        Storage,
    },
    txn::{ReadTxn, WriteTxn},
};

#[derive(Clone)]
pub struct VolumeHandle {
    vid: VolumeId,
    storage: Arc<Storage>,
}

impl VolumeHandle {
    pub(crate) fn new(vid: VolumeId, storage: Arc<Storage>) -> Self {
        Self { vid, storage }
    }

    /// Start a new read transaction at the latest local snapshot
    pub fn read_txn(&self) -> Result<ReadTxn, ClientErr> {
        Ok(ReadTxn::new(
            self.vid.clone(),
            self.snapshot()?,
            self.storage.clone(),
        ))
    }

    /// Start a new write transaction at the latest local snapshot
    pub fn write_txn(&self) -> Result<WriteTxn, ClientErr> {
        self.read_txn().map(WriteTxn::from)
    }

    /// Retrieve the latest local snapshot for the volume
    pub fn snapshot(&self) -> Result<Snapshot, ClientErr> {
        Ok(self
            .storage
            .snapshot(&self.vid, SnapshotKind::Local)
            .or_into_ctx()?
            .expect("VolumeHandle snapshot should not be missing"))
    }

    /// Subscribe to remote commits to this Volume
    pub fn subscribe_to_remote_changes(&self) -> Subscriber<VolumeId> {
        self.storage.remote_changeset().subscribe(self.vid.clone())
    }

    /// Subscribe to local commits to this Volume
    pub fn subscribe_to_local_changes(&self) -> Subscriber<VolumeId> {
        self.storage.local_changeset().subscribe(self.vid.clone())
    }
}
