use std::sync::Arc;

use culprit::{Result, ResultExt};
use graft_core::VolumeId;

use crate::{ClientErr, ClientPair};

use super::{
    storage::{
        snapshot::Snapshot,
        volume_state::{SyncDirection, VolumeStatus},
        Storage,
    },
    sync::control::SyncRpc,
    volume_reader::VolumeReader,
    volume_writer::VolumeWriter,
};

#[derive(Clone, Debug)]
pub struct VolumeHandle {
    vid: VolumeId,
    clients: Arc<ClientPair>,
    storage: Arc<Storage>,
    sync_rpc: SyncRpc,
}

impl VolumeHandle {
    pub(crate) fn new(
        vid: VolumeId,
        clients: Arc<ClientPair>,
        storage: Arc<Storage>,
        sync_rpc: SyncRpc,
    ) -> Self {
        Self { vid, clients, storage, sync_rpc }
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    /// Retrieve the current volume status
    pub fn status(&self) -> Result<VolumeStatus, ClientErr> {
        self.storage.get_volume_status(&self.vid).or_into_ctx()
    }

    /// Retrieve the latest snapshot for the volume
    pub fn snapshot(&self) -> Result<Option<Snapshot>, ClientErr> {
        self.storage.snapshot(&self.vid).or_into_ctx()
    }

    /// Open a `VolumeReader` at the latest snapshot
    pub fn reader(&self) -> Result<VolumeReader, ClientErr> {
        Ok(VolumeReader::new(
            self.vid.clone(),
            self.snapshot()?,
            self.clients.clone(),
            self.storage.clone(),
        ))
    }

    /// Open a `VolumeReader` at the provided snapshot
    pub fn reader_at(&self, snapshot: Option<Snapshot>) -> VolumeReader {
        VolumeReader::new(
            self.vid.clone(),
            snapshot,
            self.clients.clone(),
            self.storage.clone(),
        )
    }

    /// Open a `VolumeWriter` at the latest snapshot
    pub fn writer(&self) -> Result<VolumeWriter, ClientErr> {
        self.reader().map(VolumeWriter::from)
    }

    /// Open a `VolumeWriter` at the provided snapshot
    pub fn writer_at(&self, snapshot: Option<Snapshot>) -> VolumeWriter {
        VolumeWriter::from(self.reader_at(snapshot))
    }

    /// Subscribe to remote commits to this Volume
    pub fn subscribe_to_remote_changes(&self) -> crossbeam::channel::Receiver<()> {
        self.storage.remote_changeset().subscribe(self.vid.clone())
    }

    /// Subscribe to local commits to this Volume
    pub fn subscribe_to_local_changes(&self) -> crossbeam::channel::Receiver<()> {
        self.storage.local_changeset().subscribe(self.vid.clone())
    }

    /// Sync this volume with the remote. This function blocks until the sync
    /// has completed, returning any error that occurs.
    pub fn sync_with_remote(&self, direction: SyncDirection) -> Result<(), ClientErr> {
        self.sync_rpc
            .sync(self.vid.clone(), direction)
            .or_into_ctx()
    }

    /// Reset this volume to the remote. This will cause all pending commits to
    /// be rolled back and the volume status to be cleared.
    pub fn reset_to_remote(&self) -> Result<(), ClientErr> {
        self.sync_rpc
            .reset_to_remote(self.vid.clone())
            .or_into_ctx()
    }
}
