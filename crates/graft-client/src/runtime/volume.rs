use crossbeam::channel::{bounded, Sender};
use culprit::{Result, ResultExt};
use graft_core::VolumeId;

use crate::ClientErr;

use super::{
    fetcher::Fetcher,
    shared::Shared,
    storage::{snapshot::Snapshot, volume_state::SyncDirection},
    sync::SyncControl,
    volume_reader::VolumeReader,
    volume_writer::VolumeWriter,
};

#[derive(Clone)]
pub struct VolumeHandle<F> {
    vid: VolumeId,
    shared: Shared<F>,
    sync_control: Option<Sender<SyncControl>>,
}

impl<F: Fetcher> VolumeHandle<F> {
    pub(crate) fn new(
        vid: VolumeId,
        shared: Shared<F>,
        sync_control: Option<Sender<SyncControl>>,
    ) -> Self {
        Self { vid, shared, sync_control }
    }

    /// Retrieve the latest snapshot for the volume
    pub fn snapshot(&self) -> Result<Snapshot, ClientErr> {
        Ok(self
            .shared
            .storage()
            .snapshot(&self.vid)
            .or_into_ctx()?
            .expect("snapshot missing"))
    }

    /// Open a VolumeReader at the latest snapshot
    pub fn reader(&self) -> Result<VolumeReader<F>, ClientErr> {
        Ok(VolumeReader::new(
            self.vid.clone(),
            self.snapshot()?,
            self.shared.clone(),
        ))
    }

    /// Open a VolumeReader at the provided snapshot
    pub fn reader_at(&self, snapshot: Snapshot) -> VolumeReader<F> {
        VolumeReader::new(self.vid.clone(), snapshot, self.shared.clone())
    }

    /// Open a VolumeWriter at the latest snapshot
    pub fn writer(&self) -> Result<VolumeWriter<F>, ClientErr> {
        self.reader().map(VolumeWriter::from)
    }

    /// Open a VolumeWriter at the provided snapshot
    pub fn writer_at(&self, snapshot: Snapshot) -> VolumeWriter<F> {
        VolumeWriter::from(self.reader_at(snapshot))
    }

    /// Subscribe to remote commits to this Volume
    pub fn subscribe_to_remote_changes(&self) -> crossbeam::channel::Receiver<()> {
        self.shared
            .storage()
            .remote_changeset()
            .subscribe(self.vid.clone())
    }

    /// Subscribe to local commits to this Volume
    pub fn subscribe_to_local_changes(&self) -> crossbeam::channel::Receiver<()> {
        self.shared
            .storage()
            .local_changeset()
            .subscribe(self.vid.clone())
    }

    /// Sync this volume with the remote. This function blocks until the sync
    /// has completed, returning any error that occurs.
    pub fn sync_with_remote(&self, direction: SyncDirection) -> Result<(), ClientErr> {
        let (tx, rx) = bounded(1);
        self.sync_control
            .as_ref()
            .expect("sync control channel missing")
            .send(SyncControl::new(self.vid.clone(), direction, tx))
            .expect("sync control channel closed");
        rx.recv()
            .expect("sync control response channel disconnected")
    }
}
