use crossbeam::channel::{bounded, Sender};
use culprit::{Result, ResultExt};
use graft_core::VolumeId;

use crate::ClientErr;

use super::{
    fetcher::Fetcher,
    shared::Shared,
    storage::{
        snapshot::Snapshot,
        volume_state::{SyncDirection, VolumeStatus},
    },
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

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    /// Retrieve the current volume status
    pub fn status(&self) -> Result<VolumeStatus, ClientErr> {
        Ok(self
            .shared
            .storage()
            .get_volume_status(&self.vid)
            .or_into_ctx()?)
    }

    /// Check if the Volume is syncing
    pub fn is_syncing(&self) -> Result<bool, ClientErr> {
        Ok(self
            .shared
            .storage()
            .watermarks(&self.vid)
            .or_into_ctx()?
            .is_syncing())
    }

    /// Retrieve the latest snapshot for the volume
    pub fn snapshot(&self) -> Result<Option<Snapshot>, ClientErr> {
        Ok(self.shared.storage().snapshot(&self.vid).or_into_ctx()?)
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
        VolumeReader::new(self.vid.clone(), Some(snapshot), self.shared.clone())
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
        self.control(SyncControl::Sync {
            vid: self.vid.clone(),
            direction,
            complete: tx,
        });
        rx.recv()
            .expect("sync control response channel disconnected")
    }

    /// Reset this volume to the remote. This will cause all pending commits to
    /// be rolled back and the volume status to be cleared.
    pub fn reset_to_remote(&self) -> Result<(), ClientErr> {
        let (tx, rx) = bounded(1);
        self.control(SyncControl::ResetToRemote { vid: self.vid.clone(), complete: tx });
        rx.recv()
            .expect("sync control response channel disconnected")
    }

    fn control(&self, msg: SyncControl) {
        self.sync_control
            .as_ref()
            .expect("sync control channel missing")
            .send(msg)
            .expect("sync control channel closed");
    }
}
