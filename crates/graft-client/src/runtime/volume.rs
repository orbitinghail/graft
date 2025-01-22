use crossbeam::channel::{bounded, Sender};
use culprit::{Result, ResultExt};
use graft_core::VolumeId;

use crate::{runtime::storage::snapshot::SnapshotKindMask, ClientErr};

use super::{
    fetcher::Fetcher,
    shared::Shared,
    snapshot::VolumeSnapshot,
    storage::{snapshot::SnapshotKind, volume_config::SyncDirection},
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
    pub fn snapshot(&self) -> Result<VolumeSnapshot, ClientErr> {
        let mask = SnapshotKindMask::default()
            .with(SnapshotKind::Local)
            .with(SnapshotKind::Remote);
        let mut set = self
            .shared
            .storage()
            .snapshots(&self.vid, mask)
            .or_into_ctx()?;
        Ok(VolumeSnapshot::new(
            self.vid.clone(),
            set.take_local().expect("local snapshot missing"),
            set.take_remote(),
        ))
    }

    /// Open a VolumeReader at the latest snapshot
    pub fn reader(&self) -> Result<VolumeReader<F>, ClientErr> {
        Ok(VolumeReader::new(self.snapshot()?, self.shared.clone()))
    }

    /// Open a VolumeReader at the provided snapshot
    pub fn reader_at(&self, snapshot: VolumeSnapshot) -> VolumeReader<F> {
        VolumeReader::new(snapshot, self.shared.clone())
    }

    /// Open a VolumeWriter at the latest snapshot
    pub fn writer(&self) -> Result<VolumeWriter<F>, ClientErr> {
        self.reader().map(VolumeWriter::from)
    }

    /// Open a VolumeWriter at the provided snapshot
    pub fn writer_at(&self, snapshot: VolumeSnapshot) -> VolumeWriter<F> {
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

    /// Sync this volume to the latest remote snapshot
    pub fn pull_from_remote(&self) -> Result<(), ClientErr> {
        let (tx, rx) = bounded(1);
        self.sync_control
            .as_ref()
            .expect("sync control channel missing")
            .send(SyncControl::new(self.vid.clone(), SyncDirection::Pull, tx))
            .expect("sync control channel closed");
        rx.recv()
            .expect("sync control response channel disconnected")
    }
}
