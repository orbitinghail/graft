use std::sync::Arc;

use culprit::{Result, ResultExt};
use graft_core::VolumeId;

use crate::{runtime::storage::snapshot::SnapshotKindMask, ClientErr};

use super::{
    snapshot::VolumeSnapshot,
    storage::{snapshot::SnapshotKind, Storage},
    volume_reader::VolumeReader,
    volume_writer::VolumeWriter,
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

    /// Retrieve the latest snapshot for the volume
    pub fn snapshot(&self) -> Result<VolumeSnapshot, ClientErr> {
        let mask = SnapshotKindMask::default()
            .with(SnapshotKind::Local)
            .with(SnapshotKind::Remote);
        let mut set = self.storage.snapshots(&self.vid, mask).or_into_ctx()?;
        Ok(VolumeSnapshot::new(
            self.vid.clone(),
            set.take_local().expect("local snapshot missing"),
            set.take_remote(),
        ))
    }

    /// Open a VolumeReader at the latest snapshot
    pub fn reader(&self) -> Result<VolumeReader, ClientErr> {
        Ok(VolumeReader::new(self.snapshot()?, self.storage.clone()))
    }

    /// Open a VolumeReader at the provided snapshot
    pub fn reader_at(&self, snapshot: VolumeSnapshot) -> VolumeReader {
        VolumeReader::new(snapshot, self.storage.clone())
    }

    /// Open a VolumeWriter at the latest snapshot
    pub fn writer(&self) -> Result<VolumeWriter, ClientErr> {
        self.reader().map(VolumeWriter::from)
    }

    /// Open a VolumeWriter at the provided snapshot
    pub fn writer_at(&self, snapshot: VolumeSnapshot) -> VolumeWriter {
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
}
