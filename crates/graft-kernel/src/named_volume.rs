use bilrost::Message;
use culprit::Culprit;

use crate::{
    local::fjall_storage::FjallStorageErr, rt::runtime_handle::RuntimeHandle,
    volume_name::VolumeName, volume_reader::VolumeReader, volume_writer::VolumeWriter,
};
use graft_core::{VolumeId, commit_hash::CommitHash, lsn::LSN, volume_ref::VolumeRef};

#[derive(Debug, Clone, Message, PartialEq, Eq, Default)]
pub struct NamedVolumeState {
    /// The Volume name
    #[bilrost(1)]
    name: VolumeName,

    /// Reference to the latest synchronization point for the local Volume.
    #[bilrost(2)]
    local: VolumeRef,

    /// Reference to the latest synchronization point for the remote Volume.
    #[bilrost(3)]
    remote: Option<VolumeRef>,

    /// Presence of the `pending_commit` field means that the Push operation is in
    /// the process of committing to the remote. If no such Push job is currently
    /// running (i.e. it was interrupted), this field must be used to resume or
    /// abort the commit process.
    #[bilrost(4)]
    pending_commit: Option<PendingCommit>,
}

#[derive(Debug, Clone, Message, PartialEq, Eq)]
pub struct PendingCommit {
    /// The resulting remote LSN that the push job is attempting to create.
    #[bilrost(1)]
    remote_lsn: LSN,

    /// The associated commit hash. This is used to determine whether or not the
    /// commit has landed in the remote, in the case that we are interrupted
    /// while attempting to push.
    #[bilrost(2)]
    commit_hash: CommitHash,
}

impl NamedVolumeState {
    pub fn new(
        name: VolumeName,
        local: VolumeRef,
        remote: Option<VolumeRef>,
        pending_commit: Option<PendingCommit>,
    ) -> Self {
        Self { name, local, remote, pending_commit }
    }
}

pub struct NamedVolume {
    runtime: RuntimeHandle,
    name: VolumeName,
}

impl NamedVolume {
    pub(crate) fn new(runtime: RuntimeHandle, name: VolumeName) -> Self {
        Self { runtime, name }
    }

    pub fn volume_reader(&self, vid: &VolumeId) -> Result<VolumeReader, Culprit<FjallStorageErr>> {
        let snapshot = self.runtime.storage().read().snapshot(vid)?;
        Ok(VolumeReader::new(self.runtime.clone(), snapshot))
    }

    pub fn volume_writer(&self, vid: &VolumeId) -> Result<VolumeWriter, Culprit<FjallStorageErr>> {
        let read = self.runtime.storage().read();
        let snapshot = read.snapshot(vid)?;
        let page_count = read.page_count(&snapshot)?;
        Ok(VolumeWriter::new(
            self.runtime.clone(),
            snapshot,
            page_count,
        ))
    }
}
