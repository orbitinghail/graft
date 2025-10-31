use crate::{local::fjall_storage::FjallStorageErr, remote::RemoteErr, volume_name::VolumeName};
use graft_core::VolumeId;

#[derive(Debug, thiserror::Error)]
pub enum GraftErr {
    #[error(transparent)]
    Storage(FjallStorageErr),

    #[error(transparent)]
    Remote(#[from] RemoteErr),

    #[error(transparent)]
    Volume(#[from] VolumeErr),
}

impl From<FjallStorageErr> for GraftErr {
    fn from(value: FjallStorageErr) -> Self {
        match value {
            FjallStorageErr::VolumeErr(verr) => GraftErr::Volume(verr),
            other => GraftErr::Storage(other),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VolumeErr {
    #[error("Unknown Volume {0}")]
    VolumeNotFound(VolumeId),

    #[error("Concurrent write to Volume {0} detected")]
    ConcurrentWrite(VolumeId),

    #[error("Named Volume `{0}` not found")]
    NamedVolumeNotFound(VolumeName),

    #[error("Named Volume `{0}` has a pending commit and needs recovery")]
    NamedVolumeNeedsRecovery(VolumeName),

    // String should be the output of `NamedVolumeState::sync_status`
    #[error("Named Volume `{0}` has diverged from the remote; status={1}")]
    NamedVolumeDiverged(VolumeName, String),
}
