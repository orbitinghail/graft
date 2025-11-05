use crate::{
    local::fjall_storage::FjallStorageErr,
    remote::RemoteErr,
    volume_name::{VolumeName, VolumeNameErr},
};
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

impl GraftErr {
    pub(crate) fn is_remote_not_found(&self) -> bool {
        if let GraftErr::Remote(err) = self {
            err.is_not_found()
        } else {
            false
        }
    }
}

impl From<FjallStorageErr> for GraftErr {
    fn from(value: FjallStorageErr) -> Self {
        match value {
            FjallStorageErr::VolumeErr(verr) => GraftErr::Volume(verr),
            other => GraftErr::Storage(other),
        }
    }
}

impl From<VolumeNameErr> for GraftErr {
    #[inline]
    fn from(value: VolumeNameErr) -> Self {
        GraftErr::Volume(VolumeErr::InvalidVolumeName(value))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VolumeErr {
    #[error("Unknown Volume {0}")]
    VolumeNotFound(VolumeId),

    #[error("Concurrent write to Volume {0} detected")]
    ConcurrentWrite(VolumeId),

    #[error("Invalid Volume Name")]
    InvalidVolumeName(#[from] VolumeNameErr),

    #[error("Named Volume `{0}` not found")]
    NamedVolumeNotFound(VolumeName),

    #[error("Named Volume `{0}` has a pending commit and needs recovery")]
    NamedVolumeNeedsRecovery(VolumeName),

    #[error("Named Volume `{0}` has diverged from the remote")]
    NamedVolumeDiverged(VolumeName),

    #[error(
        "Named Volume `{name}` has a different remote Volume than expected; expected={expected}, actual={actual}"
    )]
    NamedVolumeRemoteMismatch {
        name: VolumeName,
        expected: VolumeId,
        actual: VolumeId,
    },
}
