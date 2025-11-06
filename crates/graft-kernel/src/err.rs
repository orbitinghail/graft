use crate::{
    local::fjall_storage::FjallStorageErr,
    remote::RemoteErr,
    volume_name::{VolumeName, VolumeNameErr},
};
use graft_core::VolumeId;

#[derive(Debug, thiserror::Error)]
pub enum KernelErr {
    #[error(transparent)]
    Storage(FjallStorageErr),

    #[error(transparent)]
    Remote(#[from] RemoteErr),

    #[error(transparent)]
    Volume(#[from] VolumeErr),
}

impl KernelErr {
    pub(crate) fn is_remote_not_found(&self) -> bool {
        if let KernelErr::Remote(err) = self {
            err.is_not_found()
        } else {
            false
        }
    }
}

impl From<FjallStorageErr> for KernelErr {
    fn from(value: FjallStorageErr) -> Self {
        match value {
            FjallStorageErr::VolumeErr(verr) => KernelErr::Volume(verr),
            other => KernelErr::Storage(other),
        }
    }
}

impl From<VolumeNameErr> for KernelErr {
    #[inline]
    fn from(value: VolumeNameErr) -> Self {
        KernelErr::Volume(VolumeErr::InvalidVolumeName(value))
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
