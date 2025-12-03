use crate::core::{LogId, VolumeId};
use crate::{local::fjall_storage::FjallStorageErr, remote::RemoteErr};

#[derive(Debug, thiserror::Error)]
pub enum KernelErr {
    #[error(transparent)]
    Storage(FjallStorageErr),

    #[error(transparent)]
    Remote(#[from] RemoteErr),

    #[error(transparent)]
    Logical(#[from] LogicalErr),
}

impl From<FjallStorageErr> for KernelErr {
    fn from(value: FjallStorageErr) -> Self {
        match value {
            FjallStorageErr::LogicalErr(verr) => KernelErr::Logical(verr),
            other => KernelErr::Storage(other),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LogicalErr {
    #[error("Concurrent write to Volume {0}")]
    VolumeConcurrentWrite(VolumeId),

    #[error("Volume {0} not found")]
    VolumeNotFound(VolumeId),

    #[error("Volume {0} has a pending commit and needs recovery")]
    VolumeNeedsRecovery(VolumeId),

    #[error("Volume {0} has diverged from the remote")]
    VolumeDiverged(VolumeId),

    #[error(
        "Volume `{vid}` has a different remote Log than expected; expected={expected}, actual={actual}"
    )]
    VolumeRemoteMismatch {
        vid: VolumeId,
        expected: LogId,
        actual: LogId,
    },
}
