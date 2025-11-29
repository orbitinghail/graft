use crate::{local::fjall_storage::FjallStorageErr, remote::RemoteErr};
use graft_core::LogId;

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
    #[error("Concurrent write to Graft {0}")]
    GraftConcurrentWrite(LogId),

    #[error("Graft {0} not found")]
    GraftNotFound(LogId),

    #[error("Graft {0} has a pending commit and needs recovery")]
    GraftNeedsRecovery(LogId),

    #[error("Graft {0} has diverged from the remote")]
    GraftDiverged(LogId),

    #[error(
        "Graft `{graft}` has a different remote Log than expected; expected={expected}, actual={actual}"
    )]
    GraftRemoteMismatch {
        graft: LogId,
        expected: LogId,
        actual: LogId,
    },
}
