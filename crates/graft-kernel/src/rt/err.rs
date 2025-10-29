use crate::{local::fjall_storage::FjallStorageErr, remote::RemoteErr, volume_name::VolumeName};

#[derive(Debug, thiserror::Error)]
#[error("fatal runtime error")]
pub struct RuntimeFatalErr;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeErr {
    #[error(transparent)]
    Storage(#[from] FjallStorageErr),

    #[error(transparent)]
    Remote(#[from] RemoteErr),

    #[error("Named Volume `{0}` not found")]
    NamedVolumeNotFound(VolumeName),

    #[error("Named Volume `{0}` has a pending commit")]
    NamedVolumeNeedsRecovery(VolumeName),

    // String should be the output of `NamedVolumeState::sync_status`
    #[error("Named Volume `{0}` has no local changes to push; status={1}")]
    NamedVolumeNoChanges(VolumeName, String),

    // String should be the output of `NamedVolumeState::sync_status`
    #[error("Named Volume `{0}` has diverged from the remote; status={1}")]
    NamedVolumeDiverged(VolumeName, String),
}
