use crate::{local::fjall_storage::FjallStorageErr, remote::RemoteErr, volume_err::VolumeErr};

#[derive(Debug, thiserror::Error)]
#[error("fatal runtime error")]
pub struct RuntimeFatalErr;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeErr {
    #[error(transparent)]
    Storage(FjallStorageErr),

    #[error(transparent)]
    Remote(#[from] RemoteErr),

    #[error(transparent)]
    Volume(#[from] VolumeErr),
}

impl From<FjallStorageErr> for RuntimeErr {
    fn from(value: FjallStorageErr) -> Self {
        match value {
            FjallStorageErr::VolumeErr(verr) => RuntimeErr::Volume(verr),
            other => RuntimeErr::Storage(other),
        }
    }
}
