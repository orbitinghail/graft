use crate::{local::fjall_storage::FjallStorageErr, remote::RemoteErr};

#[derive(Debug, thiserror::Error)]
#[error("fatal runtime error")]
pub struct RuntimeFatalErr;

#[derive(Debug, thiserror::Error)]
pub enum RuntimeErr {
    #[error(transparent)]
    Storage(#[from] FjallStorageErr),

    #[error(transparent)]
    Remote(#[from] RemoteErr),
}
