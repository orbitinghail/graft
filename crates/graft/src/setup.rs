use std::{future::pending, num::NonZero, path::PathBuf, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    local::fjall_storage::{FjallStorage, FjallStorageErr},
    remote::{RemoteConfig, RemoteErr},
    rt::runtime::Runtime,
};

#[derive(Debug, Deserialize, Serialize)]
pub struct GraftConfig {
    /// configuration for the Graft remote
    pub remote: RemoteConfig,

    /// the Graft data directory path
    pub data_dir: PathBuf,

    /// if set, specifies the autosync interval in seconds
    #[serde(default)]
    pub autosync: Option<NonZero<u64>>,
}

#[derive(Debug, Error)]
pub enum InitErr {
    #[error(transparent)]
    IoErr(#[from] std::io::Error),

    #[error(transparent)]
    Storage(#[from] FjallStorageErr),

    #[error(transparent)]
    Remote(#[from] RemoteErr),
}

/// An opinionated but simple setup method. Sets up a Tokio current thread
/// runtime on a background thread. Configures Graft using the provided config.
///
/// For more complex setups such as custom Tokio runtimes, it's recommended to
/// setup the Graft Runtime manually instead.
pub fn setup_graft(config: GraftConfig) -> Result<Runtime, InitErr> {
    // spin up a tokio current thread runtime in a new thread
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let tokio_handle = rt.handle().clone();
    std::thread::Builder::new()
        .name("graft-runtime".to_string())
        .spawn(move || {
            // run the tokio event loop in this thread
            rt.block_on(pending::<()>())
        })?;

    let remote = Arc::new(config.remote.build()?);
    let storage = Arc::new(FjallStorage::open(config.data_dir)?);
    let autosync = config.autosync.map(|s| Duration::from_secs(s.get()));
    Ok(Runtime::new(tokio_handle, remote, storage, autosync))
}
