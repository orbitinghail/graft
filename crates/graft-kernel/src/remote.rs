use std::{path::PathBuf, sync::Arc};

use culprit::Culprit;
use graft_core::lsn::LSN;
use object_store::{
    ObjectStore, aws::S3ConditionalPut, local::LocalFileSystem, memory::InMemory, path::Path,
    prefix::PrefixStore,
};
use thiserror::Error;

pub mod segment;

#[derive(Debug, Default)]
pub enum RemoteConfig {
    /// In memory object store
    #[default]
    Memory,

    /// On disk object store
    Fs { root: PathBuf },

    /// S3 compatible object store
    /// Can load most config and secrets from environment variables
    /// See `object_store::aws::builder::AmazonS3Builder` for env variable names
    S3Compatible {
        bucket: String,
        prefix: Option<String>,
    },
}

#[derive(Error, Debug)]
pub enum RemoteErr {
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Invalid path: {0}")]
    Path(#[from] object_store::path::Error),
}

pub struct Remote {
    store: Arc<dyn ObjectStore>,
}

impl Remote {
    pub fn new(config: RemoteConfig) -> Result<Self, Culprit<RemoteErr>> {
        let store: Arc<dyn ObjectStore> = match config {
            RemoteConfig::Memory => Arc::new(InMemory::new()),
            RemoteConfig::Fs { root } => Arc::new(LocalFileSystem::new_with_prefix(root)?),
            RemoteConfig::S3Compatible { bucket, prefix } => {
                let store = object_store::aws::AmazonS3Builder::from_env()
                    .with_allow_http(true)
                    .with_bucket_name(bucket)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .build()?;
                if let Some(prefix) = prefix {
                    let prefix = Path::parse(prefix)?;
                    Arc::new(PrefixStore::new(store, prefix))
                } else {
                    Arc::new(store)
                }
            }
        };

        Ok(Self { store: store })
    }

    pub fn fetch_segment_frame(&self) {}

    pub fn fetch_control(&self) {}

    pub fn fetch_checkpoints(&self, etag: &[u8]) {}

    // lsn range may be unbounded
    pub fn fetch_commits(&self, lsns: (LSN, Option<LSN>)) {}

    pub fn write_segment(&self) {}

    pub fn write_commit(&self) {}
}
