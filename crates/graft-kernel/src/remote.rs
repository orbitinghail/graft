use std::{future, path::PathBuf};

use futures::{
    Stream, StreamExt, TryStreamExt,
    stream::{self, FuturesOrdered},
};
use graft_core::{
    VolumeId,
    checkpoint_set::CheckpointSet,
    commit::Commit,
    etag::ETag,
    graft::Graft,
    lsn::{LSN, LSNSet},
    volume_control::VolumeControl,
};
use object_store::{
    ObjectStore, aws::S3ConditionalPut, local::LocalFileSystem, memory::InMemory, path::Path,
    prefix::PrefixStore,
};
use splinter_rs::{PartitionRead, Splinter};
use thiserror::Error;

pub mod segment;

const FETCH_COMMITS_CONCURRENCY: usize = 5;

#[derive(Error, Debug)]
pub enum RemoteErr {
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Invalid path: {0}")]
    Path(#[from] object_store::path::Error),
}

pub type Result<T> = culprit::Result<T, RemoteErr>;

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

impl RemoteConfig {
    pub fn build(self) -> Result<Remote> {
        Remote::with_config(self)
    }
}

pub struct Remote {
    store: Box<dyn ObjectStore>,
}

impl Remote {
    pub fn with_config(config: RemoteConfig) -> Result<Self> {
        let store: Box<dyn ObjectStore> = match config {
            RemoteConfig::Memory => Box::new(InMemory::new()),
            RemoteConfig::Fs { root } => Box::new(LocalFileSystem::new_with_prefix(root)?),
            RemoteConfig::S3Compatible { bucket, prefix } => {
                let store = object_store::aws::AmazonS3Builder::from_env()
                    .with_allow_http(true)
                    .with_bucket_name(bucket)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .build()?;
                if let Some(prefix) = prefix {
                    let prefix = Path::parse(prefix)?;
                    Box::new(PrefixStore::new(store, prefix))
                } else {
                    Box::new(store)
                }
            }
        };

        Ok(Self { store: store })
    }

    pub async fn fetch_control(&self, vid: &VolumeId) -> Result<VolumeControl> {
        todo!()
    }

    pub async fn fetch_checkpoints(
        &self,
        vid: &VolumeId,
        etag: Option<&ETag>,
    ) -> Result<Option<(ETag, CheckpointSet)>> {
        todo!()
    }

    pub async fn fetch_commits(
        &self,
        vid: &VolumeId,
        lsns: LSNSet,
    ) -> impl Stream<Item = Result<Commit>> {
        // convert the set into a stream of chunks, such that the first chunk
        // only contains the first LSN, and the remaining chunks have a maximum
        // size of REPLAY_CONCURRENCY
        // let mut iter = lsns.iter();
        // let first_chunk: Vec<LSN> = match iter.next() {
        //     Some(lsn) => vec![lsn],
        //     None => vec![],
        // };
        // let chunks = stream::once(future::ready(first_chunk))
        //     .chain(stream::iter(iter).chunks(FETCH_COMMITS_CONCURRENCY));

        todo!();
        stream::empty()
    }
}
