use std::path::PathBuf;

use bilrost::OwnedMessage;
use futures::{
    Stream,
    stream::{self},
};
use graft_core::{
    SegmentId, VolumeId,
    checkpoints::{CachedCheckpoints, Checkpoints},
    commit::Commit,
    lsn::{LSN, LSNSet},
    volume_control::VolumeControl,
};
use object_store::{
    GetOptions, ObjectStore, aws::S3ConditionalPut, local::LocalFileSystem, memory::InMemory,
    path::Path, prefix::PrefixStore,
};
use thiserror::Error;

pub mod segment;

const FETCH_COMMITS_CONCURRENCY: usize = 5;

enum RemotePath<'a> {
    Control,
    Fork(&'a VolumeId),
    CheckpointSet,
    Log(LSN),
    Segment(&'a SegmentId),
}

impl RemotePath<'_> {
    fn build(self, vid: &VolumeId) -> object_store::path::Path {
        let vid = vid.pretty();
        match self {
            Self::Control => Path::from_iter([&vid, "control"]),
            Self::Fork(fork) => Path::from_iter([&vid, "forks", &fork.pretty()]),
            Self::CheckpointSet => Path::from_iter([&vid, "checkpoints"]),
            Self::Log(lsn) => Path::from_iter([&vid, "log", &lsn.to_string()]),
            Self::Segment(sid) => Path::from_iter([&vid, "segments", &sid.to_string()]),
        }
    }
}

#[derive(Error, Debug)]
pub enum RemoteErr {
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Invalid path: {0}")]
    Path(#[from] object_store::path::Error),

    #[error("Failed to decode file: {0}")]
    Decode(#[from] bilrost::DecodeError),
}

impl RemoteErr {
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Self::ObjectStore(object_store::Error::NotFound { .. })
        )
    }

    pub fn is_not_modified(&self) -> bool {
        matches!(
            self,
            Self::ObjectStore(object_store::Error::NotModified { .. })
        )
    }
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
        let path = RemotePath::Control.build(vid);
        let result = self.store.get(&path).await?;
        let bytes = result.bytes().await?;
        Ok(VolumeControl::decode(bytes)?)
    }

    /// Fetches checkpoints for the specified volume. If `etag` is not `None`
    /// then this method will return a not modified error.
    pub async fn fetch_checkpoints(
        &self,
        vid: &VolumeId,
        etag: Option<String>,
    ) -> Result<CachedCheckpoints> {
        let path = RemotePath::CheckpointSet.build(vid);
        let mut opts = GetOptions::default();
        opts.if_none_match = etag;

        let result = self.store.get_opts(&path, opts).await?;
        let etag = result.meta.e_tag.clone();
        let bytes = result.bytes().await?;

        Ok(CachedCheckpoints::new(Checkpoints::decode(bytes)?, etag))
    }

    /// Fetches commits sorted in ascending order by LSN.
    /// Stops fetching commits as soon as we receive a NotFound error from the
    /// remote, thus even if `lsns` is full we will stop loading commits as soon
    /// as we reach the end of the log.
    pub async fn fetch_sorted_commits(
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
