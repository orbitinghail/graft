use std::{future, path::PathBuf};

use bilrost::{Message, OwnedMessage};
use bytes::Bytes;
use culprit::ResultExt;
use futures::{
    Stream, StreamExt, TryStreamExt,
    stream::{self, FuturesOrdered},
};
use graft_core::{
    SegmentId, VolumeId,
    cbe::CBE64,
    checkpoints::{CachedCheckpoints, Checkpoints},
    commit::Commit,
    lsn::LSN,
    volume_control::VolumeControl,
};
use object_store::{
    GetOptions, ObjectStore, PutOptions, PutPayload, aws::S3ConditionalPut, local::LocalFileSystem,
    memory::InMemory, path::Path, prefix::PrefixStore,
};
use thiserror::Error;

pub mod segment;

const FETCH_COMMITS_CONCURRENCY: usize = 5;

enum RemotePath<'a> {
    /// Control files are stored at `/{vid}/control`
    Control,

    /// Forks are stored at `/{vid}/forks/{fork_vid}`
    /// Forks point from the parent to the child.
    ///
    /// TODO: Implement Forks!
    // Fork(&'a VolumeId),

    /// `CheckpointSets` are stored at `/{vid}/checkpoints`
    CheckpointSet,

    /// Commits are stored at `/{vid}/log/{CBE64 hex LSN}`
    Commit(LSN),

    /// Segments are stored at `/{vid}/segments/{sid}`
    Segment(&'a SegmentId),
}

impl RemotePath<'_> {
    fn build(self, vid: &VolumeId) -> object_store::path::Path {
        let vid = vid.pretty();
        match self {
            Self::Control => Path::from_iter([&vid, "control"]),
            // TODO: Implement Forks!
            // Self::Fork(fork) => Path::from_iter([&vid, "forks", &fork.pretty()]),
            Self::CheckpointSet => Path::from_iter([&vid, "checkpoints"]),
            Self::Commit(lsn) => Path::from_iter([&vid, "log", &CBE64::from(lsn).to_string()]),
            Self::Segment(sid) => Path::from_iter([&vid, "segments", &sid.pretty()]),
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
    pub fn is_already_exists(&self) -> bool {
        matches!(
            self,
            Self::ObjectStore(object_store::Error::AlreadyExists { .. })
        )
    }

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

        Ok(Self { store })
    }

    pub async fn get_control(&self, vid: &VolumeId) -> Result<VolumeControl> {
        let path = RemotePath::Control.build(vid);
        let result = self.store.get(&path).await?;
        let bytes = result.bytes().await?;
        Ok(VolumeControl::decode(bytes)?)
    }

    /// Atomically write a volume control to the remote, returning
    /// `RemoteErr::ObjectStore(Error::AlreadyExists)` on a collision
    pub async fn put_control(&self, vid: &VolumeId, control: VolumeControl) -> Result<()> {
        let path = RemotePath::Control.build(vid);
        let payload = PutPayload::from_bytes(control.encode_to_bytes());
        self.store
            .put_opts(
                &path,
                payload,
                PutOptions {
                    mode: object_store::PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await?;
        Ok(())
    }

    /// Fetches checkpoints for the specified volume. If `etag` is not `None`
    /// then this method will return a not modified error.
    pub async fn get_checkpoints(
        &self,
        vid: &VolumeId,
        etag: Option<String>,
    ) -> Result<CachedCheckpoints> {
        let path = RemotePath::CheckpointSet.build(vid);
        let opts = GetOptions {
            if_none_match: etag,
            ..GetOptions::default()
        };

        let result = self.store.get_opts(&path, opts).await?;
        let etag = result.meta.e_tag.clone();
        let bytes = result.bytes().await?;

        Ok(CachedCheckpoints::new(Checkpoints::decode(bytes)?, etag))
    }

    /// Streams commits by LSN in the same order as the input iterator.
    /// Stops fetching commits as soon as we receive a `NotFound` error from the
    /// remote, thus even if `lsns` contains every LSN we will stop loading
    /// commits as soon as we reach the end of the log.
    pub fn stream_sorted_commits<I: IntoIterator<Item = LSN>>(
        &self,
        vid: &VolumeId,
        lsns: I,
    ) -> impl Stream<Item = Result<Commit>> {
        // convert the set into a stream of chunks, such that the first chunk
        // only contains the first LSN, and the remaining chunks have a maximum
        // size of REPLAY_CONCURRENCY
        let mut lsns = lsns.into_iter();
        let first_chunk: Vec<LSN> = match lsns.next() {
            Some(lsn) => vec![lsn],
            None => vec![],
        };
        stream::once(future::ready(first_chunk))
            .chain(stream::iter(lsns).chunks(FETCH_COMMITS_CONCURRENCY))
            .flat_map(|chunk| {
                chunk
                    .into_iter()
                    .map(|lsn| self.get_commit(vid, lsn))
                    .collect::<FuturesOrdered<_>>()
            })
            .try_take_while(|result| future::ready(Ok(result.is_some())))
            .map_ok(|result| result.unwrap())
    }

    /// Fetches a single commit, returning None if the commit is not found.
    pub async fn get_commit(&self, vid: &VolumeId, lsn: LSN) -> Result<Option<Commit>> {
        let path = RemotePath::Commit(lsn).build(vid);
        match self.store.get(&path).await {
            Ok(res) => Commit::decode(res.bytes().await?).or_into_ctx().map(Some),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Atomically write a commit to the remote, returning
    /// `RemoteErr::ObjectStore(Error::AlreadyExists)` on a collision
    pub async fn put_commit(&self, vid: &VolumeId, commit: Commit) -> Result<()> {
        let path = RemotePath::Commit(commit.lsn()).build(vid);
        let payload = PutPayload::from_bytes(commit.encode_to_bytes());
        self.store
            .put_opts(
                &path,
                payload,
                PutOptions {
                    // Perform an atomic write operation, returning
                    // AlreadyExists if the commit already exists
                    mode: object_store::PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await?;
        Ok(())
    }

    /// Uploads a segment to this Remote
    pub async fn put_segment<I: IntoIterator<Item = Bytes>>(
        &self,
        vid: &VolumeId,
        sid: &SegmentId,
        chunks: I,
    ) -> Result<()> {
        let path = RemotePath::Segment(sid).build(vid);
        self.store.put(&path, PutPayload::from_iter(chunks)).await?;
        Ok(())
    }
}
