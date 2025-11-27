use std::{future, ops::Range, path::PathBuf};

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
};
use object_store::{
    GetOptions, GetRange, ObjectStore, PutOptions, PutPayload, aws::S3ConditionalPut,
    local::LocalFileSystem, memory::InMemory, path::Path, prefix::PrefixStore,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod segment;

const FETCH_COMMITS_CONCURRENCY: usize = 5;

enum RemotePath<'a> {
    /// `CheckpointSets` are stored at `/volumes/{vid}/checkpoints`
    CheckpointSet(&'a VolumeId),

    /// Commits are stored at `/volumes/{vid}/log/{CBE64 hex LSN}`
    Commit(&'a VolumeId, LSN),

    /// Segments are stored at `/segments/{sid}`
    Segment(&'a SegmentId),
}

impl RemotePath<'_> {
    fn build(self) -> object_store::path::Path {
        match self {
            Self::CheckpointSet(vid) => {
                Path::from_iter(["volumes", &vid.serialize(), "checkpoints"])
            }
            Self::Commit(vid, lsn) => Path::from_iter([
                "volumes",
                &vid.serialize(),
                "log",
                &CBE64::from(lsn).to_string(),
            ]),
            Self::Segment(sid) => Path::from_iter(["segments", &sid.serialize()]),
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

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
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

#[derive(Debug)]
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

    /// Fetches checkpoints for the specified volume. If `etag` is not `None`
    /// then this method will return a not modified error.
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_checkpoints(
        &self,
        vid: &VolumeId,
        etag: Option<String>,
    ) -> Result<CachedCheckpoints> {
        let path = RemotePath::CheckpointSet(vid).build();
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
    pub fn stream_commits_ordered<I: IntoIterator<Item = LSN>>(
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
    #[tracing::instrument(level = "trace", skip(self, lsn), fields(lsn = %lsn))]
    pub async fn get_commit(&self, vid: &VolumeId, lsn: LSN) -> Result<Option<Commit>> {
        let path = RemotePath::Commit(vid, lsn).build();
        match self.store.get(&path).await {
            Ok(res) => Commit::decode(res.bytes().await?).or_into_ctx().map(Some),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Atomically write a commit to the remote, returning
    /// `RemoteErr::ObjectStore(Error::AlreadyExists)` on a collision
    #[tracing::instrument(level = "debug", skip(self, commit), fields(lsn = %commit.lsn()))]
    pub async fn put_commit(&self, commit: &Commit) -> Result<()> {
        let path = RemotePath::Commit(commit.vid(), commit.lsn()).build();
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
    #[tracing::instrument(level = "debug", skip(self, chunks), fields(size))]
    pub async fn put_segment<I: IntoIterator<Item = Bytes>>(
        &self,
        sid: &SegmentId,
        chunks: I,
    ) -> Result<()> {
        let path = RemotePath::Segment(sid).build();
        let payload = PutPayload::from_iter(chunks);
        tracing::Span::current().record("size", payload.content_length());
        self.store.put(&path, payload).await?;
        Ok(())
    }

    /// Reads a byte range of a segment
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_segment_range(&self, sid: &SegmentId, bytes: &Range<usize>) -> Result<Bytes> {
        let path = RemotePath::Segment(sid).build();
        let get_opts = GetOptions {
            range: Some(GetRange::Bounded(bytes.start as u64..bytes.end as u64)),
            ..GetOptions::default()
        };
        let result = self.store.get_opts(&path, get_opts).await?;
        Ok(result.bytes().await?)
    }

    /// TESTONLY: list contents of this remote in a tree-like format
    #[cfg(test)]
    pub async fn testonly_format_tree(&self) -> String {
        use itertools::Itertools;
        use std::collections::BTreeMap;
        use text_trees::{
            AnchorPosition, FormatCharacters, TreeFormatting, TreeNode, TreeOrientation,
        };

        let paths = self.store.list(None).map_ok(|obj| {
            obj.location
                .parts()
                .map(|p| p.as_ref().to_string())
                .collect_vec()
        });
        let paths: Vec<_> = paths.try_collect().await.unwrap();

        #[derive(Default)]
        struct TreeBuilder {
            children: BTreeMap<String, TreeBuilder>,
        }

        impl TreeBuilder {
            fn insert(&mut self, parts: &[String]) {
                if parts.is_empty() {
                    return;
                }

                let first = &parts[0];
                let rest = &parts[1..];

                self.children.entry(first.clone()).or_default().insert(rest);
            }

            fn to_tree_node(self, name: String) -> TreeNode<String> {
                if self.children.is_empty() {
                    // This is a leaf node
                    TreeNode::new(name)
                } else {
                    // This is a directory node
                    let child_nodes = self
                        .children
                        .into_iter()
                        .map(|(name, builder)| builder.to_tree_node(name));
                    TreeNode::with_child_nodes(name, child_nodes)
                }
            }
        }

        let mut root = TreeBuilder::default();
        for path in paths {
            root.insert(&path);
        }

        root.to_tree_node(self.store.to_string())
            .to_string_with_format(&TreeFormatting {
                prefix_str: None,
                orientation: TreeOrientation::TopDown,
                anchor: AnchorPosition::Left,
                chars: FormatCharacters::box_chars(),
            })
            .unwrap()
            .to_string()
    }
}
