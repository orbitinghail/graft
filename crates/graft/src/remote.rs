use std::{future, ops::Range, time::Duration};

use crate::core::{LogId, SegmentId, cbe::CBE64, commit::Commit, lsn::LSN};
use bilrost::{Message, OwnedMessage};
use bytes::Bytes;
use futures::{
    Stream, StreamExt, TryStreamExt,
    stream::{self, FuturesOrdered},
};
use opendal::{
    Buffer, ErrorKind, Operator,
    layers::{HttpClientLayer, RetryLayer},
    options::{ReadOptions, WriteOptions},
    raw::HttpClient,
    services::{Fs, Memory, S3},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod segment;

const REMOTE_CONCURRENCY: usize = 5;

enum RemotePath<'a> {
    /// Commits are stored at `/logs/{logid}/commits/{CBE64 hex LSN}`
    Commit(&'a LogId, LSN),

    /// Segments are stored at `/segments/{sid}`
    Segment(&'a SegmentId),
}

impl RemotePath<'_> {
    fn build(self) -> String {
        match self {
            Self::Commit(log, lsn) => format!(
                "logs/{}/commits/{}",
                &log.serialize(),
                &CBE64::from(lsn).to_string(),
            ),
            Self::Segment(sid) => format!("segments/{}", &sid.serialize()),
        }
    }
}

#[derive(Error, Debug)]
pub enum RemoteErr {
    #[error("Object store error: {0}")]
    ObjectStore(#[from] opendal::Error),

    #[error("HTTP client setup error: {0}")]
    SetupHttp(#[from] reqwest::Error),

    #[error("Failed to decode file: {0}")]
    Decode(#[from] bilrost::DecodeError),
}

impl RemoteErr {
    fn objectstore_err_kind(&self) -> Option<opendal::ErrorKind> {
        if let RemoteErr::ObjectStore(err) = self {
            Some(err.kind())
        } else {
            None
        }
    }

    pub fn precondition_failed(&self) -> bool {
        matches!(
            self.objectstore_err_kind(),
            Some(opendal::ErrorKind::ConditionNotMatch)
        )
    }

    pub fn is_not_found(&self) -> bool {
        matches!(
            self.objectstore_err_kind(),
            Some(opendal::ErrorKind::NotFound)
        )
    }
}

pub type Result<T> = std::result::Result<T, RemoteErr>;

#[derive(Debug, Deserialize, Serialize, Default, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RemoteConfig {
    /// In memory object store
    #[default]
    Memory,

    /// On disk object store
    Fs { root: String },

    /// S3 compatible object store
    /// Can load most config and secrets from standard AWS environment variables
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
    store: Operator,
}

impl Remote {
    pub fn with_config(config: RemoteConfig) -> Result<Self> {
        let store = match config {
            RemoteConfig::Memory => Operator::new(Memory::default())?.finish(),
            RemoteConfig::Fs { root } => Operator::new(Fs::default().root(&root))?.finish(),
            RemoteConfig::S3Compatible { bucket, prefix } => {
                let mut builder = S3::default().bucket(&bucket);
                if let Some(prefix) = prefix {
                    builder = builder.root(&prefix);
                }
                if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
                    builder = builder.endpoint(&endpoint);
                }
                let client = reqwest::ClientBuilder::new()
                    // use http1 to maximize throughput
                    // http2 routes all requests through a single connection
                    .http1_only()
                    // enable hickory DNS resolver for DNS caching
                    .hickory_dns(true)
                    .connect_timeout(Duration::from_secs(5))
                    .timeout(Duration::from_secs(60))
                    .build()?;

                Operator::new(builder)?
                    .layer(HttpClientLayer::new(HttpClient::with(client)))
                    .layer(RetryLayer::new())
                    .finish()
            }
        };

        Ok(Self { store })
    }

    /// Streams commits by LSN in the same order as the input iterator.
    /// Stops fetching commits as soon as we receive a `NotFound` error from the
    /// remote, thus even if `lsns` contains every LSN we will stop loading
    /// commits as soon as we reach the end of the log.
    pub fn stream_commits_ordered<I: IntoIterator<Item = LSN>>(
        &self,
        log: &LogId,
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
            .chain(stream::iter(lsns).chunks(REMOTE_CONCURRENCY))
            .flat_map(|chunk| {
                chunk
                    .into_iter()
                    .map(|lsn| self.get_commit(log, lsn))
                    .collect::<FuturesOrdered<_>>()
            })
            .try_take_while(|result| future::ready(Ok(result.is_some())))
            .map_ok(|result| result.unwrap())
    }

    /// Fetches a single commit, returning None if the commit is not found.
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn get_commit(&self, log: &LogId, lsn: LSN) -> Result<Option<Commit>> {
        let path = RemotePath::Commit(log, lsn).build();
        match self.store.read(&path).await {
            Ok(res) => Ok(Some(Commit::decode(res)?)),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// Atomically write a commit to the remote, returning
    /// `RemoteErr::ObjectStore(Error::AlreadyExists)` on a collision
    #[tracing::instrument(level = "debug", skip(self, commit), fields(log = %commit.log, lsn = %commit.lsn))]
    pub async fn put_commit(&self, commit: &Commit) -> Result<()> {
        let path = RemotePath::Commit(commit.log(), commit.lsn()).build();
        self.store
            .write_options(
                &path,
                commit.encode_to_bytes(),
                WriteOptions {
                    // Perform an atomic write operation, returning
                    // a precondition error if the commit already exists
                    if_not_exists: true,
                    concurrent: REMOTE_CONCURRENCY,
                    ..WriteOptions::default()
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
        let buffer = Buffer::from_iter(chunks);
        tracing::Span::current().record("size", buffer.len());
        self.store
            .write_options(
                &path,
                buffer,
                WriteOptions {
                    concurrent: REMOTE_CONCURRENCY,
                    ..WriteOptions::default()
                },
            )
            .await?;
        Ok(())
    }

    /// Reads a byte range of a segment
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn get_segment_range(&self, sid: &SegmentId, bytes: Range<u64>) -> Result<Bytes> {
        let path = RemotePath::Segment(sid).build();
        let buffer = self
            .store
            .read_options(
                &path,
                ReadOptions {
                    range: bytes.into(),
                    concurrent: REMOTE_CONCURRENCY,
                    ..ReadOptions::default()
                },
            )
            .await?;
        Ok(buffer.to_bytes())
    }

    /// TESTONLY: list contents of this remote in a tree-like format
    #[cfg(test)]
    pub async fn testonly_format_tree(&self) -> String {
        use itertools::Itertools;
        use std::collections::BTreeMap;
        use text_trees::{
            AnchorPosition, FormatCharacters, TreeFormatting, TreeNode, TreeOrientation,
        };

        let paths = self
            .store
            .list("")
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.path().split("/").map(|s| s.to_string()).collect_vec())
            .collect_vec();

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

        root.to_tree_node(format!("{:?}", self.store))
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
