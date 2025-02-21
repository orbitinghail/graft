use std::{future::ready, ops::RangeBounds, sync::Arc};

use bytes::Bytes;
use culprit::{Culprit, ResultExt};
use futures::{stream::FuturesUnordered, Stream, TryStreamExt};
use graft_core::{
    lsn::{LSNRangeExt, LSN},
    VolumeId,
};
use object_store::{Attributes, ObjectStore, PutMode, PutOptions, TagSet};

use crate::{
    bytes_vec::BytesVec,
    volume::commit::{commit_key_path_prefix, CommitValidationErr},
};

use super::commit::{commit_key_path, parse_commit_key, Commit, CommitKeyParseErr};

const REPLAY_CONCURRENCY: usize = 5;

#[derive(Debug, thiserror::Error)]
pub enum VolumeStoreErr {
    #[error("object store error")]
    ObjectStoreErr,

    #[error("commit already exists")]
    CommitAlreadyExists,

    #[error("commit validation error: {0}")]
    CommitValidationErr(#[from] CommitValidationErr),

    #[error("Failed to parse commit key: {0}")]
    CommitKeyParseErr(#[from] CommitKeyParseErr),
}

impl From<object_store::Error> for VolumeStoreErr {
    fn from(err: object_store::Error) -> Self {
        match err {
            object_store::Error::AlreadyExists { .. } => VolumeStoreErr::CommitAlreadyExists,
            _ => VolumeStoreErr::ObjectStoreErr,
        }
    }
}

pub struct VolumeStore {
    store: Arc<dyn ObjectStore>,
}

impl VolumeStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub async fn commit(&self, commit: Commit<BytesVec>) -> Result<(), Culprit<VolumeStoreErr>> {
        let key = commit_key_path(commit.vid(), commit.meta().lsn());
        self.store
            .put_opts(
                &key,
                commit.into_payload(),
                PutOptions {
                    mode: PutMode::Create,
                    tags: TagSet::default(),
                    attributes: Attributes::default(),
                },
            )
            .await?;
        Ok(())
    }

    /// Replay all commits for a volume contained by the specified LSN range.
    pub fn replay_unordered<'a, R: RangeBounds<LSN> + 'a>(
        &'a self,
        vid: VolumeId,
        range: &'a R,
    ) -> impl Stream<Item = Result<Commit<Bytes>, Culprit<VolumeStoreErr>>> + 'a {
        let stream = if let Some(from_lsn) = range.try_start_exclusive() {
            self.store.list_with_offset(
                Some(&commit_key_path_prefix(&vid)),
                &commit_key_path(&vid, from_lsn),
            )
        } else {
            self.store.list(Some(&commit_key_path_prefix(&vid)))
        };

        stream
            .err_into()
            // We can't use try_take_while because we can't depend on the object
            // store implementation to return keys sorted alphanumerically.
            .try_filter_map(move |meta| {
                let (key_vid, lsn) = match parse_commit_key(&meta.location).or_into_ctx() {
                    Ok((vid, lsn)) => (vid, lsn),
                    Err(err) => return ready(Err(err)),
                };
                assert!(vid == key_vid, "Unexpected volume ID in commit key");
                ready(Ok(range.contains(&lsn).then_some((key_vid, lsn))))
            })
            .try_ready_chunks(REPLAY_CONCURRENCY)
            .map_ok(|chunk| {
                chunk
                    .into_iter()
                    .map(|(vid, lsn)| self.get_commit(vid, lsn))
                    .collect::<FuturesUnordered<_>>()
            })
            .map_err(|err| err.1)
            .try_flatten()
    }

    pub async fn get_commit(
        &self,
        vid: VolumeId,
        lsn: LSN,
    ) -> Result<Commit<Bytes>, Culprit<VolumeStoreErr>> {
        let path = commit_key_path(&vid, lsn);
        let commit = self.store.get(&path).await?;
        let data = commit.bytes().await?;
        Commit::from_bytes(data).or_into_ctx()
    }
}
