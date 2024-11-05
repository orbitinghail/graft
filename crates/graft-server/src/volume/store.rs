use std::ops::RangeBounds;
use std::{future::ready, sync::Arc, time::SystemTime};

use futures::{stream::FuturesUnordered, Stream, TryStreamExt};
use graft_core::{lsn::LSN, VolumeId};
use graft_proto::common::v1::LsnRange;
use object_store::ObjectStore;

use crate::volume::commit::{commit_key_prefix, CommitValidationErr};

use super::commit::{commit_key, parse_commit_key, Commit, CommitBuilder, CommitKeyParseErr};

const REPLAY_CONCURRENCY: usize = 5;

#[derive(Debug, thiserror::Error)]
pub enum VolumeStoreErr {
    #[error(transparent)]
    ObjectStoreErr(#[from] object_store::Error),

    #[error(transparent)]
    CommitValidationErr(#[from] CommitValidationErr),

    #[error("Failed to parse commit key: {0}")]
    CommitKeyParseErr(#[from] CommitKeyParseErr),
}

pub struct VolumeStore<O> {
    store: Arc<O>,
}

impl<O: ObjectStore> VolumeStore<O> {
    pub fn new(store: Arc<O>) -> Self {
        Self { store }
    }

    pub fn prepare(
        &self,
        vid: VolumeId,
        lsn: LSN,
        checkpoint: LSN,
        last_offset: u32,
    ) -> CommitBuilder {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        CommitBuilder::new(vid, lsn, checkpoint, last_offset, timestamp)
    }

    pub async fn commit(&self, commit: CommitBuilder) -> Result<(), VolumeStoreErr> {
        let (vid, lsn, commit) = commit.freeze();
        let key = commit_key(&vid, lsn);
        self.store.put(&key, commit.into()).await?;
        Ok(())
    }

    /// Replay all commits for a volume contained by the specified LSN range.
    pub fn replay_unordered(
        &self,
        vid: VolumeId,
        range: LsnRange,
    ) -> impl Stream<Item = Result<Commit, VolumeStoreErr>> + '_ {
        let stream = if let Some(from_lsn) = range.start_exclusive() {
            self.store
                .list_with_offset(Some(&commit_key_prefix(&vid)), &commit_key(&vid, from_lsn))
        } else {
            self.store.list(Some(&commit_key_prefix(&vid)))
        };

        stream
            .map_err(VolumeStoreErr::from)
            // We can't use try_take_while because we can't depend on the object
            // store implementation to return keys sorted alphanumerically.
            .try_filter_map(move |meta| {
                let (key_vid, lsn) = match parse_commit_key(&meta.location) {
                    Ok((vid, lsn)) => (vid, lsn),
                    Err(err) => return ready(Err(err.into())),
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

    pub async fn get_commit(&self, vid: VolumeId, lsn: LSN) -> Result<Commit, VolumeStoreErr> {
        let path = commit_key(&vid, lsn);
        let commit = self.store.get(&path).await?;
        let data = commit.bytes().await?;
        Ok(Commit::from_bytes(data)?)
    }
}
