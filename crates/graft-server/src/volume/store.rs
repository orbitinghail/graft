use std::{sync::Arc, time::SystemTime};

use futures::{stream::FuturesUnordered, Stream, TryStreamExt};
use graft_core::{lsn::LSN, VolumeId};
use object_store::{path::Path, ObjectStore};

use crate::volume::commit::{commit_key_prefix, CommitValidationErr};

use super::commit::{commit_key, Commit, CommitBuilder};

const REPLAY_CONCURRENCY: usize = 5;

#[derive(Debug, thiserror::Error)]
pub enum VolumeStoreErr {
    #[error(transparent)]
    ObjectStoreErr(#[from] object_store::Error),

    #[error(transparent)]
    CommitValidationErr(#[from] CommitValidationErr),
}

pub struct VolumeStore<O> {
    store: Arc<O>,
}

impl<O: ObjectStore> VolumeStore<O> {
    pub fn new(store: Arc<O>) -> Self {
        Self { store }
    }

    pub fn prepare(&self, vid: VolumeId, lsn: LSN, last_offset: u32) -> CommitBuilder {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        CommitBuilder::new(vid, lsn, last_offset, timestamp)
    }

    pub async fn commit(&self, commit: CommitBuilder) -> Result<(), VolumeStoreErr> {
        let (vid, lsn, commit) = commit.freeze();
        let key = commit_key(&vid, lsn);
        self.store.put(&key, commit.into()).await?;
        Ok(())
    }

    /// Replay all commits for a volume optionally starting after the specified LSN.
    pub fn replay_unordered(
        &self,
        vid: VolumeId,
        from_lsn: Option<LSN>,
    ) -> impl Stream<Item = Result<Commit, VolumeStoreErr>> + '_ {
        let stream = if let Some(from_lsn) = from_lsn {
            self.store
                .list_with_offset(Some(&commit_key_prefix(&vid)), &commit_key(&vid, from_lsn))
        } else {
            self.store.list(Some(&commit_key_prefix(&vid)))
        };

        stream
            .try_ready_chunks(REPLAY_CONCURRENCY)
            .map_err(|err| VolumeStoreErr::from(err.1))
            .map_ok(|chunk| {
                chunk
                    .into_iter()
                    .map(|meta| self.get_commit(meta.location))
                    .collect::<FuturesUnordered<_>>()
            })
            .try_flatten()
    }

    async fn get_commit(&self, path: Path) -> Result<Commit, VolumeStoreErr> {
        let commit = self.store.get(&path).await?;
        let data = commit.bytes().await?;
        Ok(Commit::from_bytes(data)?)
    }
}
