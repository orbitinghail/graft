use std::{sync::Arc, time::SystemTime};

use futures::{Stream, TryStreamExt};
use graft_core::{lsn::LSN, VolumeId};
use object_store::{path::Path, ObjectStore};

use crate::volume::commit::{commit_key_prefix, CommitValidationErr};

use super::commit::{commit_key, Commit, CommitBuilder};

#[derive(Debug, thiserror::Error)]
pub enum VolumeStoreError {
    #[error(transparent)]
    ObjectStoreError(#[from] object_store::Error),

    #[error(transparent)]
    CommitValidationError(#[from] CommitValidationErr),
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

    pub async fn commit(&self, commit: CommitBuilder) -> Result<(), VolumeStoreError> {
        let (vid, lsn, commit) = commit.freeze();
        let key = commit_key(&vid, lsn);
        self.store.put(&key, commit.into()).await?;
        Ok(())
    }

    pub fn replay(
        &self,
        vid: VolumeId,
    ) -> impl Stream<Item = Result<Commit, VolumeStoreError>> + '_ {
        self.store
            .list(Some(&commit_key_prefix(&vid)))
            .map_err(|err| err.into())
            .and_then(|meta| self.get_commit(meta.location))
    }

    async fn get_commit(&self, path: Path) -> Result<Commit, VolumeStoreError> {
        let commit = self.store.get(&path).await?;
        let data = commit.bytes().await?;
        Ok(Commit::from_bytes(data)?)
    }
}
