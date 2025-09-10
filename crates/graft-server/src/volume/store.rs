use std::{
    future::{self},
    ops::RangeBounds,
    sync::Arc,
};

use bytes::Bytes;
use culprit::{Culprit, ResultExt};
use futures::{
    Stream, StreamExt, TryStreamExt,
    stream::{self, FuturesOrdered},
};
use graft_core::{
    VolumeId,
    lsn::{LSN, LSNRangeExt},
};
use object_store::{Attributes, ObjectStore, PutMode, PutOptions, TagSet};

use crate::{bytes_vec::BytesVec, volume::commit::CommitValidationErr};

use super::commit::{Commit, CommitKeyParseErr, commit_key_path};

const REPLAY_CONCURRENCY: usize = 5;

#[derive(Debug, thiserror::Error)]
pub enum VolumeStoreErr {
    #[error("object store error")]
    ObjectStoreErr,

    #[error("commit not found")]
    CommitNotFound,

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
            object_store::Error::NotFound { .. } => VolumeStoreErr::CommitNotFound,
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
                    extensions: Default::default(),
                },
            )
            .await?;
        Ok(())
    }

    /// Replay all commits for a volume contained by the specified LSN range.
    pub fn replay_ordered<'a, R: RangeBounds<LSN> + 'a>(
        &'a self,
        vid: &'a VolumeId,
        range: &'a R,
    ) -> impl Stream<Item = Result<Commit<Bytes>, Culprit<VolumeStoreErr>>> + 'a {
        // convert the range into a stream of chunks, such that the first chunk
        // only contains the first LSN, and the remaining chunks have a maximum
        // size of REPLAY_CONCURRENCY
        let mut iter = range.iter();
        let first_chunk: Vec<LSN> = iter.next().into_iter().collect();
        let chunks = stream::once(future::ready(first_chunk))
            .chain(stream::iter(iter).chunks(REPLAY_CONCURRENCY));

        chunks
            .flat_map(move |chunk| {
                chunk
                    .into_iter()
                    .map(|lsn| self.get_commit(vid, lsn))
                    .collect::<FuturesOrdered<_>>()
            })
            .try_take_while(|result| future::ready(Ok(result.is_some())))
            .map_ok(|result| result.unwrap())
    }

    pub async fn get_commit(
        &self,
        vid: &VolumeId,
        lsn: LSN,
    ) -> Result<Option<Commit<Bytes>>, Culprit<VolumeStoreErr>> {
        // inject a random sleep when testing to verify replay_ordered will
        // correctly reorder futures when they come back out of order. Due to
        // tokio time mocking this shouldn't actually make the tests slower. We
        // set the sleep extremely high to make it obvious if tokio time mocking
        // is not working correctly.
        #[cfg(test)]
        {
            use std::time::Duration;
            // smaller lsns = larger sleeps
            let duration = Duration::from_millis(100000 - u64::from(lsn));
            tokio::time::sleep(duration).await;
        }

        let path = commit_key_path(vid, lsn);
        match self.store.get(&path).await {
            Ok(res) => Commit::from_bytes(res.bytes().await?)
                .or_into_ctx()
                .map(Some),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use graft_core::{ClientId, PageCount};

    use super::*;
    use crate::{
        testutil::test_object_store::{ObjectStoreOp, TestObjectStore},
        volume::commit::{CommitBuilder, CommitMeta},
    };

    #[graft_test::test]
    async fn test_replay() {
        let objstore = Arc::new(TestObjectStore::default());
        let volstore = VolumeStore::new(objstore.clone());

        let vid = VolumeId::random();
        let cid = ClientId::random();

        // create some commits
        let commits = (1..13)
            .map(LSN::new)
            .map(|lsn| {
                CommitMeta::new(
                    vid.clone(),
                    cid.clone(),
                    lsn,
                    LSN::new(1),
                    PageCount::new(1),
                    SystemTime::now(),
                )
            })
            .collect::<Vec<_>>();

        for meta in commits.iter().cloned() {
            let commit = CommitBuilder::new_with_capacity(meta, 0).build();
            volstore.commit(commit).await.unwrap();
        }

        // verify that we can replay commits, and the result is ordered, and the
        // expected number of requests were issued
        let start_lsn = LSN::new(1);
        let lsns = start_lsn..;
        let replay = volstore.replay_ordered(&vid, &lsns);

        // zip the replay with the expected commits
        let mut zipped = replay.zip(futures::stream::iter(commits.clone()));

        let mut count = 0;
        while let Some((result, expected)) = zipped.next().await {
            let commit = result.unwrap();
            assert_eq!(commit.meta(), &expected);
            count += 1;

            assert!(objstore.count_hits(ObjectStoreOp::Get).await >= count);
        }

        // verify that we got the expected number of commits
        assert_eq!(count, commits.len());

        // we expect the object store to receive 16 gets
        // there are 12 commits in the store (lsns: 1..=12)
        // replay concurrency is 5; but we always request the first LSN before checking any additional LSNs
        // so the request batches are: 1, 5, 5, 5
        assert_eq!(
            objstore.count_hits(ObjectStoreOp::Get).await,
            1 + (((commits.len() / REPLAY_CONCURRENCY) + 1) * REPLAY_CONCURRENCY)
        );
        // and no list ops were used
        assert_eq!(objstore.count_hits(ObjectStoreOp::List).await, 0);
    }
}
