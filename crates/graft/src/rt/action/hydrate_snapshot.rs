use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;

use crate::{
    GraftErr,
    local::fjall_storage::FjallStorage,
    remote::Remote,
    rt::action::{Action, fetch_segment::FetchSegment},
    snapshot::Snapshot,
};

const HYDRATE_CONCURRENCY: usize = 5;

/// Downloads all missing pages for a Snapshot.
#[derive(Debug)]
pub struct HydrateSnapshot {
    pub snapshot: Snapshot,
}

impl Action for HydrateSnapshot {
    async fn run(self, storage: &FjallStorage, remote: &Remote) -> Result<(), GraftErr> {
        let missing_frames = storage.read().find_missing_frames(&self.snapshot)?;
        futures::stream::iter(
            missing_frames
                .into_iter()
                // coalesce adjacent frames to minimize requests
                .coalesce(|a, b| a.coalesce(b)),
        )
        .map(Ok)
        .try_for_each_concurrent(HYDRATE_CONCURRENCY, |range| {
            FetchSegment { range }.run(storage, remote)
        })
        .await
    }
}
