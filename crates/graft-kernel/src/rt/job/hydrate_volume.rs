use std::fmt::Debug;

use culprit::{Result, ResultExt};
use futures::{StreamExt, TryStreamExt};
use graft_core::{VolumeId, lsn::LSN};
use itertools::Itertools;

use crate::{
    KernelErr, local::fjall_storage::FjallStorage, remote::Remote, rt::job::Job, snapshot::Snapshot,
};

const HYDRATE_CONCURRENCY: usize = 5;

/// Downloads all missing pages for a Volume up to an optional maximum LSN.
/// If `max_lsn` is not specified, will hydrate the Volume up to its latest snapshot.
pub struct Opts {
    /// The Volume to hydrate.
    pub vid: VolumeId,

    /// An optional maximum LSN to fetch.
    pub max_lsn: Option<LSN>,
}

impl Debug for Opts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("HydrateVolume");
        out.field("vid", &self.vid);
        if let Some(max_lsn) = self.max_lsn {
            out.field("max_lsn", &max_lsn.to_string());
            out.finish()
        } else {
            out.finish_non_exhaustive()
        }
    }
}

pub async fn run(storage: &FjallStorage, remote: &Remote, opts: Opts) -> Result<(), KernelErr> {
    // build a snapshot to search for commits
    let snapshot = Snapshot::new(opts.vid, LSN::FIRST..=opts.max_lsn.unwrap_or(LSN::LAST));

    let missing_frames = storage
        .read()
        .find_missing_frames(&snapshot)
        .or_into_ctx()?;
    futures::stream::iter(
        missing_frames
            .into_iter()
            // coalesce adjacent frames to minimize requests
            .coalesce(|a, b| a.coalesce(b)),
    )
    .map(Ok)
    .try_for_each_concurrent(HYDRATE_CONCURRENCY, |frame| {
        Job::fetch_segment(frame).run(storage, remote)
    })
    .await
}
