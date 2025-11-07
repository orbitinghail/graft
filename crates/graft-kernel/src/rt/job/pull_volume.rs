use std::fmt::Debug;

use culprit::{Result, ResultExt};
use graft_core::{
    VolumeId,
    checkpoints::Checkpoints,
    lsn::{LSN, LSNRangeExt},
};
use itertools::{EitherOrBoth, Itertools};
use range_set_blaze::RangeOnce;
use tokio_stream::StreamExt;

use crate::{
    KernelErr,
    local::fjall_storage::{FjallStorage, ReadGuard, WriteBatch},
    remote::Remote,
};

/// Pulls commits and metadata from a remote.
pub struct Opts {
    /// The Volume to fetch.
    pub vid: VolumeId,

    /// An optional maximum LSN to fetch.
    pub max_lsn: Option<LSN>,
}

impl Debug for Opts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("PullVolume");
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
    let reader = storage.read();
    let mut batch = storage.batch();

    // refresh checkpoint commits if needed
    refresh_checkpoint_commits(&reader, &mut batch, remote, &opts.vid).await?;

    // calculate the lsn range to retrieve
    let start = reader
        .latest_lsn(&opts.vid)
        .or_into_ctx()?
        .map_or(LSN::FIRST, |lsn| lsn.next());
    let end = opts.max_lsn.unwrap_or(LSN::LAST);
    let lsns = start..=end;

    tracing::debug!(vid = ?opts.vid, lsns = %lsns.to_string(), "pulling volume commits");

    // figure out which lsns we are missing
    let existing_lsns = storage.read().lsns(&opts.vid, &lsns).or_into_ctx()?;
    let missing_lsns = RangeOnce::new(lsns) - existing_lsns.into_ranges();

    // fetch missing lsns
    let mut commits = remote.stream_commits_ordered(&opts.vid, missing_lsns.flat_map(|r| r.iter()));
    while let Some(commit) = commits.try_next().await.or_into_ctx()? {
        batch.write_commit(commit);
    }

    batch.commit().or_into_ctx()
}

async fn refresh_checkpoint_commits(
    reader: &ReadGuard<'_>,
    batch: &mut WriteBatch<'_>,
    remote: &Remote,
    vid: &VolumeId,
) -> Result<(), KernelErr> {
    let cached_checkpoints = reader.checkpoints(vid).or_into_ctx()?;
    let (old_etag, old_checkpoints) = match &cached_checkpoints {
        Some(c) => (c.etag().map(|e| e.to_string()), c.checkpoints()),
        None => (None, &Checkpoints::EMPTY),
    };

    let new_checkpoints = match remote.get_checkpoints(vid, old_etag).await {
        Ok(c) => c,
        Err(err) if err.ctx().is_not_modified() || err.ctx().is_not_found() => return Ok(()),
        Err(err) => Err(err).or_into_ctx()?,
    };

    // Checkpoints are sorted, thus we can merge join the two lists of LSNs to
    // figure out which ones were added.
    let added: Vec<LSN> = old_checkpoints
        .iter()
        .merge_join_by(new_checkpoints.checkpoints().iter(), Ord::cmp)
        .filter_map(|join| match join {
            EitherOrBoth::Right(v) => Some(*v),
            _ => None,
        })
        .collect();

    let mut commits = remote.stream_commits_ordered(vid, added);
    while let Some(commit) = commits.try_next().await.or_into_ctx()? {
        batch.write_commit(commit);
    }
    Ok(())
}
