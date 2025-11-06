use std::{fmt::Debug, ops::Bound};

use culprit::{Result, ResultExt};
use futures::{StreamExt, TryStreamExt};
use graft_core::{
    PageIdx, SegmentId, VolumeId, commit::SegmentRangeRef, lsn::LSN, pageset::PageSet,
};
use itertools::Itertools;
use tryiter::TryIteratorExt;

use crate::{KernelErr, local::fjall_storage::FjallStorage, remote::Remote, rt::job::Job};

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
    let outstanding_frames = get_outstanding_frames(storage, opts)?;
    futures::stream::iter(outstanding_frames)
        .map(Ok)
        .try_for_each_concurrent(HYDRATE_CONCURRENCY, |(sid, frame)| {
            Job::fetch_segment(sid, frame).run(storage, remote)
        })
        .await
}

fn get_outstanding_frames(
    storage: &FjallStorage,
    opts: Opts,
) -> Result<Vec<(SegmentId, SegmentRangeRef)>, KernelErr> {
    let reader = storage.read();
    let snapshot = reader.snapshot_at(&opts.vid, opts.max_lsn).or_into_ctx()?;

    let mut outstanding_frames: Vec<(SegmentId, SegmentRangeRef)> = vec![];

    // the set of pages we are searching for.
    // we remove pages from this set as we iterate through commits.
    let mut pages = PageSet::from_range(reader.page_count(&snapshot).or_into_ctx()?.pageidxs());

    let mut page_count = reader.page_count(&snapshot).or_into_ctx()?;
    let mut commits = reader.commits(snapshot.search_path());
    while !pages.is_empty()
        && let Some(commit) = commits.try_next().or_into_ctx()?
    {
        // if we encounter a smaller commit on our travels, we need to shrink
        // the page_count to ensure that truncation is respected
        page_count = page_count.min(commit.page_count);

        if let Some(idx) = commit.segment_idx {
            // figure out which pages to ignore from this commit
            let truncate_start = match page_count.last_pageidx() {
                Some(pi) => {
                    if pi == PageIdx::LAST {
                        Bound::Excluded(PageIdx::LAST)
                    } else {
                        Bound::Included(pi)
                    }
                }
                None => Bound::Unbounded,
            };

            let mut commit_pages = idx.pageset.clone();
            // ignore pages we don't want
            commit_pages.remove_page_range((truncate_start, Bound::Unbounded));

            // figure out which pages we need from this commit
            let outstanding = pages.cut(&commit_pages);
            let frames = idx.iter_frames(|pages| outstanding.contains(*pages.start()));
            // combine adjacent frames if possible to reduce the number of remote requests
            for frame in frames.coalesce(|a, b| a.coalesce(b)) {
                outstanding_frames.push((idx.sid.clone(), frame));
            }
        }
    }

    Ok(outstanding_frames)
}
