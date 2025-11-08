use std::time::Duration;

use crate::{
    PageHash, PageTracker,
    workload::{load_tracker, recover_and_sync_volume},
};

use crossbeam::channel::RecvTimeoutError;
use culprit::{Culprit, ResultExt};
use graft_core::{VolumeId, page_idx::PageIdxRangeExt};
use precept::{expect_always_or_unreachable, expect_reachable, expect_sometimes};
use rand::{Rng, distr::uniform::SampleRange, seq::IndexedRandom};
use serde::{Deserialize, Serialize};
use tracing::field;

use super::{Workload, WorkloadEnv, WorkloadErr};

/// The `SimpleReader` workload validates that a subset of the pages in volume
/// are consistent with the index page at `PageIdx(129)`. It expects the volume is
/// only written to by the `SimpleWriter` workload.
///
/// The workload subscribes to changes for the volume. On change, the workload
/// picks a random subset of pages and then verifies each page matches its hash
/// in the index.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SimpleReader {
    vids: Vec<VolumeId>,
    recv_timeout_ms: u64,

    #[serde(skip)]
    vid: Option<VolumeId>,
}

impl Workload for SimpleReader {
    fn run<R: Rng>(&mut self, env: &mut WorkloadEnv<R>) -> Result<(), Culprit<WorkloadErr>> {
        let recv_timeout = Duration::from_millis(self.recv_timeout_ms);

        // pick volume id randomly on first run, and store in self.vid
        let vid = self
            .vid
            .get_or_insert_with(|| self.vids.choose(&mut env.rng).unwrap().clone());
        tracing::info!("SimpleReader workload is using volume: {}", vid);

        let mut oracle = LeapOracle::default();
        let handle = env
            .runtime
            .open_volume(vid, VolumeConfig::new(SyncDirection::Pull))
            .or_into_ctx()?;

        // ensure the volume is recovered and synced with the server
        recover_and_sync_volume(&env.cid, &handle).or_into_ctx()?;

        let subscription = handle.subscribe_to_remote_changes();

        let mut last_snapshot: Option<Snapshot> = None;
        let mut seen_nonempty = false;

        while env.ticker.tick() {
            // wait for the next commit
            match subscription.recv_timeout(recv_timeout) {
                Ok(()) => (),
                Err(RecvTimeoutError::Timeout) => {
                    tracing::info!("timeout while waiting for next commit, looping");
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => panic!("subscription closed"),
            }

            expect_reachable!(
                "reader workload received commit",
                { "cid": env.cid, "vid": vid }
            );

            let reader = handle.reader().or_into_ctx()?;
            let snapshot = reader.snapshot().expect("snapshot missing");

            tracing::info!(snapshot=?snapshot, "received commit for volume {:?}", vid);

            expect_sometimes!(
                last_snapshot
                    .replace(snapshot.clone())
                    .is_none_or(|last| &last != snapshot),
                "the snapshot is different after receiving a commit notification",
                { "snapshot": snapshot, "cid": env.cid, "vid": vid }
            );

            let page_count = snapshot.pages();
            if seen_nonempty {
                expect_always_or_unreachable!(
                    page_count > 0,
                    "the snapshot should never be empty after we have seen a non-empty snapshot",
                    { "snapshot": snapshot, "cid": env.cid, "vid": vid }
                );
            }

            if page_count.is_empty() {
                tracing::info!("volume is empty, waiting for the next commit");
                continue;
            } else {
                seen_nonempty = true;
            }

            // load the page index
            let page_tracker = load_tracker(&mut oracle, &reader, &env.cid).or_into_ctx()?;

            // pick a random subset of pages to read starting from the second
            // page, ending at the last page, and always at least one page
            let pageidxs = PageTracker::MAX_PAGES.pageidxs();
            let num_idxs = pageidxs.clone().sample_single(&mut env.rng)?.to_u32();
            let start_idx = pageidxs.sample_single(&mut env.rng)?;
            let end_idx = start_idx
                .saturating_add(num_idxs)
                .min(PageTracker::MAX_PAGES.last_pageidx().unwrap());
            let pageidxs = start_idx..=end_idx;

            tracing::info!(?vid, ?pageidxs, "validating pages in range");

            // ensure all pages are either empty or have the expected hash
            for pageidx in pageidxs.iter() {
                assert_ne!(
                    pageidx,
                    PageTracker::PAGEIDX,
                    "pageidxs should not include the page tracker"
                );

                let span =
                    tracing::info_span!("read_page", ?pageidx, hash = field::Empty).entered();

                let page = reader.read(&mut oracle, pageidx).or_into_ctx()?;
                let actual_hash = PageHash::new(&page);
                span.record("hash", actual_hash.to_string());

                if let Some(expected_hash) = page_tracker.get_hash(pageidx) {
                    expect_always_or_unreachable!(
                        expected_hash == &actual_hash,
                        "page should have expected contents",
                        {
                            "pageidx": pageidx,
                            "cid": env.cid,
                            "vid": vid,
                            "snapshot": snapshot.to_string(),
                            "expected": expected_hash.to_string(),
                            "actual": actual_hash.to_string()
                        }
                    );
                } else {
                    expect_always_or_unreachable!(
                        page.is_empty(),
                        "page should be empty as it has never been written to",
                        {
                            "pageidx": pageidx,
                            "cid": env.cid,
                            "vid": vid,
                            "snapshot": snapshot.to_string(),
                            "actual": actual_hash.to_string()
                        }
                    );
                }

                drop(span);
            }
        }
        Ok(())
    }
}
