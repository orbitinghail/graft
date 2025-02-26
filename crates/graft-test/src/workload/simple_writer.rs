use std::{thread::sleep, time::Duration};

use crate::{
    workload::{load_tracker, recover_and_sync_volume},
    PageHash,
};

use culprit::{Culprit, ResultExt};
use graft_client::{
    oracle::LeapOracle,
    runtime::{
        storage::volume_state::{SyncDirection, VolumeConfig, VolumeStatus},
        volume_reader::VolumeRead,
        volume_writer::VolumeWrite,
    },
};
use graft_core::{page::Page, PageCount, PageIdx, VolumeId};
use precept::{expect_always_or_unreachable, expect_sometimes};
use rand::{distr::uniform::SampleRange, seq::IndexedRandom, Rng};
use serde::{Deserialize, Serialize};
use tracing::field;

use super::{Workload, WorkloadEnv, WorkloadErr};

/// The `SimpleWriter` workload mutates a set of pages in the range `2..=pages+1`
/// while maintaining an index page at `PageIdx(1)`.
///
/// Every `interval_ms` ms, the workload picks a random page, verifies it
/// matches it's hash in the index, and then randomly mutates it along with it's
/// index entry.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SimpleWriter {
    vids: Vec<VolumeId>,
    interval_ms: u64,
    pages: PageCount,

    #[serde(skip)]
    vid: Option<VolumeId>,
}

impl Workload for SimpleWriter {
    fn run<R: Rng>(&mut self, env: &mut WorkloadEnv<R>) -> Result<(), Culprit<WorkloadErr>> {
        let interval = Duration::from_millis(self.interval_ms);

        // pick volume id randomly on first run, and store in self.vid
        let vid = self
            .vid
            .get_or_insert_with(|| self.vids.choose(&mut env.rng).unwrap().clone());
        tracing::info!("SimpleWriter workload is using volume: {}", vid);

        let mut oracle = LeapOracle::default();
        let handle = env
            .runtime
            .open_volume(vid, VolumeConfig::new(SyncDirection::Both))
            .or_into_ctx()?;

        let status = handle.status().or_into_ctx()?;
        expect_sometimes!(
            status != VolumeStatus::Ok,
            "volume is not ok when workload starts",
            { "cid": env.cid, "vid": vid }
        );

        // ensure the volume is recovered and synced with the server
        recover_and_sync_volume(&handle).or_into_ctx()?;

        while env.ticker.tick() {
            // check the volume status to see if we need to reset
            let status = handle.status().or_into_ctx()?;
            if status != VolumeStatus::Ok {
                let span = tracing::info_span!("reset_volume", ?status, vid=?handle.vid(), result=field::Empty).entered();
                precept::expect_always!(
                    status != VolumeStatus::InterruptedPush,
                    "volume needs reset after workload start",
                    { "cid": env.cid, "vid": handle.vid(), "status": status }
                );
                // reset the volume to the latest remote snapshot
                handle.reset_to_remote().or_into_ctx()?;
                span.record("result", format!("{:?}", handle.snapshot().or_into_ctx()?));
            }

            // open a reader
            let reader = handle.reader().or_into_ctx()?;

            // randomly pick a pageidx
            // select the next pageidx to ensure we don't pick the first page
            let pageidx = self
                .pages
                .pageidxs()
                .sample_single(&mut env.rng)?
                .saturating_next();

            // generate a new page and hash it
            let new_page: Page = env.rng.random();
            let new_hash = PageHash::new(&new_page);

            let span = tracing::info_span!(
                "write_page",
                pageidx=pageidx.to_string(),
                snapshot=?reader.snapshot(),
                ?new_hash,
                tracker_hash=field::Empty
            )
            .entered();

            // load the tracker and the expected page hash
            let mut page_tracker = load_tracker(&mut oracle, &reader, &env.cid).or_into_ctx()?;
            let expected_hash = page_tracker.upsert(pageidx, new_hash.clone());

            // verify the page is missing or present as expected
            let page = reader.read(&mut oracle, pageidx).or_into_ctx()?;
            let actual_hash = PageHash::new(&page);

            if let Some(expected_hash) = expected_hash {
                expect_always_or_unreachable!(
                    expected_hash == actual_hash,
                    "page should have expected contents",
                    {
                        "pageidx": pageidx,
                        "cid": env.cid,
                        "vid": vid,
                        "snapshot": format!("{:?}", reader.snapshot()),
                        "expected": expected_hash,
                        "actual": actual_hash
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
                        "snapshot": reader.snapshot(),
                        "actual": actual_hash
                    }
                );
            }

            // serialize the page tracker with the updated page and output it's hash for debugging
            let tracker_page = page_tracker.serialize_into_page().or_into_ctx()?;
            span.record("tracker_hash", PageHash::new(&tracker_page).to_string());

            // write out the updated page tracker and the new page
            let mut writer = reader.upgrade();
            writer.write(PageIdx::FIRST, tracker_page);
            writer.write(pageidx, new_page);

            // commit the changes
            let pre_commit_remote = writer.snapshot().and_then(|s| s.remote());
            let reader = writer.commit().or_into_ctx()?;
            let post_commit_remote = reader.snapshot().and_then(|s| s.remote());

            expect_sometimes!(
                pre_commit_remote != post_commit_remote,
                "remote LSN changed concurrently with local commit",
                {
                    "pre_commit": pre_commit_remote,
                    "snapshot": reader.snapshot(),
                    "cid": env.cid,
                    "vid": vid,
                }
            );

            drop(span);

            sleep(interval);
        }
        Ok(())
    }
}
