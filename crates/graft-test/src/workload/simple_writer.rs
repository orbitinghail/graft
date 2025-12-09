use std::{thread::sleep, time::Duration};

use crate::{
    PageHash, PageTracker,
    workload::{load_tracker, recover_and_sync_volume},
};

use graft::core::{VolumeId, page::Page};
use graft_client::{
    oracle::LeapOracle,
    runtime::{
        storage::volume_state::{SyncDirection, VolumeConfig, VolumeStatus},
        volume_reader::VolumeRead,
        volume_writer::VolumeWrite,
    },
};
use precept::{expect_always_or_unreachable, expect_sometimes};
use rand::{Rng, distr::uniform::SampleRange, seq::IndexedRandom};
use serde::{Deserialize, Serialize};
use tracing::field;
use zerocopy::IntoBytes;

use super::{Workload, WorkloadEnv, WorkloadErr};

/// The `SimpleWriter` workload mutates a set of pages in the range `1..=128`
/// while maintaining an index page at `PageIdx(129)`.
///
/// Every `interval_ms` ms, the workload picks a random page, verifies it
/// matches it's hash in the index, and then randomly mutates it along with it's
/// index entry.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SimpleWriter {
    vids: Vec<VolumeId>,
    interval_ms: u64,

    #[serde(skip)]
    vid: Option<VolumeId>,
}

impl Workload for SimpleWriter {
    fn run<R: Rng>(&mut self, env: &mut WorkloadEnv<R>) -> Result<(), WorkloadErr> {
        let interval = Duration::from_millis(self.interval_ms);

        // pick volume id randomly on first run, and store in self.vid
        let vid = self
            .vid
            .get_or_insert_with(|| self.vids.choose(&mut env.rng).unwrap().clone());
        tracing::info!("SimpleWriter workload is using volume: {}", vid);

        let mut oracle = LeapOracle::default();
        let handle = env
            .runtime
            .open_volume(vid, VolumeConfig::new(SyncDirection::Both))?;

        let status = handle.status()?;
        expect_sometimes!(
            status != VolumeStatus::Ok,
            "volume is not ok when workload starts",
            { "cid": env.cid, "vid": vid }
        );

        // ensure the volume is recovered and synced with the server
        recover_and_sync_volume(&env.cid, &handle)?;

        while env.ticker.tick() {
            // check the volume status to see if we need to reset
            let status = handle.status()?;
            if status != VolumeStatus::Ok {
                let span = tracing::info_span!("reset_volume", ?status, vid=?handle.vid(), result=field::Empty).entered();
                precept::expect_always!(
                    status != VolumeStatus::InterruptedPush,
                    "volume needs reset after workload start",
                    { "cid": env.cid, "vid": handle.vid(), "status": status }
                );
                // reset the volume to the latest remote snapshot
                handle.reset_to_remote()?;
                span.record("result", format!("{:?}", handle.snapshot()?));
            }

            let span = tracing::info_span!(
                "write_page",
                pageidx = field::Empty,
                snapshot = field::Empty,
                new_hash = field::Empty,
                tracker_hash = field::Empty
            )
            .entered();

            // open a reader
            let reader = handle.reader()?;

            // randomly pick a pageidx
            let pageidx = PageTracker::MAX_PAGES
                .pageidxs()
                .sample_single(&mut env.rng)?;

            // generate a new page and hash it
            let new_page: Page = env.rng.random();
            let new_hash = PageHash::new(&new_page);

            span.record("pageidx", pageidx.to_u32());
            span.record("snapshot", format!("{:?}", reader.snapshot()));
            span.record("new_hash", new_hash.to_string());

            // load the tracker and the expected page hash
            let mut page_tracker = load_tracker(&mut oracle, &reader, &env.cid)?;
            let expected_hash = page_tracker.insert(pageidx, new_hash);

            // verify the page is missing or present as expected
            let page = reader.read(&mut oracle, pageidx)?;
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
                        "snapshot": format!("{:?}", reader.snapshot()),
                        "actual": actual_hash.to_string()
                    }
                );
            }

            // serialize the page tracker with the updated page and output it's hash for debugging
            let tracker_page = Page::try_from(page_tracker.as_bytes())?;
            span.record("tracker_hash", PageHash::new(&tracker_page).to_string());

            // write out the updated page tracker and the new page
            let mut writer = reader.upgrade();
            writer.write(PageTracker::PAGEIDX, tracker_page);
            writer.write(pageidx, new_page);

            // commit the changes
            let pre_commit_remote = writer.snapshot().and_then(|s| s.remote());
            let reader = writer.commit()?;
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
