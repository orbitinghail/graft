use culprit::{Result, ResultExt};
use graft_core::{lsn::LSN, page::Page, page_offset::PageOffset};
use graft_proto::pagestore::v1::PageAtOffset;
use serde::Serialize;
use tryiter::TryIteratorExt;

use crate::{
    runtime::storage::{volume_state::VolumeState, Storage},
    ClientErr, ClientPair,
};

#[derive(Debug)]
pub enum Job {
    Pull(PullJob),
    Push(PushJob),
}

impl Job {
    pub fn pull(state: VolumeState) -> Self {
        Job::Pull(PullJob { state })
    }

    pub fn push(state: VolumeState) -> Self {
        Job::Push(PushJob { state })
    }

    pub fn run(self, storage: &Storage, clients: &ClientPair) -> Result<(), ClientErr> {
        match self {
            Job::Pull(job) => job.run(storage, clients),
            Job::Push(job) => job.run(storage, clients),
        }
    }
}

#[derive(Debug)]
pub struct PullJob {
    state: VolumeState,
}

impl PullJob {
    fn run(self, storage: &Storage, clients: &ClientPair) -> Result<(), ClientErr> {
        log::debug!(
            "pulling volume {:?}; snapshot {:?}",
            self.state.vid(),
            self.state.snapshot()
        );

        // pull starting at the next LSN after the last pulled snapshot
        let start_lsn = self
            .state
            .snapshot()
            .remote()
            .and_then(|lsn| lsn.next())
            .unwrap_or(LSN::FIRST);

        if let Some((snapshot, _, changed)) = clients
            .metastore()
            .pull_offsets(self.state.vid(), start_lsn..)?
        {
            let snapshot_lsn = snapshot.lsn().expect("invalid LSN");

            assert!(
                snapshot_lsn >= start_lsn,
                "invalid snapshot LSN; expected >= {}; got {}; last snapshot {:?}",
                start_lsn,
                snapshot_lsn,
                self.state.snapshot()
            );
            log::debug!("received remote snapshot at LSN {}", snapshot_lsn);

            storage
                .receive_remote_commit(self.state.vid(), snapshot, changed)
                .or_into_ctx()?;
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct PushJob {
    state: VolumeState,
}

impl PushJob {
    fn run(self, storage: &Storage, clients: &ClientPair) -> Result<(), ClientErr> {
        if let Err(err) = self.run_inner(storage, clients) {
            if let Err(inner) = storage.rollback_sync_to_remote(self.state.vid()) {
                log::error!("failed to rollback sync to remote: {:?}", inner);
                Err(err.with_note(format!("rollback failed after push job failed: {}", inner)))
            } else {
                Err(err)
            }
        } else {
            Ok(())
        }
    }

    fn run_inner(&self, storage: &Storage, clients: &ClientPair) -> Result<(), ClientErr> {
        log::debug!(
            "pushing volume {:?}; current snapshot {:?}; current watermarks {:?}",
            self.state.vid(),
            self.state.snapshot(),
            self.state.watermarks(),
        );

        // setup temporary storage for pages
        // TODO: we will eventually stream pages directly to the remote
        let mut pages = Vec::new();
        let mut upsert_page = |offset: PageOffset, page: Page| {
            // binary search upsert the page into pages
            match pages.binary_search_by_key(&offset, |p: &PageAtOffset| p.offset()) {
                Ok(i) => {
                    // replace the page in the list with this page
                    pages[i].data = page.into();
                }
                Err(i) => {
                    // insert the page into the list
                    pages.insert(i, PageAtOffset::new(offset, page));
                }
            }
        };

        // prepare the sync
        let (snapshot, lsns, mut commits) = storage
            .prepare_sync_to_remote(self.state.vid())
            .or_into_ctx()?;

        let page_count = snapshot.pages();

        // load all of the pages into memory
        // TODO: stream pages directly to the remote
        while let Some((lsn, offsets)) = commits.try_next().or_into_ctx()? {
            let mut commit_pages = storage.query_pages(self.state.vid(), lsn, &offsets);
            while let Some((offset, page)) = commit_pages.try_next().or_into_ctx()? {
                // it's a fatal error if the page is None or Pending
                let page = page
                    .expect("page missing from storage")
                    .expect("page missing from storage");

                // if the page is still contained within the page_count, include it
                if page_count.contains(offset) {
                    upsert_page(offset, page);
                }
            }
        }

        #[cfg(feature = "antithesis")]
        antithesis_sdk::assert_always_or_unreachable!(
            !pages.is_empty(),
            "push job should always push at least one page",
            &serde_json::json!({ "job": self, })
        );
        debug_assert!(
            !pages.is_empty(),
            "push job should always push at least one page"
        );

        // write the pages to the pagestore if there are any pages
        let segments = if !pages.is_empty() {
            clients.pagestore().write_pages(self.state.vid(), pages)?
        } else {
            Vec::new()
        };

        // commit the segments to the metastore
        let snapshot = clients.metastore().commit(
            self.state.vid(),
            snapshot.remote(),
            page_count,
            segments,
        )?;

        // complete the sync
        storage
            .complete_sync_to_remote(self.state.vid(), snapshot, lsns)
            .or_into_ctx()?;

        Ok(())
    }
}
