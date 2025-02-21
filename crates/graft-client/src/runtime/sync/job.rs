use culprit::{Result, ResultExt};
use graft_core::{
    gid::ClientId,
    lsn::{LSNRangeExt, LSN},
    page::Page,
    PageIdx, VolumeId,
};
use graft_proto::pagestore::v1::PageAtIdx;
use serde::Serialize;
use tryiter::TryIteratorExt;

use crate::{runtime::storage::Storage, ClientErr, ClientPair};

#[derive(Debug)]
pub enum Job {
    Pull(PullJob),
    Push(PushJob),
}

impl Job {
    pub fn pull(vid: VolumeId) -> Self {
        Job::Pull(PullJob { vid, reset: false })
    }

    pub fn pull_and_reset(vid: VolumeId) -> Self {
        Job::Pull(PullJob { vid, reset: true })
    }

    pub fn push(vid: VolumeId, cid: ClientId) -> Self {
        Job::Push(PushJob { vid, cid })
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
    vid: VolumeId,

    /// when reset is true, the PullJob will reset the volume to the remote
    /// volume state. This will rollback any pending local commits and clear the
    /// volume status.
    reset: bool,
}

impl PullJob {
    fn run(self, storage: &Storage, clients: &ClientPair) -> Result<(), ClientErr> {
        let state = storage.volume_state(&self.vid).or_into_ctx()?;

        // pull starting at the next LSN after the last pulled snapshot
        let start_lsn = state
            .snapshot()
            .and_then(|s| s.remote())
            .map(|lsn| lsn.next().expect("lsn overflow"))
            .unwrap_or(LSN::FIRST);
        let lsns = start_lsn..;

        let _span =
            tracing::trace_span!("PullJob", vid = ?self.vid, snapshot = ?state.snapshot(), ?lsns)
                .entered();

        if let Some((snapshot, _, changed)) = clients
            .metastore()
            .pull_graft(&self.vid, lsns)
            .or_into_ctx()?
        {
            let snapshot_lsn = snapshot.lsn().expect("invalid LSN");

            assert!(
                snapshot_lsn >= start_lsn,
                "invalid snapshot LSN; expected >= {}; got {}; last snapshot {:?}",
                start_lsn,
                snapshot_lsn,
                state.snapshot()
            );
            tracing::trace!("received remote snapshot at LSN {snapshot_lsn}");

            if self.reset {
                storage
                    .reset_volume_to_remote(&self.vid, snapshot, changed)
                    .or_into_ctx()?;
            } else {
                storage
                    .receive_remote_commit(&self.vid, snapshot, changed)
                    .or_into_ctx()?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct PushJob {
    vid: VolumeId,
    cid: ClientId,
}

impl PushJob {
    fn run(self, storage: &Storage, clients: &ClientPair) -> Result<(), ClientErr> {
        // prepare the sync
        let (snapshot, lsns, mut commits) =
            storage.prepare_sync_to_remote(&self.vid).or_into_ctx()?;

        let _span = tracing::trace_span!("PushJob", vid=?self.vid, ?snapshot, ?lsns).entered();

        // setup temporary storage for pages
        // TODO: we will eventually stream pages directly to the remote
        let mut pages = Vec::new();
        let mut upsert_page = |pageidx: PageIdx, page: Page| {
            // binary search upsert the page into pages
            match pages.binary_search_by_key(&pageidx.to_u32(), |p: &PageAtIdx| p.pageidx) {
                Ok(i) => {
                    // replace the page in the list with this page
                    pages[i].data = page.into();
                }
                Err(i) => {
                    // insert the page into the list
                    pages.insert(i, PageAtIdx::new(pageidx, page));
                }
            }
        };

        let page_count = snapshot.pages();

        let mut num_commits = 0;
        let expected_num_commits = lsns.try_len().expect("lsns is RangeInclusive");

        // load all of the pages into memory
        // TODO: stream pages directly to the remote
        while let Some((lsn, graft)) = commits.try_next().or_into_ctx()? {
            num_commits += 1;
            let mut commit_pages = storage.query_pages(&self.vid, lsn, &graft);
            while let Some((pageidx, page)) = commit_pages.try_next().or_into_ctx()? {
                // it's a fatal error if the page is None or Pending
                let page = page
                    .expect("page missing from storage")
                    .expect("page missing from storage");

                // if the page is still contained within the page_count, include it
                if page_count.contains(pageidx) {
                    upsert_page(pageidx, page);
                }
            }
        }
        precept::expect_always_or_unreachable!(
            num_commits == expected_num_commits,
            "push job always pushes all expected commits",
            { "job": self }
        );
        debug_assert_eq!(
            num_commits, expected_num_commits,
            "push job always pushes all expected commits"
        );

        // write the pages to the pagestore if there are any pages
        let segments = if !pages.is_empty() {
            clients
                .pagestore()
                .write_pages(&self.vid, pages)
                .or_into_ctx()?
        } else {
            Vec::new()
        };

        precept::maybe_fault!(0.1, "PushJob: before metastore commit", { "cid": self.cid });

        // commit the segments to the metastore
        let remote_snapshot = match clients.metastore().commit(
            &self.vid,
            &self.cid,
            snapshot.remote(),
            page_count,
            segments,
        ) {
            Ok(remote_snapshot) => remote_snapshot,
            Err(err) => {
                // if the commit was rejected, notify storage
                if err.ctx().is_commit_rejected() {
                    if let Err(reject_err) = storage.rejected_sync_to_remote(&self.vid) {
                        return Err(
                            err.with_note(format!("rejected sync to remote failed: {reject_err}"))
                        );
                    }
                }
                return Err(err);
            }
        };

        precept::maybe_fault!(0.1, "PushJob: after metastore commit", { "cid": self.cid });

        // complete the sync
        storage
            .complete_sync_to_remote(&self.vid, snapshot, remote_snapshot, lsns)
            .or_into_ctx()?;

        Ok(())
    }
}
