use culprit::{Result, ResultExt};
use graft_core::{lsn::LSN, page::Page, page_offset::PageOffset, VolumeId};
use graft_proto::pagestore::v1::PageAtOffset;
use serde::Serialize;
use tryiter::TryIteratorExt;

use crate::{
    runtime::storage::{
        snapshot::{Snapshot, SnapshotKind},
        Storage,
    },
    ClientErr, ClientPair,
};

#[derive(Debug)]
pub enum Job {
    Pull(PullJob),
    Push(PushJob),
}

impl Job {
    pub fn pull(vid: VolumeId, remote_snapshot: Option<Snapshot>) -> Self {
        Job::Pull(PullJob { vid, remote_snapshot })
    }

    pub fn push(
        vid: VolumeId,
        remote_snapshot: Option<Snapshot>,
        sync_snapshot: Option<Snapshot>,
        snapshot: Snapshot,
    ) -> Self {
        Job::Push(PushJob {
            vid,
            remote_snapshot,
            sync_snapshot,
            snapshot,
        })
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
    /// The volume to pull from the remote.
    vid: VolumeId,

    /// The last snapshot of the volume that was pulled from the remote.
    remote_snapshot: Option<Snapshot>,
}

impl PullJob {
    fn run(self, storage: &Storage, clients: &ClientPair) -> Result<(), ClientErr> {
        log::debug!(
            "pulling volume {:?}; last snapshot {:?}",
            self.vid,
            self.remote_snapshot
        );

        // pull starting at the next LSN after the last pulled snapshot
        let start_lsn = self
            .remote_snapshot
            .as_ref()
            .and_then(|s| s.lsn().next())
            .unwrap_or(LSN::FIRST);

        if let Some((snapshot, _, changed)) =
            clients.metastore().pull_offsets(&self.vid, start_lsn..)?
        {
            let snapshot_lsn = snapshot.lsn().expect("invalid LSN");

            assert!(
                snapshot_lsn >= start_lsn,
                "invalid snapshot LSN; expected >= {}; got {}; last snapshot {:?}",
                start_lsn,
                snapshot_lsn,
                self.remote_snapshot
            );
            log::debug!("received remote snapshot at LSN {}", snapshot_lsn);

            storage
                .receive_remote_commit(&self.vid, snapshot.into(), changed)
                .or_into_ctx()?;
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct PushJob {
    /// The volume to push to the remote.
    vid: VolumeId,

    /// The last remote snapshot.
    remote_snapshot: Option<Snapshot>,

    /// The last local snapshot of the volume that was pushed to the remote.
    sync_snapshot: Option<Snapshot>,

    /// The current local snapshot of the volume.
    snapshot: Snapshot,
}

impl PushJob {
    fn run(self, storage: &Storage, clients: &ClientPair) -> Result<(), ClientErr> {
        log::debug!(
            "pushing volume {:?}; last sync {:?}; current snapshot {:?}; remote snapshot {:?}",
            self.vid,
            self.sync_snapshot,
            self.snapshot,
            self.remote_snapshot
        );

        // the range of local LSNs to push to the remote
        let start_lsn = self
            .sync_snapshot
            .as_ref()
            .map(|s| s.lsn())
            .unwrap_or(LSN::FIRST);
        let lsn_range = start_lsn..=self.snapshot.lsn();
        let page_count = self.snapshot.pages();

        // update the sync snapshot to the current snapshot.
        // we do this OUTSIDE of the batch to ensure that the snapshot is
        // updated even if the push fails this allows us to detect a failed push
        // during recovery
        storage
            .set_snapshot(&self.vid, SnapshotKind::Sync, self.snapshot.clone())
            .or_into_ctx()?;

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

        // load all of the pages into memory
        // TODO: stream pages directly to the pagestore
        let mut commits = storage.query_commits(&self.vid, lsn_range.clone());
        let mut found_commit = false;
        while let Some((lsn, offsets)) = commits.try_next().or_into_ctx()? {
            found_commit = true;
            let mut commit_pages = storage.query_pages(&self.vid, lsn, &offsets);
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
            found_commit,
            "push job should always find at least one commit",
            &serde_json::json!({
                "job": self,
                "lsn_range": lsn_range,
            })
        );

        // write the pages to the pagestore if there are any pages
        let segments = if !pages.is_empty() {
            clients.pagestore().write_pages(&self.vid, pages)?
        } else {
            Vec::new()
        };

        // commit the segments to the metastore
        let last_remote_lsn = self.remote_snapshot.as_ref().map(|s| s.lsn());
        let remote_snapshot =
            clients
                .metastore()
                .commit(&self.vid, last_remote_lsn, page_count, segments)?;

        storage
            .complete_sync(&self.vid, remote_snapshot.into(), lsn_range)
            .or_into_ctx()?;

        Ok(())
    }
}
