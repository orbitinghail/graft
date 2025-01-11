use culprit::{Result, ResultExt};
use graft_core::{page::Page, page_offset::PageOffset, VolumeId};
use graft_proto::pagestore::v1::PageAtOffset;
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
    pub fn pull(vid: VolumeId, snapshot: Option<Snapshot>) -> Self {
        Job::Pull(PullJob { vid, snapshot })
    }

    pub fn push(vid: VolumeId, sync_snapshot: Option<Snapshot>, snapshot: Snapshot) -> Self {
        Job::Push(PushJob { vid, sync_snapshot, snapshot })
    }

    pub async fn run(self, storage: &Storage, clients: &ClientPair) -> Result<(), ClientErr> {
        match self {
            Job::Pull(job) => job.run(storage, clients).await,
            Job::Push(job) => job.run(storage, clients).await,
        }
    }
}

#[derive(Debug)]
pub struct PullJob {
    /// The volume to pull from the remote.
    vid: VolumeId,

    /// The last snapshot of the volume that was pulled from the remote.
    snapshot: Option<Snapshot>,
}

impl PullJob {
    async fn run(self, storage: &Storage, clients: &ClientPair) -> Result<(), ClientErr> {
        log::debug!(
            "pulling volume {:?}; last snapshot {:?}",
            self.vid,
            self.snapshot
        );

        // pull starting at the next LSN after the last pulled snapshot
        let start_lsn = self
            .snapshot
            .as_ref()
            .and_then(|s| s.lsn().next())
            .unwrap_or_default();

        if let Some((snapshot, _, changed)) = clients
            .metastore()
            .pull_offsets(&self.vid, start_lsn..)
            .await?
        {
            storage
                .receive_remote_commit(
                    &self.vid,
                    snapshot.is_checkpoint(),
                    snapshot.into(),
                    changed,
                )
                .or_into_ctx()?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct PushJob {
    /// The volume to push to the remote.
    vid: VolumeId,

    /// The last snapshot of the volume that was pushed to the remote.
    sync_snapshot: Option<Snapshot>,

    /// The current local snapshot of the volume.
    snapshot: Snapshot,
}

impl PushJob {
    async fn run(self, storage: &Storage, clients: &ClientPair) -> Result<(), ClientErr> {
        log::debug!(
            "pushing volume {:?}; last sync {:?}; current snapshot {:?}",
            self.vid,
            self.sync_snapshot,
            self.snapshot
        );

        // the range of local LSNs to push to the remote
        let start_lsn = self
            .sync_snapshot
            .as_ref()
            .map(|s| s.lsn())
            .unwrap_or_default();
        let lsn_range = start_lsn..=self.snapshot.lsn();
        let page_count = self.snapshot.page_count();

        // update the sync snapshot to the current snapshot
        // we do this OUTSIDE of the batch to ensure that the snapshot is updated even if the push fails
        // this allows us to detect a failed push during recovery
        storage
            .set_snapshot(&self.vid, SnapshotKind::Sync, self.snapshot)
            .or_into_ctx()?;

        // load all of the pages into memory
        // TODO: stream pages directly to the pagestore
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

        {
            let mut commits = storage.query_commits(&self.vid, lsn_range.clone());
            while let Some((lsn, offsets)) = commits.try_next().or_into_ctx()? {
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
        }

        // write the pages to the pagestore
        let segments = clients.pagestore().write_pages(&self.vid, pages).await?;

        // commit the segments to the metastore
        let snapshot_lsn = self.sync_snapshot.as_ref().map(|s| s.lsn());
        let remote_snapshot = clients
            .metastore()
            .commit(&self.vid, snapshot_lsn, page_count, segments)
            .await?;

        storage
            .complete_sync(
                &self.vid,
                remote_snapshot.is_checkpoint(),
                remote_snapshot.into(),
                lsn_range,
            )
            .or_into_ctx()?;

        Ok(())
    }
}
