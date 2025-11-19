use std::{sync::Arc, time::Duration};

use bytestring::ByteString;
use culprit::ResultExt;
use graft_core::{
    PageIdx, SegmentId, VolumeId,
    checksum::Checksum,
    commit::{SegmentIdx, SegmentRangeRef},
    lsn::LSN,
    page::Page,
    pageset::PageSet,
};
use tracing::Instrument;
use tryiter::TryIteratorExt;

use crate::{
    KernelErr,
    graft::{Graft, GraftStatus},
    remote::Remote,
    rt::{
        action::{Action, FetchSegment, FetchVolume, HydrateVolume, RemoteCommit},
        task::{autosync::AutosyncTask, supervise},
    },
    snapshot::Snapshot,
    tag_handle::TagHandle,
};

use crate::local::fjall_storage::FjallStorage;

type Result<T> = culprit::Result<T, KernelErr>;

#[derive(Clone, Debug)]
pub struct RuntimeHandle {
    inner: Arc<RuntimeHandleInner>,
}

#[derive(Debug)]
struct RuntimeHandleInner {
    tokio: tokio::runtime::Handle,
    storage: Arc<FjallStorage>,
    remote: Arc<Remote>,
}

impl RuntimeHandle {
    /// Create a Graft `RuntimeHandle` wrapping the provided Tokio runtime handle.
    pub fn new(
        tokio_rt: tokio::runtime::Handle,
        remote: Arc<Remote>,
        storage: Arc<FjallStorage>,
        autosync: Option<Duration>,
    ) -> RuntimeHandle {
        // spin up background tasks as needed
        if let Some(interval) = autosync {
            let _guard = tokio_rt.enter();
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tokio_rt.spawn(supervise(
                storage.clone(),
                remote.clone(),
                AutosyncTask::new(ticker),
            ));
        }
        RuntimeHandle {
            inner: Arc::new(RuntimeHandleInner { tokio: tokio_rt, storage, remote }),
        }
    }

    pub fn iter_tags(&self) -> impl Iterator<Item = Result<(ByteString, VolumeId)>> {
        self.storage()
            .read()
            .iter_tags()
            .map_err(|err| err.map_ctx(KernelErr::from))
    }

    pub fn tag_exists(&self, name: &str) -> Result<bool> {
        self.storage().read().tag_exists(name).or_into_ctx()
    }

    pub fn get_or_create_tag(&self, tag: &str) -> Result<TagHandle> {
        let graft = self.storage().get_or_create_tag(tag).or_into_ctx()?;
        Ok(TagHandle::new(self.clone(), tag.into(), graft.local))
    }

    pub fn iter_grafts(&self) -> impl Iterator<Item = Result<Graft>> {
        self.storage()
            .read()
            .iter_grafts()
            .map_err(|err| err.map_ctx(KernelErr::from))
    }

    pub fn graft_status(&self, graft: &VolumeId) -> Result<GraftStatus> {
        let reader = self.storage().read();
        let state = reader.graft(graft).or_into_ctx()?;
        let latest_local = reader.latest_lsn(&state.local).or_into_ctx()?;
        let latest_remote = reader.latest_lsn(&state.remote).or_into_ctx()?;
        Ok(state.status(latest_local, latest_remote))
    }

    pub(crate) fn storage(&self) -> &FjallStorage {
        &self.inner.storage
    }

    pub(crate) fn create_staged_segment(&self) -> SegmentIdx {
        // TODO: need to keep track of staged segments in memory to prevent the GC from clearing them
        SegmentIdx::new(SegmentId::random(), PageSet::default())
    }

    pub(crate) fn read_page(&self, snapshot: &Snapshot, pageidx: PageIdx) -> Result<Page> {
        let reader = self.storage().read();
        if let Some(commit) = reader.search_page(snapshot, pageidx).or_into_ctx()? {
            let idx = commit
                .segment_idx()
                .expect("BUG: commit claims to contain pageidx");

            if let Some(page) = reader.read_page(idx.sid().clone(), pageidx).or_into_ctx()? {
                return Ok(page);
            }

            // fallthrough to loading the page from the remote
            let frame = idx
                .frame_for_pageidx(pageidx)
                .expect("BUG: no frame for pageidx");

            // fetch the segment frame containing the page
            self.fetch_segment_range(frame)?;

            // now that we've fetched the segment, read the page again using a
            // fresh storage reader
            Ok(self
                .storage()
                .read()
                .read_page(idx.sid.clone(), pageidx)
                .or_into_ctx()?
                .expect("BUG: page not found after fetching"))
        } else {
            Ok(Page::EMPTY)
        }
    }

    pub fn checksum(&self, snapshot: &Snapshot) -> Result<Checksum> {
        self.storage().read().checksum(snapshot).or_into_ctx()
    }

    pub fn missing_pages(&self, snapshot: &Snapshot) -> Result<PageSet> {
        let missing_frames = self
            .storage()
            .read()
            .find_missing_frames(snapshot)
            .or_into_ctx()?;
        // merge missing_frames into a single PageSet
        Ok(missing_frames
            .into_iter()
            .fold(PageSet::EMPTY, |mut pageset, frame| {
                pageset |= frame.pageset;
                pageset
            }))
    }

    pub fn fork(&self, snapshot: &Snapshot) -> Result<Graft> {
        self.storage().fork_snapshot(snapshot).or_into_ctx()
    }

    pub fn fetch_segment_range(&self, range: SegmentRangeRef) -> Result<()> {
        self.run_action(FetchSegment { range })
    }

    pub fn hydrate_volume(&self, vid: VolumeId, max_lsn: Option<LSN>) -> Result<()> {
        self.run_action(HydrateVolume { vid, max_lsn })
    }

    pub fn fetch_volume(&self, vid: VolumeId, max_lsn: Option<LSN>) -> Result<()> {
        self.run_action(FetchVolume { vid, max_lsn })
    }

    pub fn pull_graft(&self, graft: VolumeId) -> Result<()> {
        let graft = self.inner.storage.read().graft(&graft).or_into_ctx()?;
        self.fetch_volume(graft.remote, None)?;
        self.inner
            .storage
            .sync_remote_to_local(graft.local)
            .or_into_ctx()
    }

    pub fn push_graft(&self, graft: VolumeId) -> Result<()> {
        self.run_action(RemoteCommit { graft })
    }

    fn run_action<A: Action>(&self, action: A) -> Result<()> {
        let span = tracing::debug_span!("Action::run", ?action);

        self.inner.tokio.block_on(
            action
                .run(&self.inner.storage, &self.inner.remote)
                .instrument(span),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use graft_core::{PageIdx, page::Page};
    use tokio::time::sleep;

    use crate::{
        local::fjall_storage::FjallStorage, remote::RemoteConfig,
        rt::runtime_handle::RuntimeHandle, volume_reader::VolumeRead, volume_writer::VolumeWrite,
    };

    #[graft_test::test]
    fn runtime_sanity() {
        let tokio_rt = tokio::runtime::Builder::new_current_thread()
            .start_paused(true)
            .enable_all()
            .build()
            .unwrap();

        let remote = Arc::new(RemoteConfig::Memory.build().unwrap());
        let storage = Arc::new(FjallStorage::open_temporary().unwrap());
        let runtime = RuntimeHandle::new(
            tokio_rt.handle().clone(),
            remote.clone(),
            storage,
            Some(Duration::from_secs(1)),
        );

        let handle = runtime.get_or_create_tag("leader").unwrap();
        let remote_vid = handle.remote().unwrap();

        assert_eq!(handle.status().unwrap().to_string(), "_ r_",);

        // sanity check volume writer semantics
        let mut writer = handle.writer().unwrap();
        for i in [1u8, 2, 5, 9] {
            let pageidx = PageIdx::must_new(i as u32);
            let page = Page::test_filled(i);
            writer.write_page(pageidx, page.clone()).unwrap();
            assert_eq!(writer.read_page(pageidx).unwrap(), page);
        }
        writer.commit().unwrap();

        assert_eq!(handle.status().unwrap().to_string(), "1 r_",);

        // sanity check volume reader semantics
        let reader = handle.reader().unwrap();
        tracing::debug!("got snapshot {:?}", reader.snapshot());
        for i in [1u8, 2, 5, 9] {
            let pageidx = PageIdx::must_new(i as u32);
            let page = Page::test_filled(i);
            assert_eq!(
                reader.read_page(pageidx).unwrap().into_bytes(),
                page.into_bytes()
            );
        }

        // create a second runtime connected to the same remote
        let storage = Arc::new(FjallStorage::open_temporary().unwrap());
        let runtime_2 = RuntimeHandle::new(
            tokio_rt.handle().clone(),
            remote.clone(),
            storage,
            Some(Duration::from_secs(1)),
        );

        // open the same graft in the second runtime
        let mut handle_2 = runtime_2.get_or_create_tag("follower").unwrap();
        handle_2.clone_remote(Some(remote_vid)).unwrap();

        // let both runtimes run for a little while
        tokio_rt.block_on(async {
            // this sleep lets tokio advance time, allowing the runtime to flush all its jobs
            sleep(Duration::from_secs(5)).await;
            let tree = remote.testonly_format_tree().await;
            tracing::info!("remote tree\n{tree}")
        });

        assert_eq!(handle.status().unwrap().to_string(), "1 r1",);
        assert_eq!(handle_2.status().unwrap().to_string(), "1 r1",);

        // sanity check volume reader semantics in the second runtime
        let reader_2 = handle_2.reader().unwrap();
        let task = tokio_rt.spawn_blocking(move || {
            for i in [1u8, 2, 5, 9] {
                let pageidx = PageIdx::must_new(i as u32);
                tracing::info!("checking page {pageidx}");
                let expected = Page::test_filled(i);
                let actual = reader_2.read_page(pageidx).unwrap();
                assert_eq!(expected, actual, "read unexpected page contents");
            }
        });
        tokio_rt.block_on(task).unwrap();

        // now write to the second volume, and sync back to the first
        let mut writer_2 = handle_2.writer().unwrap();
        for i in [3u8, 4, 5, 7] {
            let pageidx = PageIdx::must_new(i as u32);
            let page = Page::test_filled(i + 10);
            writer_2.write_page(pageidx, page.clone()).unwrap();
            assert_eq!(writer_2.read_page(pageidx).unwrap(), page);
        }
        writer_2.commit().unwrap();

        // let both runtimes run for a little while
        tokio_rt.block_on(async {
            // this sleep lets tokio advance time, allowing the runtime to flush all its jobs
            sleep(Duration::from_secs(5)).await;
            let tree = remote.testonly_format_tree().await;
            tracing::info!("remote tree\n{tree}")
        });

        assert_eq!(handle.status().unwrap().to_string(), "2 r2",);
        assert_eq!(handle_2.status().unwrap().to_string(), "2 r2",);

        // sanity check volume reader semantics in the first runtime
        let reader = handle.reader().unwrap();
        let task = tokio_rt.spawn_blocking(move || {
            for i in [3u8, 4, 5, 7] {
                let pageidx = PageIdx::must_new(i as u32);
                tracing::info!("checking page {pageidx}");
                let expected = Page::test_filled(i + 10);
                let actual = reader.read_page(pageidx).unwrap();
                assert_eq!(expected, actual, "read unexpected page contents");
            }
        });
        tokio_rt.block_on(task).unwrap();
    }
}
