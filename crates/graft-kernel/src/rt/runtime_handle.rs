use std::{ops::Deref, str::FromStr, sync::Arc, time::Duration};

use culprit::ResultExt;
use graft_core::{PageIdx, SegmentId, VolumeId, commit::SegmentIdx, graft::Graft, page::Page};
use tokio::task::JoinHandle;

use crate::{
    GraftErr,
    named_volume::NamedVolume,
    page_status::PageStatus,
    remote::Remote,
    rt::{
        rpc::RpcHandle,
        runtime::{Event, Runtime, RuntimeFatalErr},
    },
    snapshot::Snapshot,
    volume_name::VolumeName,
};

use tokio::sync::mpsc;
use tokio_stream::{
    StreamExt,
    wrappers::{IntervalStream, ReceiverStream},
};

use crate::local::fjall_storage::FjallStorage;

type Result<T> = culprit::Result<T, GraftErr>;

#[derive(Clone, Debug)]
pub struct RuntimeHandle {
    inner: Arc<RuntimeHandleInner>,
}

#[derive(Debug)]
struct RuntimeHandleInner {
    _handle: JoinHandle<std::result::Result<(), RuntimeFatalErr>>,
    storage: Arc<FjallStorage>,
    rpc: RpcHandle,
}

impl RuntimeHandle {
    /// Spawn the Graft Runtime into the provided Tokio Runtime.
    /// Returns a `RuntimeHandle` which can be used to interact with the Graft Runtime.
    pub fn spawn(
        tokio_rt: &tokio::runtime::Handle,
        remote: Arc<Remote>,
        storage: Arc<FjallStorage>,
    ) -> RuntimeHandle {
        let (tx, rx) = mpsc::channel(8);

        // Make sure we have a runtime context while setting up streams
        let _tokio_guard = tokio_rt.enter();

        let rx = ReceiverStream::new(rx).map(Event::Rpc);
        let ticks =
            IntervalStream::new(tokio::time::interval(Duration::from_secs(1))).map(Event::Tick);
        let commits = storage.subscribe_commits().map(Event::Commits);
        let events = Box::pin(rx.merge(ticks).merge(commits));

        let runtime = Runtime::new(remote, storage.clone(), events);
        let handle = tokio_rt.spawn(runtime.start());

        RuntimeHandle {
            inner: Arc::new(RuntimeHandleInner {
                _handle: handle,
                storage,
                rpc: RpcHandle::new(tx),
            }),
        }
    }

    pub(crate) fn rpc(&self) -> &RpcHandle {
        &self.inner.rpc
    }

    pub fn volume_exists<S: Deref<Target = str>>(&self, name: S) -> Result<bool> {
        let name: VolumeName = VolumeName::from_str(&name)?;
        self.storage()
            .read()
            .named_volume_exists(&name)
            .or_into_ctx()
    }

    pub fn open_volume<S: Deref<Target = str>>(
        &self,
        name: S,
        remote_vid: Option<VolumeId>,
    ) -> Result<NamedVolume> {
        let name: VolumeName = name.parse().expect("invalid Volume name");
        // make sure the named volume exists
        self.storage()
            .open_named_volume(name.clone(), remote_vid)
            .or_into_ctx()?;
        Ok(NamedVolume::new(self.clone(), name))
    }

    pub(crate) fn storage(&self) -> &FjallStorage {
        &self.inner.storage
    }

    pub(crate) fn create_staged_segment(&self) -> SegmentIdx {
        // TODO: need to keep track of staged segments in memory to prevent the GC from clearing them
        SegmentIdx::new(SegmentId::random(), Graft::default())
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
            self.inner.rpc.fetch_segment_range(idx.sid.clone(), frame)?;

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

    pub(crate) fn page_status(&self, snapshot: &Snapshot, pageidx: PageIdx) -> Result<PageStatus> {
        let reader = self.storage().read();
        if let Some(commit) = reader.search_page(snapshot, pageidx).or_into_ctx()? {
            let idx = commit
                .segment_idx()
                .expect("BUG: commit claims to contain pageidx");
            if reader.has_page(idx.sid().clone(), pageidx).or_into_ctx()? {
                Ok(PageStatus::Available(commit.lsn()))
            } else {
                Ok(PageStatus::Pending(commit.lsn()))
            }
        } else {
            Ok(PageStatus::Empty(snapshot.lsn()))
        }
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
        let runtime = RuntimeHandle::spawn(tokio_rt.handle(), remote.clone(), storage);

        let volume = runtime.open_volume("leader", None).unwrap();
        let remote_vid = volume.status().unwrap().remote.vid().clone();

        // sanity check volume writer semantics
        let mut writer = volume.writer().unwrap();
        for i in [1u8, 2, 5, 9] {
            let pageidx = PageIdx::must_new(i as u32);
            let page = Page::test_filled(i);
            writer.write_page(pageidx, page.clone()).unwrap();
            assert_eq!(writer.read_page(pageidx).unwrap(), page);
        }
        writer.commit().unwrap();

        // sanity check volume reader semantics
        let reader = volume.reader().unwrap();
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
        let runtime_2 = RuntimeHandle::spawn(tokio_rt.handle(), remote.clone(), storage);

        // open the same named volume in the second runtime
        let volume_2 = runtime_2.open_volume("follower", Some(remote_vid)).unwrap();

        // let both runtimes run for a little while
        tokio_rt.block_on(async {
            // this sleep lets tokio advance time, allowing the runtime to flush all its jobs
            sleep(Duration::from_secs(5)).await;
            let tree = remote.testonly_format_tree().await;
            tracing::info!("remote tree\n{tree}")
        });

        assert_eq!(volume.status().unwrap().to_string(), "1 r1",);
        assert_eq!(volume_2.status().unwrap().to_string(), "1 r1",);

        // sanity check volume reader semantics in the second runtime
        let reader_2 = volume_2.reader().unwrap();
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
        let mut writer_2 = volume_2.writer().unwrap();
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

        assert_eq!(volume.status().unwrap().to_string(), "2 r2",);
        assert_eq!(volume_2.status().unwrap().to_string(), "2 r2",);

        // sanity check volume reader semantics in the first runtime
        let reader = volume.reader().unwrap();
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
