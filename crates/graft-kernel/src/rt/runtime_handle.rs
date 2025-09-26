use std::{sync::Arc, time::Duration};

use culprit::Culprit;
use graft_core::{PageIdx, SegmentId, commit::SegmentIdx, graft::Graft, page::Page};
use tokio::task::JoinHandle;

use crate::{
    local::fjall_storage::FjallStorageErr,
    named_volume::NamedVolume,
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

#[derive(Clone, Debug)]
pub struct RuntimeHandle {
    inner: Arc<RuntimeHandleInner>,
}

#[derive(Debug)]
struct RuntimeHandleInner {
    handle: JoinHandle<Result<(), RuntimeFatalErr>>,
    storage: Arc<FjallStorage>,
    rpc: RpcHandle,
}

impl RuntimeHandle {
    /// Spawn the Graft Runtime into the provided Tokio Runtime.
    /// Returns a `RuntimeHandle` which can be used to interact with the Graft Runtime.
    pub fn spawn(handle: &tokio::runtime::Handle, storage: Arc<FjallStorage>) -> RuntimeHandle {
        let (tx, rx) = mpsc::channel(8);

        let rx = ReceiverStream::new(rx).map(Event::Rpc);
        let ticks =
            IntervalStream::new(tokio::time::interval(Duration::from_secs(1))).map(|_| Event::Tick);
        let events = Box::pin(rx.merge(ticks));

        let runtime = Runtime::new(storage.clone(), events);
        let handle = handle.spawn(runtime.start());

        RuntimeHandle {
            inner: Arc::new(RuntimeHandleInner { handle, storage, rpc: RpcHandle::new(tx) }),
        }
    }

    pub fn open_volume(&self, name: VolumeName) -> Result<NamedVolume, Culprit<FjallStorageErr>> {
        // make sure the named volume exists
        self.storage().open_named_volume(name.clone())?;
        Ok(NamedVolume::new(self.clone(), name))
    }

    pub(crate) fn storage(&self) -> &FjallStorage {
        &self.inner.storage
    }

    pub(crate) fn create_staged_segment(&self) -> SegmentIdx {
        // TODO: need to keep track of staged segments in memory to prevent the GC from clearing them
        SegmentIdx::new(SegmentId::random(), Graft::default())
    }

    pub(crate) fn read_page(
        &self,
        snapshot: &Snapshot,
        pageidx: PageIdx,
    ) -> Result<Page, Culprit<FjallStorageErr>> {
        let storage = self.storage().read();
        if let Some(commit) = storage.search_page(snapshot, pageidx)? {
            let idx = commit
                .segment_idx()
                .expect("BUG: commit claims to contain pageidx");

            if let Some(page) = storage.read_page(idx.sid().clone(), pageidx)? {
                return Ok(page);
            }

            // page is not available locally, fall back to loading the page from remote storage.
            // let frame = idx
            //     .frame_for_pageidx(pageidx)
            //     .expect("commit claims to contain pageidx but no frame found");

            todo!("load page from remote storage")
        }

        Ok(Page::EMPTY)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use graft_core::{PageIdx, page::Page};

    use crate::{
        local::fjall_storage::FjallStorage, rt::runtime_handle::RuntimeHandle,
        volume_name::VolumeName, volume_reader::VolumeRead, volume_writer::VolumeWrite,
    };

    #[graft_test::test]
    async fn runtime_sanity() {
        let storage = Arc::new(FjallStorage::open_temporary().unwrap());
        let tokio_rt = tokio::runtime::Handle::current();
        let runtime = RuntimeHandle::spawn(&tokio_rt, storage);

        let volume = runtime.open_volume(VolumeName::DEFAULT).unwrap();

        // sanity check volume writer semantics
        let mut writer = volume.writer().unwrap();
        for i in [1u8, 2, 5, 9] {
            let pageidx = PageIdx::new(i as u32);
            let page = Page::test_filled(i);
            writer.write_page(pageidx, page.clone()).unwrap();
            assert_eq!(writer.read_page(pageidx).unwrap(), page);
        }
        writer.commit().unwrap();

        // sanity check volume reader semantics
        let reader = volume.reader().unwrap();
        for i in [1u8, 2, 5, 9] {
            let pageidx = PageIdx::new(i as u32);
            let page = Page::test_filled(i);
            assert_eq!(
                reader.read_page(pageidx).unwrap().into_bytes(),
                page.into_bytes()
            );
        }
    }
}
