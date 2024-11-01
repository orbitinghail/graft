use std::sync::Arc;

use futures::FutureExt;
use graft_core::SegmentId;
use object_store::{path::Path, ObjectStore};
use tokio::sync::mpsc;

use crate::supervisor::{SupervisedTask, TaskCfg, TaskCtx};

use super::{
    bus::{Bus, CommitSegmentReq, StoreSegmentReq},
    cache::Cache,
};

pub struct SegmentUploaderTask<O, C> {
    input: mpsc::Receiver<StoreSegmentReq>,
    output: Bus<CommitSegmentReq>,
    store: Arc<O>,
    cache: Arc<C>,
}

impl<O: ObjectStore, C: Cache> SupervisedTask for SegmentUploaderTask<O, C> {
    fn cfg(&self) -> TaskCfg {
        TaskCfg { name: "segment-uploader" }
    }

    async fn run(mut self, ctx: TaskCtx) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                Some(req) = self.input.recv() => {
                    self.handle_store_request(req).await?;
                }

                _ = ctx.wait_shutdown() => {
                    // Shutdown immediately, discarding any pending writes
                    break;
                }
            }
        }
        Ok(())
    }
}

impl<O: ObjectStore, C: Cache> SegmentUploaderTask<O, C> {
    pub fn new(
        input: mpsc::Receiver<StoreSegmentReq>,
        output: Bus<CommitSegmentReq>,
        store: Arc<O>,
        cache: Arc<C>,
    ) -> Self {
        Self { input, output, store, cache }
    }

    async fn handle_store_request(&mut self, req: StoreSegmentReq) -> anyhow::Result<()> {
        tracing::debug!("handling request: {:?}", req);

        let segment = req.segment;
        let sid = SegmentId::random();
        let path = Path::from(sid.pretty());
        let (segment, offsets) = segment.serialize();

        let upload_task = self
            .store
            .put(&path, segment.clone().into())
            .map(|inner| inner.map_err(|e| anyhow::anyhow!(e)));
        let cache_task = self
            .cache
            .put(&sid, segment)
            .map(|inner| inner.map_err(|e| anyhow::anyhow!(e)));

        tokio::try_join!(upload_task, cache_task)?;

        self.output
            .publish(CommitSegmentReq { sid, offsets: Arc::new(offsets) })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use graft_core::{gid::VolumeId, page::Page};
    use object_store::memory::InMemory;

    use crate::segment::{cache::mem::MemCache, closed::ClosedSegment, open::OpenSegment};

    use super::*;

    #[tokio::test(flavor = "current_thread", unhandled_panic = "shutdown_runtime")]

    async fn test_uploader_sanity() {
        let (input_tx, input_rx) = mpsc::channel(1);
        let commit_bus = Bus::new(1);
        let mut commit_rx = commit_bus.subscribe();

        let store = Arc::new(InMemory::default());
        let cache = Arc::new(MemCache::default());

        let task = SegmentUploaderTask::new(input_rx, commit_bus, store.clone(), cache.clone());
        task.testonly_spawn();

        let mut segment = OpenSegment::default();

        // add a couple pages
        let vid = VolumeId::random();
        let page0 = Page::test_filled(1);
        let page1 = Page::test_filled(2);
        segment.insert(vid.clone(), 0, page0.clone()).unwrap();
        segment.insert(vid.clone(), 1, page1.clone()).unwrap();

        input_tx.send(StoreSegmentReq { segment }).await.unwrap();

        let commit = commit_rx.recv().await.unwrap();

        // check the stored segment
        let path = Path::from(commit.sid.pretty());
        let obj = store.get(&path).await.unwrap();
        let bytes = obj.bytes().await.unwrap();
        let segment = ClosedSegment::from_bytes(&bytes).unwrap();

        assert_eq!(segment.len(), 2);
        assert_eq!(segment.find_page(vid.clone(), 0), Some(page0));
        assert_eq!(segment.find_page(vid.clone(), 1), Some(page1));

        // check that the cached and stored segment are identical
        let cached = cache.get(&commit.sid).await.unwrap().unwrap();
        assert_eq!(cached, bytes);
    }
}
