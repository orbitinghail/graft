use std::sync::Arc;

use futures::FutureExt;
use graft_core::guid::SegmentId;
use object_store::{path::Path, ObjectStore};
use tokio::sync::mpsc;

use crate::{
    storage::cache::Cache,
    supervisor::{SupervisedTask, TaskCfg, TaskCtx},
};

use super::bus::{CommitSegmentRequest, StoreSegmentRequest};

pub struct SegmentUploaderTask<O, C> {
    input: mpsc::Receiver<StoreSegmentRequest>,
    output: mpsc::Sender<CommitSegmentRequest>,
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
        input: mpsc::Receiver<StoreSegmentRequest>,
        output: mpsc::Sender<CommitSegmentRequest>,
        store: Arc<O>,
        cache: Arc<C>,
    ) -> Self {
        Self { input, output, store, cache }
    }

    async fn handle_store_request(&mut self, req: StoreSegmentRequest) -> anyhow::Result<()> {
        let groups = req.groups;
        let segment = req.segment;
        let sid = SegmentId::random();
        let path = Path::from(sid.pretty());
        let segment = segment.serialize();

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
            .send(CommitSegmentRequest { groups, sid })
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use graft_core::{guid::VolumeId, page::Page};
    use object_store::memory::InMemory;

    use crate::{
        segment::{
            bus::{RequestGroup, RequestGroupAggregate},
            closed::ClosedSegment,
            open::OpenSegment,
        },
        storage::mem::MemCache,
    };

    use super::*;

    #[tokio::test(flavor = "current_thread", unhandled_panic = "shutdown_runtime")]

    async fn test_uploader_sanity() {
        let (input_tx, input_rx) = mpsc::channel(1);
        let (output_tx, mut output_rx) = mpsc::channel(1);

        let store = Arc::new(InMemory::default());
        let cache = Arc::new(MemCache::default());

        let task = SegmentUploaderTask::new(input_rx, output_tx, store.clone(), cache.clone());
        task.testonly_spawn();

        let mut segment = OpenSegment::default();
        let group = RequestGroup::next();
        let mut groups = RequestGroupAggregate::default();

        // add a couple pages
        let vid = VolumeId::random();
        let page0 = Page::test_filled(1);
        let page1 = Page::test_filled(2);
        segment.insert(vid.clone(), 0, page0.clone()).unwrap();
        groups.add(group);
        segment.insert(vid.clone(), 1, page1.clone()).unwrap();
        groups.add(group);

        input_tx
            .send(StoreSegmentRequest { groups: groups.clone(), segment })
            .await
            .unwrap();

        let commit = output_rx.recv().await.unwrap();

        // groups should be unchanged
        assert_eq!(commit.groups, groups);

        // check the stored segment
        let path = Path::from(commit.sid.pretty());
        let obj = store.get(&path).await.unwrap();
        let bytes = obj.bytes().await.unwrap();
        let segment = ClosedSegment::from_bytes(&bytes).unwrap();

        assert_eq!(segment.len(), 2);
        assert_eq!(segment.find_page(vid.clone(), 0), Some(page0.as_ref()));
        assert_eq!(segment.find_page(vid.clone(), 1), Some(page1.as_ref()));

        // check that the cached and stored segment are identical
        let cached = cache.get(&commit.sid).await.unwrap().unwrap();
        assert_eq!(cached, bytes);
    }
}
