use std::{io, sync::Arc};

use bytes::Buf;
use culprit::Culprit;
use futures::{FutureExt, TryFutureExt};
use graft_core::SegmentId;
use measured::{CounterVec, Histogram, MetricGroup, metric::histogram::Thresholds};
use object_store::{ObjectStore, PutPayload, path::Path};
use thiserror::Error;
use tokio::sync::mpsc;

use crate::{
    metrics::labels::ResultLabelSet,
    supervisor::{SupervisedTask, TaskCfg, TaskCtx},
};

use super::{
    bus::{Bus, CommitSegmentReq, StoreSegmentReq},
    cache::Cache,
};

#[derive(Debug, Error)]
pub enum UploaderErr {
    #[error("failed to cache segment")]
    Cache,

    #[error("failed to upload segment")]
    Upload(io::ErrorKind),
}

impl From<object_store::Error> for UploaderErr {
    fn from(_: object_store::Error) -> Self {
        UploaderErr::Cache
    }
}

impl From<io::Error> for UploaderErr {
    fn from(err: io::Error) -> Self {
        UploaderErr::Upload(err.kind())
    }
}

#[derive(MetricGroup)]
#[metric(new())]
pub struct SegmentUploaderMetrics {
    /// Number of segments uploaded, broken down by result
    uploaded_segments: CounterVec<ResultLabelSet>,

    /// Size of segments uploaded in bytes
    // Generates 8 buckets from 128 KiB to 16 MiB
    #[metric(metadata = Thresholds::exponential_buckets(131_072.0, 2.0))]
    segment_size_bytes: Histogram<8>,
}

impl Default for SegmentUploaderMetrics {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SegmentUploaderTask<C> {
    metrics: Arc<SegmentUploaderMetrics>,
    input: mpsc::Receiver<StoreSegmentReq>,
    output: Bus<CommitSegmentReq>,
    store: Arc<dyn ObjectStore>,
    cache: Arc<C>,
}

impl<C: Cache> SupervisedTask for SegmentUploaderTask<C> {
    type Err = UploaderErr;

    fn cfg(&self) -> TaskCfg {
        TaskCfg { name: "segment-uploader" }
    }

    async fn run(mut self, ctx: TaskCtx) -> Result<(), Culprit<UploaderErr>> {
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

impl<C: Cache> SegmentUploaderTask<C> {
    pub fn new(
        metrics: Arc<SegmentUploaderMetrics>,
        input: mpsc::Receiver<StoreSegmentReq>,
        output: Bus<CommitSegmentReq>,
        store: Arc<dyn ObjectStore>,
        cache: Arc<C>,
    ) -> Self {
        Self { metrics, input, output, store, cache }
    }

    async fn handle_store_request(
        &mut self,
        req: StoreSegmentReq,
    ) -> Result<(), Culprit<UploaderErr>> {
        tracing::debug!("handling request: {:?}", req);

        let segment = req.segment;
        let sid = SegmentId::random();
        let path = Path::from(sid.pretty());
        let (segment, grafts) = segment.serialize(sid.clone());

        self.metrics
            .segment_size_bytes
            .observe(segment.remaining() as f64);

        let upload_task = self
            .store
            .put(&path, PutPayload::from_iter(segment.iter().cloned()))
            .err_into::<UploaderErr>()
            .inspect(|result| {
                self.metrics.uploaded_segments.inc(result.into());
            });
        let cache_task = self.cache.put(&sid, segment).err_into();

        tokio::try_join!(upload_task, cache_task)?;

        self.output
            .publish(CommitSegmentReq { sid, grafts: Arc::new(grafts) });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use graft_core::{gid::VolumeId, page::Page, pageidx};
    use object_store::memory::InMemory;

    use crate::segment::{cache::mem::MemCache, closed::ClosedSegment, open::OpenSegment};

    use super::*;

    #[graft_test::test]
    async fn test_uploader_sanity() {
        let (input_tx, input_rx) = mpsc::channel(1);
        let commit_bus = Bus::new(1);
        let mut commit_rx = commit_bus.subscribe();

        let store = Arc::new(InMemory::default());
        let cache = Arc::new(MemCache::default());

        let task = SegmentUploaderTask::new(
            Default::default(),
            input_rx,
            commit_bus,
            store.clone(),
            cache.clone(),
        );
        task.testonly_spawn();

        let mut segment = OpenSegment::default();

        // add a couple pages
        let vid = VolumeId::random();
        let page0 = Page::test_filled(1);
        let page1 = Page::test_filled(2);
        segment
            .insert(vid.clone(), pageidx!(1), page0.clone())
            .unwrap();
        segment
            .insert(vid.clone(), pageidx!(2), page1.clone())
            .unwrap();

        input_tx.send(StoreSegmentReq { segment }).await.unwrap();

        let commit = commit_rx.recv().await.unwrap();

        // check the stored segment
        let path = Path::from(commit.sid.pretty());
        let obj = store.get(&path).await.unwrap();
        let bytes = obj.bytes().await.unwrap();
        let segment = ClosedSegment::from_bytes(&bytes).unwrap();

        assert_eq!(segment.pages(), 2);
        assert_eq!(segment.find_page(vid.clone(), pageidx!(1)), Some(page0));
        assert_eq!(segment.find_page(vid.clone(), pageidx!(2)), Some(page1));

        // check that the cached and stored segment are identical
        let cached = cache.get(&commit.sid).await.unwrap().unwrap();
        assert_eq!(cached, bytes);
    }
}
