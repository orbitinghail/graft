use std::{convert::Infallible, sync::Arc};

use bytes::Buf;
use culprit::Culprit;
use event_listener::{Event, EventListener, IntoNotification};
use futures::FutureExt;
use measured::{CounterVec, Histogram, MetricGroup, metric::histogram::Thresholds};
use object_store::{ObjectStore, PutPayload, path::Path};
use thiserror::Error;
use tokio::sync::mpsc;

use crate::{
    api::error::ApiErrCtx,
    metrics::labels::ResultLabelSet,
    supervisor::{SupervisedTask, TaskCfg, TaskCtx},
};

use super::{cache::Cache, open::OpenSegment};

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

#[derive(Debug, Clone, Error)]
#[error("failed to upload segment")]
pub struct SegmentUploadErr;

impl From<SegmentUploadErr> for ApiErrCtx {
    fn from(_: SegmentUploadErr) -> Self {
        ApiErrCtx::SegmentUploadErr
    }
}

impl From<object_store::Error> for SegmentUploadErr {
    fn from(_: object_store::Error) -> Self {
        Self
    }
}

// Event that is triggered when a segment upload completes
pub type SegmentUploadEvent = Event<Result<(), SegmentUploadErr>>;
pub type SegmentUploadListener = EventListener<Result<(), SegmentUploadErr>>;

pub struct StoreSegmentMsg {
    segment: OpenSegment,
    complete: SegmentUploadEvent,
}

impl StoreSegmentMsg {
    pub fn new(segment: OpenSegment, complete: SegmentUploadEvent) -> Self {
        Self { segment, complete }
    }

    pub fn segment(&self) -> &OpenSegment {
        &self.segment
    }
}

impl std::fmt::Debug for StoreSegmentMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreSegmentMsg")
            .field("segment", &self.segment)
            .finish()
    }
}

pub struct SegmentUploaderTask<C> {
    metrics: Arc<SegmentUploaderMetrics>,
    input: mpsc::Receiver<StoreSegmentMsg>,
    store: Arc<dyn ObjectStore>,
    cache: Arc<C>,
}

impl<C: Cache + 'static> SupervisedTask for SegmentUploaderTask<C> {
    type Err = Infallible;

    fn cfg(&self) -> TaskCfg {
        TaskCfg { name: "segment-uploader" }
    }

    async fn run(mut self, ctx: TaskCtx) -> Result<(), Culprit<Infallible>> {
        loop {
            tokio::select! {
                Some(req) = self.input.recv() => {
                    self.handle_store_request(req).await;
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

impl<C: Cache + 'static> SegmentUploaderTask<C> {
    pub fn new(
        metrics: Arc<SegmentUploaderMetrics>,
        input: mpsc::Receiver<StoreSegmentMsg>,
        store: Arc<dyn ObjectStore>,
        cache: Arc<C>,
    ) -> Self {
        Self { metrics, input, store, cache }
    }

    #[tracing::instrument(name = "upload segment", skip(self), fields(sid))]
    async fn handle_store_request(&mut self, req: StoreSegmentMsg) {
        // skip uploading segment if all writers are gone
        if req.complete.total_listeners() == 0 {
            return;
        }

        // serialize the segment
        let segment = req.segment;
        let (sid, segment) = segment.serialize();

        tracing::Span::current().record("sid", sid.short());

        self.metrics
            .segment_size_bytes
            .observe(segment.remaining() as f64);

        // optimistically cache the segment
        // we don't care if this fails or we don't end up using the segment
        // since we can always redownload missing segments and segments ids are
        // never reused (and are globally unique)
        {
            let cache = self.cache.clone();
            let sid = sid.clone();
            let segment = segment.clone();
            tokio::spawn(async move {
                // small chance that we don't cache the segment, forcing a future
                // request to pull the segment from the store
                precept::maybe_fault!(0.1, "skipping segment cache when uploading segment", {
                    return;
                }, { "sid": sid });

                if let Err(err) = cache.put(&sid, segment).await {
                    tracing::error!("failed to cache segment {:?}\n{:?}", sid, err);
                }
            });
        }

        let path = Path::from(sid.pretty());
        if let Err(err) = self
            .store
            .put(&path, PutPayload::from_iter(segment.iter().cloned()))
            .inspect(|result| {
                self.metrics.uploaded_segments.inc(result.into());
            })
            .await
        {
            tracing::error!("failed to upload segment {:?}\n{:?}", sid, err);
            req.complete.notify(usize::MAX.tag(Err(SegmentUploadErr)));
        } else {
            req.complete.notify(usize::MAX.tag(Ok(())));
        }
    }
}

#[cfg(test)]
mod tests {
    use graft_core::{gid::VolumeId, page::Page, pageidx};
    use object_store::memory::InMemory;

    use crate::segment::{cache::mem::MemCache, closed::ClosedSegment};

    use super::*;

    #[graft_test::test]
    async fn test_uploader_sanity() {
        let (input_tx, input_rx) = mpsc::channel(1);

        let store = Arc::new(InMemory::default());
        let cache = Arc::new(MemCache::default());

        let task =
            SegmentUploaderTask::new(Default::default(), input_rx, store.clone(), cache.clone());
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

        let sid = segment.sid().clone();

        let event = Event::with_tag();
        let listener = event.listen();
        input_tx
            .send(StoreSegmentMsg { segment, complete: event })
            .await
            .unwrap();

        listener.await.unwrap();

        // check the stored segment
        let path = Path::from(sid.pretty());
        let obj = store.get(&path).await.unwrap();
        let bytes = obj.bytes().await.unwrap();
        let segment = ClosedSegment::from_bytes(&bytes).unwrap();

        assert_eq!(segment.pages(), 2);
        assert_eq!(segment.find_page(&vid, pageidx!(1)), Some(page0));
        assert_eq!(segment.find_page(&vid, pageidx!(2)), Some(page1));

        // check that the cached and stored segment are identical
        let cached = cache.get(&sid).await.unwrap().unwrap();
        assert_eq!(cached, bytes);
    }
}
