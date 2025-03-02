use std::{convert::Infallible, process, sync::Arc};

use bytes::Buf;
use culprit::Culprit;
use futures::FutureExt;
use graft_core::{SegmentId, VolumeId};
use graft_tracing::running_in_antithesis;
use measured::{CounterVec, Histogram, MetricGroup, metric::histogram::Thresholds};
use object_store::{ObjectStore, PutPayload, path::Path};
use thiserror::Error;
use tokio::sync::mpsc;

use crate::{
    api::error::ApiErrCtx,
    metrics::labels::ResultLabelSet,
    supervisor::{SupervisedTask, TaskCfg, TaskCtx},
};

use super::{bus::Bus, cache::Cache, multigraft::MultiGraft, open::OpenSegment};

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
#[error("object store error occurred while uploading segment")]
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

#[derive(Debug)]
pub struct StoreSegmentMsg {
    pub segment: OpenSegment,
}

#[derive(Debug, Clone)]
pub enum SegmentUploadMsg {
    Success {
        grafts: Arc<MultiGraft>,
        sid: SegmentId,
    },
    Failure {
        grafts: Arc<MultiGraft>,
        err: Culprit<SegmentUploadErr>,
    },
}

impl SegmentUploadMsg {
    pub fn graft(&self, vid: &VolumeId) -> Option<&splinter::SplinterRef<bytes::Bytes>> {
        match self {
            Self::Success { grafts, .. } => grafts.get(vid),
            Self::Failure { grafts, .. } => grafts.get(vid),
        }
    }

    pub fn sid(&self) -> Result<&SegmentId, Culprit<SegmentUploadErr>> {
        match self {
            Self::Success { sid, .. } => Ok(sid),
            Self::Failure { err, .. } => Err(err.clone()),
        }
    }
}

pub struct SegmentUploaderTask<C> {
    metrics: Arc<SegmentUploaderMetrics>,
    input: mpsc::Receiver<StoreSegmentMsg>,
    output: Bus<SegmentUploadMsg>,
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
        output: Bus<SegmentUploadMsg>,
        store: Arc<dyn ObjectStore>,
        cache: Arc<C>,
    ) -> Self {
        Self { metrics, input, output, store, cache }
    }

    #[tracing::instrument(name = "upload segment", skip(self))]
    async fn handle_store_request(&mut self, req: StoreSegmentMsg) {
        let segment = req.segment;
        let sid = SegmentId::random();
        let path = Path::from(sid.pretty());
        let (segment, grafts) = segment.serialize(sid.clone());

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
                // 20% chance that we don't cache the segment, forcing a future
                // request to pull the segment from the store
                precept::maybe_fault!(0.2, "skipping segment cache when uploading segment", {
                    return;
                });

                if let Err(err) = cache.put(&sid, segment).await {
                    tracing::error!("failed to cache segment {:?}\n{:?}", sid, err);

                    // for now, we inject a process crash when running in antithesis
                    // as it's not clear yet if antithesis can cause the cache to fail
                    // TODO: replace this with a precept::fault once confirmed
                    // that antithesis can't reach this failure
                    if running_in_antithesis() {
                        tracing::error!("crashing process on cache failure");
                        process::exit(1);
                    }
                }
            });
        }

        let grafts = Arc::new(grafts);

        if let Err(err) = self
            .store
            .put(&path, PutPayload::from_iter(segment.iter().cloned()))
            .inspect(|result| {
                self.metrics.uploaded_segments.inc(result.into());
            })
            .await
        {
            self.output
                .publish(SegmentUploadMsg::Failure { grafts, err: Culprit::from_err(err) });
        } else {
            self.output
                .publish(SegmentUploadMsg::Success { grafts, sid });
        }
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
        let segment_upload_bus = Bus::new(1);
        let mut segment_rx = segment_upload_bus.subscribe();

        let store = Arc::new(InMemory::default());
        let cache = Arc::new(MemCache::default());

        let task = SegmentUploaderTask::new(
            Default::default(),
            input_rx,
            segment_upload_bus,
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

        input_tx.send(StoreSegmentMsg { segment }).await.unwrap();

        let segment = segment_rx.recv().await.unwrap();

        let sid = segment.sid().unwrap();

        // check the graft
        let graft = segment.graft(&vid).unwrap();
        assert_eq!(graft.cardinality(), 2);
        assert!(graft.contains(1));
        assert!(graft.contains(2));

        // check the stored segment
        let path = Path::from(sid.pretty());
        let obj = store.get(&path).await.unwrap();
        let bytes = obj.bytes().await.unwrap();
        let segment = ClosedSegment::from_bytes(&bytes).unwrap();

        assert_eq!(segment.pages(), 2);
        assert_eq!(segment.find_page(vid.clone(), pageidx!(1)), Some(page0));
        assert_eq!(segment.find_page(vid.clone(), pageidx!(2)), Some(page1));

        // check that the cached and stored segment are identical
        let cached = cache.get(&sid).await.unwrap().unwrap();
        assert_eq!(cached, bytes);
    }
}
