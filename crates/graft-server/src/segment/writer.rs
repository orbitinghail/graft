//! A Segment writer is a task which builds open segments and passes them on

use std::sync::Arc;

use culprit::{Culprit, ResultExt};
use measured::{Counter, MetricGroup};
use thiserror::Error;
use tokio::{
    sync::mpsc::{self, error::SendError},
    time::{Duration, Instant, sleep_until},
};

use super::{
    bus::{StoreSegmentReq, WritePageReq},
    open::OpenSegment,
};
use crate::supervisor::{SupervisedTask, TaskCfg, TaskCtx};

#[derive(Debug, Error)]
pub enum WriterErr {
    #[error("output channel is closed")]
    OutputChannelClosed,
}

impl<T> From<SendError<T>> for WriterErr {
    fn from(_: SendError<T>) -> Self {
        Self::OutputChannelClosed
    }
}

#[derive(MetricGroup, Default)]
pub struct SegmentWriterMetrics {
    /// Number of page writes
    page_writes: Counter,

    /// Number of segments that have been flushed
    flushed_segments: Counter,
}

pub struct SegmentWriterTask {
    metrics: Arc<SegmentWriterMetrics>,
    input: mpsc::Receiver<WritePageReq>,
    output: mpsc::Sender<StoreSegmentReq>,

    segment: OpenSegment,
    flush_interval: Duration,
    next_flush: Instant,
}

impl SupervisedTask for SegmentWriterTask {
    type Err = WriterErr;

    fn cfg(&self) -> TaskCfg {
        TaskCfg { name: "segment-writer" }
    }

    async fn run(mut self, ctx: TaskCtx) -> Result<(), Culprit<WriterErr>> {
        loop {
            tokio::select! {
                biased;

                _ = ctx.wait_shutdown() => {
                    // Shutdown immediately, discarding any pending writes
                    break;
                }

                Some(req) = self.input.recv() => {
                    self.handle_page_request(req).await?;
                }

                _ = sleep_until(self.next_flush) => {
                    self.handle_flush().await?;
                }
            }
        }
        Ok(())
    }
}

impl SegmentWriterTask {
    pub fn new(
        metrics: Arc<SegmentWriterMetrics>,
        input: mpsc::Receiver<WritePageReq>,
        output: mpsc::Sender<StoreSegmentReq>,
        flush_interval: Duration,
    ) -> Self {
        Self {
            metrics,
            input,
            output,
            segment: Default::default(),
            flush_interval,
            next_flush: Instant::now() + flush_interval,
        }
    }

    async fn handle_page_request(&mut self, req: WritePageReq) -> Result<(), Culprit<WriterErr>> {
        tracing::trace!("writing page {} to volume {:?}", req.pageidx, req.vid);
        self.metrics.page_writes.inc();

        // if the segment is full, flush it and start a new one
        if !self.segment.has_space_for(&req.vid) {
            self.handle_flush().await?;
        }

        self.segment
            .insert(req.vid, req.pageidx, req.page)
            .expect("segment is not full");

        Ok(())
    }

    /// Flush the current segment and start a new one
    async fn handle_flush(&mut self) -> Result<(), Culprit<WriterErr>> {
        // only flush non-empty segments
        if !self.segment.is_empty() {
            tracing::trace!(
                "flushing segment to uploader with {} pages and {} volumes",
                self.segment.pages(),
                self.segment.volumes()
            );

            precept::expect_sometimes!(
                self.segment.volumes() > 1,
                "flushed segment has more than one volume",
                {
                    "volumes": self.segment.volumes(),
                    "pages": self.segment.pages(),
                }
            );

            // send the current segment to the output
            self.output
                .send(StoreSegmentReq {
                    segment: std::mem::take(&mut self.segment),
                })
                .await
                .or_into_ctx()?;

            self.metrics.flushed_segments.inc();
        }

        // update next_flush
        self.next_flush = Instant::now() + self.flush_interval;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use graft_core::{VolumeId, page::Page, pageidx};

    use super::*;

    #[graft_test::test]
    async fn test_writer_sanity() {
        let (input_tx, input_rx) = mpsc::channel(1);
        let (output_tx, mut output_rx) = mpsc::channel(1);

        let task = SegmentWriterTask::new(
            Default::default(),
            input_rx,
            output_tx,
            Duration::from_secs(1),
        );
        task.testonly_spawn();

        // add a couple pages
        let vid = VolumeId::random();
        let page0 = Page::test_filled(1);
        let page1 = Page::test_filled(2);

        input_tx
            .send(WritePageReq {
                vid: vid.clone(),
                pageidx: pageidx!(1),
                page: page0.clone(),
            })
            .await
            .unwrap();

        input_tx
            .send(WritePageReq {
                vid: vid.clone(),
                pageidx: pageidx!(2),
                page: page1.clone(),
            })
            .await
            .unwrap();

        // wait for the flush
        let req = output_rx.recv().await.unwrap();

        assert_eq!(req.segment.find_page(&vid, pageidx!(1)).unwrap(), &page0);
        assert_eq!(req.segment.find_page(&vid, pageidx!(2)).unwrap(), &page1);
    }
}
