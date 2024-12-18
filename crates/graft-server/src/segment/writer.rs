//! A Segment writer is a task which builds open segments and passes them on

use std::sync::Arc;

use measured::{Counter, MetricGroup};
use tokio::{
    sync::mpsc,
    time::{sleep_until, Duration, Instant},
};

use super::{
    bus::{StoreSegmentReq, WritePageReq},
    open::OpenSegment,
};
use crate::supervisor::{SupervisedTask, TaskCfg, TaskCtx};

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
    fn cfg(&self) -> TaskCfg {
        TaskCfg { name: "segment-writer" }
    }

    async fn run(mut self, ctx: TaskCtx) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                Some(req) = self.input.recv() => {
                    self.handle_page_request(req).await?;
                }

                _ = sleep_until(self.next_flush) => {
                    tracing::debug!(?self.flush_interval, "flush interval elapsed; flushing segment");
                    self.handle_flush().await?;
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

    async fn handle_page_request(&mut self, req: WritePageReq) -> anyhow::Result<()> {
        tracing::debug!("handling request: {:?}", req);
        self.metrics.page_writes.inc();

        // if the segment is full, flush it and start a new one
        if !self.segment.has_space_for(&req.vid) {
            self.handle_flush().await?;
        }

        self.segment.insert(req.vid, req.offset, req.page)?;

        Ok(())
    }

    /// Flush the current segment and start a new one
    async fn handle_flush(&mut self) -> anyhow::Result<()> {
        // only flush non-empty segments
        if !self.segment.is_empty() {
            // send the current segment to the output
            self.output
                .send(StoreSegmentReq {
                    segment: std::mem::take(&mut self.segment),
                })
                .await?;

            self.metrics.flushed_segments.inc();
        }

        // update next_flush
        self.next_flush = Instant::now() + self.flush_interval;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use graft_core::{page::Page, page_offset::PageOffset, VolumeId};

    use super::*;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
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
                offset: PageOffset::new(0),
                page: page0.clone(),
            })
            .await
            .unwrap();

        input_tx
            .send(WritePageReq {
                vid: vid.clone(),
                offset: PageOffset::new(1),
                page: page1.clone(),
            })
            .await
            .unwrap();

        // wait for the flush
        let req = output_rx.recv().await.unwrap();

        assert_eq!(
            req.segment.find_page(&vid, PageOffset::new(0)).unwrap(),
            &page0
        );
        assert_eq!(
            req.segment.find_page(&vid, PageOffset::new(1)).unwrap(),
            &page1
        );
    }
}
