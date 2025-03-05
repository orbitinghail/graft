//! A Segment writer is a task which builds open segments and passes them on

use std::sync::Arc;

use culprit::{Culprit, ResultExt};
use graft_core::{PageIdx, VolumeId, page::Page};
use measured::{Counter, MetricGroup};
use thiserror::Error;
use tokio::{
    sync::mpsc::{self, Sender, error::SendError},
    time::{Duration, Instant, sleep_until},
};

use super::{
    open::OpenSegment,
    uploader::{SegmentUploadMsg, StoreSegmentMsg},
};
use crate::{
    segment::closed::SEGMENT_MAX_PAGES,
    supervisor::{SupervisedTask, TaskCfg, TaskCtx},
};

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
    /// Number of pages written to segments
    page_writes: Counter,

    /// Number of segments that have been flushed
    flushed_segments: Counter,
}

#[derive(Debug)]
pub struct WritePagesMsg {
    vid: VolumeId,
    pages: Vec<(PageIdx, Page)>,
    segment_tx: Sender<SegmentUploadMsg>,
}

impl WritePagesMsg {
    pub fn new(
        vid: VolumeId,
        pages: Vec<(PageIdx, Page)>,
        segment_tx: Sender<SegmentUploadMsg>,
    ) -> Self {
        Self { vid, pages, segment_tx }
    }
}

pub struct SegmentWriterTask {
    metrics: Arc<SegmentWriterMetrics>,
    input: mpsc::Receiver<WritePagesMsg>,
    output: Sender<StoreSegmentMsg>,

    // the active open segment being written to
    segment: OpenSegment,
    // a list of writer's waiting for the segment to upload
    writers: Vec<Sender<SegmentUploadMsg>>,

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
                    self.handle_write(req).await?;
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
        input: mpsc::Receiver<WritePagesMsg>,
        output: Sender<StoreSegmentMsg>,
        flush_interval: Duration,
    ) -> Self {
        Self {
            metrics,
            input,
            output,
            segment: Default::default(),
            writers: Default::default(),
            flush_interval,
            next_flush: Instant::now() + flush_interval,
        }
    }

    async fn handle_write(&mut self, req: WritePagesMsg) -> Result<(), Culprit<WriterErr>> {
        tracing::trace!("writing {} pages to volume {:?}", req.pages.len(), req.vid);
        self.metrics.page_writes.inc_by(req.pages.len() as u64);

        let mut pages = req.pages.into_iter();
        loop {
            // flush current segment if full
            if !self.segment.has_space_for(&req.vid) {
                self.handle_flush().await?
            }

            // write as many pages as possible to the current segment
            self.segment
                .batch_insert(req.vid.clone(), &mut pages)
                .expect("segment is not full");
            self.writers.push(req.segment_tx.clone());

            // if the iterator is exhausted we are done
            if pages.len() == 0 {
                break;
            }
        }

        // if the segment is full of pages, we can trigger an early flush
        if self.segment.pages() == SEGMENT_MAX_PAGES {
            self.handle_flush().await?;
        }

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
                .send(StoreSegmentMsg {
                    segment: std::mem::take(&mut self.segment),
                    writers: std::mem::take(&mut self.writers),
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

        let (tx, _) = mpsc::channel(1);

        input_tx
            .send(WritePagesMsg {
                vid: vid.clone(),
                pages: vec![(pageidx!(1), page0.clone()), (pageidx!(2), page1.clone())],
                segment_tx: tx,
            })
            .await
            .unwrap();

        // wait for the flush
        let req = output_rx.recv().await.unwrap();

        assert_eq!(req.segment.find_page(&vid, pageidx!(1)).unwrap(), &page0);
        assert_eq!(req.segment.find_page(&vid, pageidx!(2)).unwrap(), &page1);
    }
}
