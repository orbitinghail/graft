//! A Segment writer is a task which builds open segments and passes them on

use std::sync::Arc;

use culprit::{Culprit, ResultExt};
use event_listener::Event;
use graft_core::{PageIdx, SegmentId, VolumeId, page::Page};
use measured::{Counter, MetricGroup};
use splinter::Splinter;
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    time::{Duration, Instant, sleep_until},
};

use super::{
    open::OpenSegment,
    uploader::{SegmentUploadEvent, SegmentUploadListener, StoreSegmentMsg},
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

impl<T> From<mpsc::error::SendError<T>> for WriterErr {
    fn from(_: mpsc::error::SendError<T>) -> Self {
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

pub struct WritePagesRequest {
    vid: VolumeId,
    pages: Vec<(PageIdx, Page)>,
    reply: oneshot::Sender<WritePagesResponse>,
}

impl WritePagesRequest {
    pub fn new(
        vid: VolumeId,
        pages: Vec<(PageIdx, Page)>,
        reply: oneshot::Sender<WritePagesResponse>,
    ) -> Self {
        Self { vid, pages, reply }
    }
}

pub struct WritePagesResponse {
    segments: Vec<(SegmentId, Splinter, SegmentUploadListener)>,
}

impl WritePagesResponse {
    pub fn len(&self) -> usize {
        self.segments.len()
    }
}

impl IntoIterator for WritePagesResponse {
    type Item = (SegmentId, Splinter, SegmentUploadListener);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.segments.into_iter()
    }
}

pub struct SegmentWriterTask {
    metrics: Arc<SegmentWriterMetrics>,
    input: mpsc::Receiver<WritePagesRequest>,
    output: mpsc::Sender<StoreSegmentMsg>,

    // the active open segment being written to
    segment: OpenSegment,
    event: SegmentUploadEvent,

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
        input: mpsc::Receiver<WritePagesRequest>,
        output: mpsc::Sender<StoreSegmentMsg>,
        flush_interval: Duration,
    ) -> Self {
        Self {
            metrics,
            input,
            output,
            segment: Default::default(),
            event: Event::with_tag(),
            flush_interval,
            next_flush: Instant::now() + flush_interval,
        }
    }

    async fn handle_write(&mut self, req: WritePagesRequest) -> Result<(), Culprit<WriterErr>> {
        tracing::trace!("writing {} pages to volume {:?}", req.pages.len(), req.vid);
        self.metrics.page_writes.inc_by(req.pages.len() as u64);

        let mut segments = Vec::new();
        let mut pages = req.pages.into_iter();
        loop {
            // flush current segment if full or it already contains the current volume.
            // this ensures that two separate write requests to the same volume
            // can't end up in the same segment
            if !self.segment.has_space_for(&req.vid) || self.segment.contains_vid(&req.vid) {
                self.handle_flush().await?
            }

            // write as many pages as possible to the current segment
            let graft = self
                .segment
                .batch_insert(req.vid.clone(), &mut pages)
                .expect("segment was just verified to have space");
            segments.push((self.segment.sid().clone(), graft, self.event.listen()));

            // if the iterator is exhausted we are done
            if pages.len() == 0 {
                break;
            }
        }

        // if the segment is full of pages, we can trigger an early flush
        if self.segment.pages() == SEGMENT_MAX_PAGES {
            self.handle_flush().await?;
        }

        // reply to the write request
        let _ = req.reply.send(WritePagesResponse { segments });

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
                .send(StoreSegmentMsg::new(
                    std::mem::take(&mut self.segment),
                    std::mem::replace(&mut self.event, Event::with_tag()),
                ))
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

        let (tx, rx) = oneshot::channel();

        input_tx
            .send(WritePagesRequest {
                vid: vid.clone(),
                pages: vec![(pageidx!(1), page0.clone()), (pageidx!(2), page1.clone())],
                reply: tx,
            })
            .await
            .unwrap();

        // wait for the reply
        let response = rx.await.unwrap();

        let (_, graft, _) = response.into_iter().next().unwrap();
        assert!(graft.contains(1));
        assert!(graft.contains(2));

        // wait for the flush
        let flush = output_rx.recv().await.unwrap();
        let segment = flush.segment();
        assert_eq!(segment.find_page(&vid, pageidx!(1)), Some(&page0));
        assert_eq!(segment.find_page(&vid, pageidx!(2)), Some(&page1));
    }
}
