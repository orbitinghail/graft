//! A Segment writer is a task which builds open segments and passes them on

use std::time::{Duration, Instant};

use tokio::{sync::mpsc, time::sleep};

use crate::supervisor::{SupervisedTask, TaskCfg, TaskCtx};

use super::{
    bus::{RequestGroupAggregate, StoreSegmentRequest, WritePageRequest},
    open::OpenSegment,
};

pub struct SegmentWriterTask {
    input: mpsc::Receiver<WritePageRequest>,
    output: mpsc::Sender<StoreSegmentRequest>,

    groups: RequestGroupAggregate,
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
            // Three cases:
            // We receive a page write request
            // We receive a shutdown signal
            // Flush interval has passed

            let flush_in = self.next_flush.duration_since(Instant::now());

            tokio::select! {
                Some(req) = self.input.recv() => {
                    self.handle_page_request(req).await?;
                }

                _ = sleep(flush_in) => {
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
        input: mpsc::Receiver<WritePageRequest>,
        output: mpsc::Sender<StoreSegmentRequest>,
        flush_interval: Duration,
    ) -> Self {
        Self {
            input,
            output,
            groups: RequestGroupAggregate::default(),
            segment: Default::default(),
            flush_interval,
            next_flush: Instant::now() + flush_interval,
        }
    }

    async fn handle_page_request(&mut self, req: WritePageRequest) -> anyhow::Result<()> {
        tracing::debug!("handling request: {:?}", req);

        // if the segment is full, flush it and start a new one
        if self.segment.is_full() {
            self.handle_flush().await?;
        }

        self.groups.add(req.group);
        self.segment.insert(req.vid, req.offset, req.page)?;

        Ok(())
    }

    /// Flush the current segment and start a new one
    async fn handle_flush(&mut self) -> anyhow::Result<()> {
        // only flush non-empty segments
        if !self.segment.is_empty() {
            debug_assert_eq!(
                self.groups.total_count() as usize,
                self.segment.len(),
                "groups and segment size mismatch"
            );

            // send the current segment to the output
            self.output
                .send(StoreSegmentRequest {
                    groups: std::mem::take(&mut self.groups),
                    segment: std::mem::take(&mut self.segment),
                })
                .await?;
        }

        // update next_flush
        self.next_flush = Instant::now() + self.flush_interval;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use graft_core::{guid::VolumeId, page::Page};

    use crate::segment::bus::RequestGroup;

    use super::*;

    #[tokio::test(
        flavor = "current_thread",
        unhandled_panic = "shutdown_runtime",
        start_paused = true
    )]
    async fn test_writer_sanity() {
        let (input_tx, input_rx) = mpsc::channel(1);
        let (output_tx, mut output_rx) = mpsc::channel(1);

        let task = SegmentWriterTask::new(input_rx, output_tx, Duration::from_secs(1));
        task.testonly_spawn();

        // add a couple pages
        let vid = VolumeId::random();
        let group = RequestGroup::next();
        let page0 = Page::test_filled(1);
        let page1 = Page::test_filled(2);

        input_tx
            .send(WritePageRequest {
                group,
                vid: vid.clone(),
                offset: 0,
                page: page0.clone(),
            })
            .await
            .unwrap();

        input_tx
            .send(WritePageRequest {
                group,
                vid: vid.clone(),
                offset: 1,
                page: page1.clone(),
            })
            .await
            .unwrap();

        // wait for the flush
        let req = output_rx.recv().await.unwrap();

        assert_eq!(req.groups.count(group), 2);
        assert_eq!(req.segment.find_page(vid.clone(), 0).unwrap(), &page0);
        assert_eq!(req.segment.find_page(vid.clone(), 1).unwrap(), &page1);
    }
}
