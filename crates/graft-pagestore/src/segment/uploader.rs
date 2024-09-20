use std::{io, sync::Arc};

use graft_core::guid::SegmentId;
use object_store::{path::Path, ObjectStore};
use tokio::sync::mpsc;

use crate::supervisor::{SupervisedTask, TaskCfg, TaskCtx};

use super::bus::{CommitSegmentRequest, StoreSegmentRequest};

pub struct SegmentUploaderTask {
    input: mpsc::Receiver<StoreSegmentRequest>,
    output: mpsc::Sender<CommitSegmentRequest>,
    store: Arc<dyn ObjectStore>,
}

impl SupervisedTask for SegmentUploaderTask {
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

impl SegmentUploaderTask {
    pub fn new(
        input: mpsc::Receiver<StoreSegmentRequest>,
        output: mpsc::Sender<CommitSegmentRequest>,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self { input, output, store }
    }

    async fn handle_store_request(&mut self, req: StoreSegmentRequest) -> anyhow::Result<()> {
        let groups = req.groups;
        let segment = req.segment;
        let sid = SegmentId::random();

        let mut buf = io::Cursor::new(Vec::with_capacity(segment.encoded_size()));
        segment.write_to(&mut buf)?;
        let buf = buf.into_inner();

        let path = Path::from(sid.pretty());
        self.store.put(&path, buf.into()).await?;

        self.output
            .send(CommitSegmentRequest { groups, sid })
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use graft_core::{
        guid::VolumeId,
        page::{Page, PAGESIZE},
    };
    use object_store::memory::InMemory;

    use crate::segment::{
        bus::{RequestGroup, RequestGroupAggregate},
        closed::ClosedSegment,
        open::OpenSegment,
    };

    use super::*;

    #[tokio::test(flavor = "current_thread", unhandled_panic = "shutdown_runtime")]

    async fn test_uploader_sanity() {
        let (input_tx, input_rx) = mpsc::channel(1);
        let (output_tx, mut output_rx) = mpsc::channel(1);

        let store = Arc::new(InMemory::default());

        let task = SegmentUploaderTask::new(input_rx, output_tx, store.clone());
        task.testonly_spawn();

        let mut segment = OpenSegment::default();
        let group = RequestGroup::next();
        let mut groups = RequestGroupAggregate::default();
        groups.add(group);

        // add a couple pages
        let vid = VolumeId::random();
        let page0 = Page::from(&[1; PAGESIZE]);
        let page1 = Page::from(&[2; PAGESIZE]);
        segment.insert(vid.clone(), 0, page0.clone()).unwrap();
        segment.insert(vid.clone(), 1, page1.clone()).unwrap();

        input_tx
            .send(StoreSegmentRequest { groups: groups.clone(), segment })
            .await
            .unwrap();

        let commit = output_rx.recv().await.unwrap();

        // groups should be unchanged
        assert_eq!(commit.groups, groups);

        let path = Path::from(commit.sid.pretty());
        let obj = store.get(&path).await.unwrap();
        let bytes = obj.bytes().await.unwrap();
        let segment = ClosedSegment::from_bytes(&bytes).unwrap();

        assert_eq!(segment.len(), 2);
        assert_eq!(segment.find_page(vid.clone(), 0), Some(page0.as_ref()));
        assert_eq!(segment.find_page(vid.clone(), 1), Some(page1.as_ref()));
    }
}
