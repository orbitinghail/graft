use tokio::sync::{broadcast, mpsc};

use crate::segment::bus::{Bus, CommitSegmentReq, WritePageReq};

pub struct ApiState {
    page_tx: mpsc::Sender<WritePageReq>,
    commit_bus: Bus<CommitSegmentReq>,
}

impl ApiState {
    pub fn new(page_tx: mpsc::Sender<WritePageReq>, commit_bus: Bus<CommitSegmentReq>) -> Self {
        Self { page_tx, commit_bus }
    }

    pub async fn write_page(&self, req: WritePageReq) {
        self.page_tx.send(req).await.unwrap();
    }

    pub fn subscribe_commits(&self) -> broadcast::Receiver<CommitSegmentReq> {
        self.commit_bus.subscribe()
    }
}
