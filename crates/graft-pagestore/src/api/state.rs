use tokio::sync::mpsc::{Receiver, Sender};

use crate::segment::bus::{CommitSegmentRequest, WritePageRequest};

pub struct ApiState {
    page_tx: Sender<WritePageRequest>,
    commit_rx: Receiver<CommitSegmentRequest>,
}

impl ApiState {
    pub fn new(
        page_tx: Sender<WritePageRequest>,
        commit_rx: Receiver<CommitSegmentRequest>,
    ) -> Self {
        Self { page_tx, commit_rx }
    }

    pub async fn write_page(&self, req: WritePageRequest) {
        self.page_tx.send(req).await.unwrap();
    }
}
