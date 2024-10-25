use tokio::sync::{broadcast, mpsc};

use crate::{
    segment::{
        bus::{Bus, CommitSegmentReq, WritePageReq},
        loader::Loader,
    },
    volume::catalog::VolumeCatalog,
};

pub struct ApiState<O, C> {
    page_tx: mpsc::Sender<WritePageReq>,
    commit_bus: Bus<CommitSegmentReq>,
    catalog: VolumeCatalog,
    loader: Loader<O, C>,
}

impl<O, C> ApiState<O, C> {
    pub fn new(
        page_tx: mpsc::Sender<WritePageReq>,
        commit_bus: Bus<CommitSegmentReq>,
        catalog: VolumeCatalog,
        loader: Loader<O, C>,
    ) -> Self {
        Self { page_tx, commit_bus, catalog, loader }
    }

    pub async fn write_page(&self, req: WritePageReq) {
        self.page_tx.send(req).await.unwrap();
    }

    pub fn subscribe_commits(&self) -> broadcast::Receiver<CommitSegmentReq> {
        self.commit_bus.subscribe()
    }

    pub fn catalog(&self) -> &VolumeCatalog {
        &self.catalog
    }

    pub fn loader(&self) -> &Loader<O, C> {
        &self.loader
    }
}
