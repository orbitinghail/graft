use graft_client::MetaStoreClient;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

use axum::{routing::post, Router};
use object_store::ObjectStore;

use crate::{
    segment::{
        bus::{Bus, CommitSegmentReq, WritePageReq},
        cache::Cache,
        loader::SegmentLoader,
    },
    volume::{catalog::VolumeCatalog, updater::VolumeCatalogUpdater},
};

mod read_pages;
mod write_pages;

pub struct PagestoreApiState<O, C> {
    page_tx: mpsc::Sender<WritePageReq>,
    commit_bus: Bus<CommitSegmentReq>,
    catalog: VolumeCatalog,
    loader: SegmentLoader<O, C>,
    metastore: MetaStoreClient,
    updater: VolumeCatalogUpdater,
}

impl<O, C> PagestoreApiState<O, C> {
    pub fn new(
        page_tx: mpsc::Sender<WritePageReq>,
        commit_bus: Bus<CommitSegmentReq>,
        catalog: VolumeCatalog,
        loader: SegmentLoader<O, C>,
        metastore: MetaStoreClient,
        updater: VolumeCatalogUpdater,
    ) -> Self {
        Self {
            page_tx,
            commit_bus,
            catalog,
            loader,
            metastore,
            updater,
        }
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

    pub fn loader(&self) -> &SegmentLoader<O, C> {
        &self.loader
    }

    pub fn metastore_client(&self) -> &MetaStoreClient {
        &self.metastore
    }

    pub fn updater(&self) -> &VolumeCatalogUpdater {
        &self.updater
    }
}

pub fn pagestore_router<O, C>() -> Router<Arc<PagestoreApiState<O, C>>>
where
    O: ObjectStore + Sync + Send + 'static,
    C: Cache + Sync + Send + 'static,
{
    Router::new()
        .route("/pagestore/v1/read_pages", post(read_pages::handler))
        .route("/pagestore/v1/write_pages", post(write_pages::handler))
}
