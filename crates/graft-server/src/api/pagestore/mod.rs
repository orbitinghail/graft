use graft_client::MetastoreClient;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

use axum::{
    routing::{get, post},
    Router,
};

use crate::{
    segment::{
        bus::{Bus, CommitSegmentReq, WritePageReq},
        cache::Cache,
        loader::SegmentLoader,
    },
    volume::{catalog::VolumeCatalog, updater::VolumeCatalogUpdater},
};

use super::health;

mod read_pages;
mod write_pages;

pub struct PagestoreApiState<C> {
    page_tx: mpsc::Sender<WritePageReq>,
    commit_bus: Bus<CommitSegmentReq>,
    catalog: VolumeCatalog,
    loader: SegmentLoader<C>,
    metastore: MetastoreClient,
    updater: VolumeCatalogUpdater,
}

impl<C> PagestoreApiState<C> {
    pub fn new(
        page_tx: mpsc::Sender<WritePageReq>,
        commit_bus: Bus<CommitSegmentReq>,
        catalog: VolumeCatalog,
        loader: SegmentLoader<C>,
        metastore: MetastoreClient,
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

    pub fn loader(&self) -> &SegmentLoader<C> {
        &self.loader
    }

    pub fn metastore_client(&self) -> &MetastoreClient {
        &self.metastore
    }

    pub fn updater(&self) -> &VolumeCatalogUpdater {
        &self.updater
    }
}

pub fn pagestore_router<C>() -> Router<Arc<PagestoreApiState<C>>>
where
    C: Cache + Sync + Send + 'static,
{
    Router::new()
        .route("/pagestore/v1/health", get(health::handler))
        .route("/pagestore/v1/read_pages", post(read_pages::handler))
        .route("/pagestore/v1/write_pages", post(write_pages::handler))
}
