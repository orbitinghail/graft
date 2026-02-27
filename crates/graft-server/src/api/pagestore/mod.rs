use graft_client::MetastoreClient;
use graft_core::VolumeId;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};

use axum::routing::post;

use crate::{
    limiter::Limiter,
    segment::{
        bus::{Bus, CommitSegmentReq, WritePageReq},
        cache::Cache,
        loader::SegmentLoader,
    },
    volume::{catalog::VolumeCatalog, updater::VolumeCatalogUpdater},
};

use super::routes::Routes;

mod read_pages;
mod write_pages;

pub struct PagestoreApiState<C> {
    page_tx: mpsc::Sender<WritePageReq>,
    commit_bus: Bus<CommitSegmentReq>,
    catalog: VolumeCatalog,
    loader: SegmentLoader<C>,
    metastore: MetastoreClient,
    updater: VolumeCatalogUpdater,
    volume_write_limiter: Limiter<VolumeId>,
}

impl<C> PagestoreApiState<C> {
    pub fn new(
        page_tx: mpsc::Sender<WritePageReq>,
        commit_bus: Bus<CommitSegmentReq>,
        catalog: VolumeCatalog,
        loader: SegmentLoader<C>,
        metastore: MetastoreClient,
        updater: VolumeCatalogUpdater,
        write_concurrency: usize,
    ) -> Self {
        Self {
            page_tx,
            commit_bus,
            catalog,
            loader,
            metastore,
            updater,
            volume_write_limiter: Limiter::new(write_concurrency),
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

    pub fn volume_write_limiter(&self) -> &Limiter<VolumeId> {
        &self.volume_write_limiter
    }
}

pub fn pagestore_routes<C>() -> Routes<Arc<PagestoreApiState<C>>>
where
    C: Cache + Sync + Send + 'static,
{
    vec![
        ("/pagestore/v1/read_pages", post(read_pages::handler)),
        ("/pagestore/v1/write_pages", post(write_pages::handler)),
    ]
}
