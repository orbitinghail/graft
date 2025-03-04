use graft_client::MetastoreClient;
use graft_core::{PageIdx, VolumeId, page::Page};
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver};

use axum::routing::post;

use crate::{
    limiter::Limiter,
    segment::{
        cache::Cache, loader::SegmentLoader, uploader::SegmentUploadMsg, writer::WritePagesMsg,
    },
    volume::{catalog::VolumeCatalog, updater::VolumeCatalogUpdater},
};

use super::routes::Routes;

mod read_pages;
mod write_pages;

pub struct PagestoreApiState<C> {
    page_tx: mpsc::Sender<WritePagesMsg>,
    catalog: VolumeCatalog,
    loader: SegmentLoader<C>,
    metastore: MetastoreClient,
    updater: VolumeCatalogUpdater,
    volume_write_limiter: Limiter<VolumeId>,
}

impl<C> PagestoreApiState<C> {
    pub fn new(
        page_tx: mpsc::Sender<WritePagesMsg>,
        catalog: VolumeCatalog,
        loader: SegmentLoader<C>,
        metastore: MetastoreClient,
        updater: VolumeCatalogUpdater,
        write_concurrency: usize,
    ) -> Self {
        Self {
            page_tx,
            catalog,
            loader,
            metastore,
            updater,
            volume_write_limiter: Limiter::new(write_concurrency),
        }
    }

    pub async fn write_pages(
        &self,
        vid: VolumeId,
        pages: Vec<(PageIdx, Page)>,
    ) -> Receiver<SegmentUploadMsg> {
        let (segment_tx, segment_rx) = tokio::sync::mpsc::channel(4);
        self.page_tx
            .send(WritePagesMsg::new(vid, pages, segment_tx))
            .await
            .unwrap();
        segment_rx
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
