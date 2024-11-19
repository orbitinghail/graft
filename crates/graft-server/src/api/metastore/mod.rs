use std::sync::Arc;

use axum::{
    routing::{get, post},
    Router,
};

use crate::volume::{catalog::VolumeCatalog, store::VolumeStore, updater::VolumeCatalogUpdater};

mod commit;
mod health;
mod pull_commits;
mod pull_offsets;
mod snapshot;

pub struct MetastoreApiState {
    store: Arc<VolumeStore>,
    catalog: VolumeCatalog,
    updater: VolumeCatalogUpdater,
}

impl MetastoreApiState {
    pub fn new(
        store: Arc<VolumeStore>,
        catalog: VolumeCatalog,
        updater: VolumeCatalogUpdater,
    ) -> Self {
        Self { store, catalog, updater }
    }

    pub fn store(&self) -> &VolumeStore {
        &self.store
    }

    pub fn catalog(&self) -> &VolumeCatalog {
        &self.catalog
    }

    pub fn updater(&self) -> &VolumeCatalogUpdater {
        &self.updater
    }
}

pub fn metastore_router() -> Router<Arc<MetastoreApiState>> {
    Router::new()
        .route("/metastore/v1/health", get(health::handler))
        .route("/metastore/v1/snapshot", post(snapshot::handler))
        .route("/metastore/v1/pull_offsets", post(pull_offsets::handler))
        .route("/metastore/v1/pull_commits", post(pull_commits::handler))
        .route("/metastore/v1/commit", post(commit::handler))
}
