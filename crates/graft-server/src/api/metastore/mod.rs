use std::sync::Arc;

use axum::routing::post;

use crate::volume::{catalog::VolumeCatalog, store::VolumeStore, updater::VolumeCatalogUpdater};

use super::routes::Routes;

mod commit;
mod pull_commits;
mod pull_graft;
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

pub fn metastore_routes() -> Routes<Arc<MetastoreApiState>> {
    vec![
        ("/metastore/v1/snapshot", post(snapshot::handler)),
        ("/metastore/v1/pull_graft", post(pull_graft::handler)),
        ("/metastore/v1/pull_commits", post(pull_commits::handler)),
        ("/metastore/v1/commit", post(commit::handler)),
    ]
}
