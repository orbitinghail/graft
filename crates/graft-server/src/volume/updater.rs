use foldhash::fast::RandomState;
use graft_core::VolumeId;
use object_store::ObjectStore;

use crate::limiter::Limiter;

use super::{catalog::VolumeCatalog, store::VolumeStore};

pub struct VolumeCatalogUpdater {
    limiter: Limiter<VolumeId, RandomState>,
}

impl VolumeCatalogUpdater {
    pub fn new(concurrency_limit: usize) -> Self {
        Self { limiter: Limiter::new(concurrency_limit) }
    }

    pub async fn update_catalog_from_store<O: ObjectStore>(
        &self,
        store: &VolumeStore<O>,
        catalog: &VolumeCatalog,
    ) -> std::io::Result<()> {
        todo!()
    }
}
