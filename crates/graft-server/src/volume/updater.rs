use foldhash::fast::RandomState;
use futures::TryStreamExt;
use graft_core::{lsn::LSN, VolumeId};
use object_store::ObjectStore;

use crate::limiter::Limiter;

use super::{
    catalog::{VolumeCatalog, VolumeCatalogErr},
    store::{VolumeStore, VolumeStoreErr},
};

#[derive(Debug, thiserror::Error)]
pub enum UpdateErr {
    #[error(transparent)]
    CatalogErr(#[from] VolumeCatalogErr),

    #[error(transparent)]
    StoreErr(#[from] VolumeStoreErr),
}

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
        vid: &VolumeId,
        min_lsn: Option<LSN>,
    ) -> Result<(), UpdateErr> {
        // read the latest lsn for the volume in the catalog
        let initial_lsn = catalog.latest_snapshot(vid)?.map(|s| s.lsn());

        // if catalog lsn >= lsn_at_least, then no update is needed
        if initial_lsn.is_some_and(|l1| min_lsn.is_some_and(|l2| l1 >= l2)) {
            return Ok(());
        }

        // acquire a permit to update the volume
        let _permit = self.limiter.acquire(vid).await;

        // check the catalog again in case another task has updated the volume
        let catalog_lsn = catalog.latest_snapshot(vid)?.map(|s| s.lsn());

        // check to see if we can exit early
        if match (catalog_lsn, min_lsn) {
            (None, None) => false,
            (None, Some(_)) => false,
            // another task may have updated the volume concurrently; since we
            // don't have a minimum lsn to acquire we can just use the other
            // task's update
            (Some(catalog_lsn), None) => initial_lsn != Some(catalog_lsn),
            // another task may have updated the volume concurrently; since we
            // have a minimum lsn to acquire we can only use the other task's
            // update if it meets the minimum lsn requirement
            (Some(catalog_lsn), Some(min_lsn)) => catalog_lsn >= min_lsn,
        } {
            return Ok(());
        }

        // update the catalog
        let mut batch = catalog.batch_insert();
        let mut commits = store.replay_unordered(vid.clone(), catalog_lsn);
        while let Some(commit) = commits.try_next().await? {
            batch.insert_commit(commit)?;
        }
        batch.commit()?;

        // return; dropping the permit and allowing other updates to proceed
        Ok(())
    }
}
