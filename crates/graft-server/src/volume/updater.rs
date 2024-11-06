use std::ops::Range;

use foldhash::fast::RandomState;
use futures::TryStreamExt;
use graft_core::{lsn::LSN, VolumeId};
use graft_proto::common::v1::LsnRange;
use object_store::ObjectStore;

use crate::limiter::Limiter;

use super::{
    catalog::{VolumeCatalog, VolumeCatalogErr},
    commit::CommitMeta,
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

    /// Load the specified volume snapshot, updating the catalog if necessary.
    pub async fn snapshot<O: ObjectStore>(
        &self,
        store: &VolumeStore<O>,
        catalog: &VolumeCatalog,
        vid: &VolumeId,
        lsn: Option<LSN>,
    ) -> Result<Option<CommitMeta>, UpdateErr> {
        // if a specific lsn is requested and we have a snapshot for it, return it
        if let Some(lsn) = lsn {
            if let Some(snapshot) = catalog.snapshot(vid.clone(), lsn)? {
                return Ok(Some(snapshot));
            }
        }

        // otherwise we need to update the catalog
        self.update_catalog_from_store(store, catalog, vid, lsn)
            .await?;

        // return the requested snapshot or latest
        if let Some(lsn) = lsn {
            Ok(catalog.snapshot(vid.clone(), lsn)?)
        } else {
            Ok(catalog.latest_snapshot(vid)?)
        }
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
        let mut commits = store.replay_unordered(
            vid.clone(),
            LsnRange::from_bounds(&(catalog_lsn.unwrap_or_default()..)),
        );
        while let Some(commit) = commits.try_next().await? {
            batch.insert_commit(commit)?;
        }
        batch.commit()?;

        // return; dropping the permit and allowing other updates to proceed
        Ok(())
    }

    pub async fn update_catalog_from_store_in_range<O: ObjectStore>(
        &self,
        store: &VolumeStore<O>,
        catalog: &VolumeCatalog,
        vid: &VolumeId,
        lsns: &Range<LSN>,
    ) -> Result<(), UpdateErr> {
        // we can return early if the catalog already contains the requested LSNs
        if catalog.contains_range(vid, &lsns)? {
            return Ok(());
        }

        // acquire a permit to update the volume
        let _permit = self.limiter.acquire(vid).await;

        // check the catalog again in case another task has updated the volume
        if catalog.contains_range(vid, &lsns)? {
            return Ok(());
        }

        // update the catalog
        let mut batch = catalog.batch_insert();
        let mut commits = store.replay_unordered(vid.clone(), LsnRange::from_bounds(lsns));
        while let Some(commit) = commits.try_next().await? {
            batch.insert_commit(commit)?;
        }
        batch.commit()?;

        // return; dropping the permit and allowing other updates to proceed
        Ok(())
    }
}
