use std::ops::RangeBounds;

use culprit::{Culprit, ResultExt};
use futures::TryStreamExt;
use graft_client::MetastoreClient;
use graft_core::{lsn::LSN, VolumeId};
use tokio::task::spawn_blocking;

use crate::limiter::Limiter;

use super::{
    catalog::{VolumeCatalog, VolumeCatalogErr},
    commit::CommitMeta,
    store::{VolumeStore, VolumeStoreErr},
};

#[derive(Debug, thiserror::Error)]
pub enum UpdateErr {
    #[error("volume catalog error")]
    CatalogErr(#[from] VolumeCatalogErr),

    #[error("volume store error")]
    StoreErr(#[from] VolumeStoreErr),

    #[error("client error")]
    ClientErr(#[from] graft_client::ClientErr),
}

pub struct VolumeCatalogUpdater {
    limiter: Limiter<VolumeId>,
}

impl VolumeCatalogUpdater {
    pub fn new(concurrency_limit: usize) -> Self {
        Self { limiter: Limiter::new(concurrency_limit) }
    }

    /// Load the specified volume snapshot, updating the catalog if necessary.
    pub async fn snapshot(
        &self,
        store: &VolumeStore,
        catalog: &VolumeCatalog,
        vid: &VolumeId,
        lsn: Option<LSN>,
    ) -> Result<Option<CommitMeta>, Culprit<UpdateErr>> {
        // if a specific lsn is requested and we have a snapshot for it, return it
        if let Some(lsn) = lsn {
            if let Some(snapshot) = catalog.snapshot(vid.clone(), lsn).or_into_ctx()? {
                return Ok(Some(snapshot));
            }
        }

        // otherwise we need to update the catalog
        self.update_catalog_from_store(store, catalog, vid, lsn)
            .await?;

        // return the requested snapshot or latest
        if let Some(lsn) = lsn {
            Ok(catalog.snapshot(vid.clone(), lsn).or_into_ctx()?)
        } else {
            Ok(catalog.latest_snapshot(vid).or_into_ctx()?)
        }
    }

    pub async fn update_catalog_from_store(
        &self,
        store: &VolumeStore,
        catalog: &VolumeCatalog,
        vid: &VolumeId,
        min_lsn: Option<LSN>,
    ) -> Result<(), Culprit<UpdateErr>> {
        tracing::debug!(
            ?min_lsn,
            "updating catalog for volume {vid:?} from object store"
        );

        // read the latest lsn for the volume in the catalog
        let initial_lsn = catalog.latest_snapshot(vid).or_into_ctx()?.map(|s| s.lsn());

        // if catalog lsn >= lsn_at_least, then no update is needed
        if initial_lsn.is_some_and(|l1| min_lsn.is_some_and(|l2| l1 >= l2)) {
            tracing::debug!(?initial_lsn, "catalog for volume {vid:?} is up-to-date");
            return Ok(());
        }

        // acquire a permit to update the volume
        let _permit = self.limiter.acquire(vid).await;

        // check the catalog again in case another task has updated the volume
        let catalog_lsn = catalog.latest_snapshot(vid).or_into_ctx()?.map(|s| s.lsn());

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
            tracing::debug!(
                ?catalog_lsn,
                ?min_lsn,
                "catalog for volume {vid:?} is up-to-date"
            );
            return Ok(());
        }

        // we only need to reply commits that happened after the last snapshot
        let start_lsn = catalog_lsn.and_then(|lsn| lsn.next()).unwrap_or(LSN::FIRST);
        let lsns = start_lsn..;

        // update the catalog from the store
        let mut commits = store.replay_unordered(vid.clone(), &lsns);

        // only create a transaction if we have commits to replay
        if let Some(commit) = commits.try_next().await.or_into_ctx()? {
            let mut batch = catalog.batch_insert();
            batch.insert_commit(commit).or_into_ctx()?;
            while let Some(commit) = commits.try_next().await.or_into_ctx()? {
                batch.insert_commit(commit).or_into_ctx()?;
            }
            batch.commit().or_into_ctx()?;
        }

        // return; dropping the permit and allowing other updates to proceed
        Ok(())
    }

    pub async fn update_catalog_from_store_in_range<R: RangeBounds<LSN>>(
        &self,
        store: &VolumeStore,
        catalog: &VolumeCatalog,
        vid: &VolumeId,
        lsns: &R,
    ) -> Result<(), Culprit<UpdateErr>> {
        // we can return early if the catalog already contains the requested LSNs
        if catalog.contains_range(vid, lsns).or_into_ctx()? {
            return Ok(());
        }

        // acquire a permit to update the volume
        let _permit = self.limiter.acquire(vid).await;

        // check the catalog again in case another task has updated the volume
        if catalog.contains_range(vid, lsns).or_into_ctx()? {
            return Ok(());
        }

        // update the catalog
        let mut batch = catalog.batch_insert();
        let mut commits = store.replay_unordered(vid.clone(), lsns);
        while let Some(commit) = commits.try_next().await.or_into_ctx()? {
            batch.insert_commit(commit).or_into_ctx()?;
        }
        batch.commit().or_into_ctx()?;

        // return; dropping the permit and allowing other updates to proceed
        Ok(())
    }

    pub async fn update_catalog_from_client(
        &self,
        client: &MetastoreClient,
        catalog: &VolumeCatalog,
        vid: &VolumeId,
        min_lsn: LSN,
    ) -> Result<(), Culprit<UpdateErr>> {
        // read the latest lsn for the volume in the catalog
        let catalog_lsn = catalog.latest_snapshot(vid).or_into_ctx()?.map(|s| s.lsn());

        // if catalog lsn >= min_lsn, then no update is needed
        if catalog_lsn >= Some(min_lsn) {
            return Ok(());
        }

        // acquire a permit to update the volume
        let _permit = self.limiter.acquire(vid).await;

        // check the catalog again in case another task has updated the volume
        // while we were waiting for a permit
        let catalog_lsn = catalog.latest_snapshot(vid).or_into_ctx()?.map(|s| s.lsn());
        if catalog_lsn >= Some(min_lsn) {
            return Ok(());
        }

        // we only need to reply commits that happened after the last snapshot
        let start_lsn = catalog_lsn.and_then(|lsn| lsn.next()).unwrap_or(LSN::FIRST);

        tracing::trace!(
            ?min_lsn,
            ?catalog_lsn,
            ?start_lsn,
            "updating catalog for volume {vid:?} from metastore"
        );

        // update the catalog from the client
        // TODO: switch this to an async client once one exists
        let commits = {
            let client = client.clone();
            let vid = vid.clone();
            spawn_blocking(move || client.pull_commits(&vid, start_lsn..))
                .await
                .expect("spawn_blocking failed")
                .or_into_ctx()?
        };

        let mut batch = catalog.batch_insert();
        for commit in commits {
            let snapshot = commit.snapshot.expect("missing snapshot");
            let meta: CommitMeta = snapshot.try_into().expect("invalid snapshot");

            batch
                .insert_snapshot(vid.clone(), meta, commit.segments)
                .or_into_ctx()?;
        }

        batch.commit().or_into_ctx()?;

        // return; dropping the permit and allowing other updates to proceed
        Ok(())
    }
}
