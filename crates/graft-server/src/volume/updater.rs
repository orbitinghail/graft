use std::{fmt::Debug, ops::RangeInclusive};

use culprit::{Culprit, ResultExt};
use futures::TryStreamExt;
use graft_client::MetastoreClient;
use graft_core::{VolumeId, lsn::LSN};
use tokio::task::spawn_blocking;
use tracing::{Instrument, Level, field};

use crate::limiter::{Limiter, Permit};

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
        if let Some(lsn) = lsn
            && let Some(snapshot) = catalog.snapshot(vid.clone(), lsn).or_into_ctx()?
        {
            return Ok(Some(snapshot));
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
        // read the latest lsn for the volume in the catalog
        let initial_lsn = catalog.latest_snapshot(vid).or_into_ctx()?.map(|s| s.lsn());

        // if catalog lsn >= lsn_at_least, then no update is needed
        if initial_lsn.is_some_and(|l1| min_lsn.is_some_and(|l2| l1 >= l2)) {
            tracing::trace!(latest_lsn=?initial_lsn, ?min_lsn, ?vid, "catalog is already up-to-date");
            return Ok(());
        }

        // acquire a permit to update the volume
        let permit = self.limiter.acquire(vid).await;

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
                latest_lsn=?catalog_lsn, ?min_lsn, ?vid,
                "reused concurrent catalog update from store"
            );
            precept::expect_reachable!(
                "reused concurrent catalog update from store",
                {
                    "latest_lsn": catalog_lsn,
                    "min_lsn": min_lsn,
                    "vid": vid,
                }
            );
            return Ok(());
        }

        // we only need to reply commits that happened after the last snapshot
        let start_lsn = catalog_lsn.and_then(|lsn| lsn.next()).unwrap_or(LSN::FIRST);
        let lsns = start_lsn..=LSN::LAST;

        // update the catalog from the store
        self.replay_commits_from_store(store, catalog, permit, vid, &lsns)
            .await
    }

    pub async fn update_catalog_from_store_in_range(
        &self,
        store: &VolumeStore,
        catalog: &VolumeCatalog,
        vid: &VolumeId,
        lsns: &RangeInclusive<LSN>,
    ) -> Result<(), Culprit<UpdateErr>> {
        // we can return early if the catalog already contains the requested LSNs
        if catalog.contains_range(vid, &lsns).or_into_ctx()? {
            tracing::trace!(?lsns, ?vid, "catalog is already up-to-date");
            return Ok(());
        }

        // acquire a permit to update the volume
        let permit = self.limiter.acquire(vid).await;

        // check the catalog again in case another task has concurrently retrieved the requested lsns
        if catalog.contains_range(vid, &lsns).or_into_ctx()? {
            tracing::debug!(
                ?lsns,
                ?vid,
                "reused concurrent catalog update from store in range"
            );
            return Ok(());
        }

        // update the catalog from the store
        self.replay_commits_from_store(store, catalog, permit, vid, lsns)
            .await
    }

    #[tracing::instrument(level = Level::DEBUG, skip_all, fields(vid, lsns, latest_lsn))]
    async fn replay_commits_from_store(
        &self,
        store: &VolumeStore,
        catalog: &VolumeCatalog,
        _permit: Permit<'_>,
        vid: &VolumeId,
        lsns: &RangeInclusive<LSN>,
    ) -> Result<(), Culprit<UpdateErr>> {
        let mut commits = store.replay_ordered(vid, &lsns);

        let mut latest_lsn = *lsns.start();
        if let Some(commit) = commits.try_next().await.or_into_ctx()? {
            // only create a batch if we have commits to replay
            let mut batch = catalog.batch_insert();
            latest_lsn = latest_lsn.max(commit.meta().lsn());
            batch.insert_commit(&commit).or_into_ctx()?;
            while let Some(commit) = commits.try_next().await.or_into_ctx()? {
                latest_lsn = latest_lsn.max(commit.meta().lsn());
                batch.insert_commit(&commit).or_into_ctx()?;
            }
            batch.commit().or_into_ctx()?;
        }

        tracing::Span::current().record("latest_lsn", u64::from(latest_lsn));

        Ok(())
    }

    pub async fn update_catalog_from_metastore(
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
            tracing::trace!(latest_lsn=?catalog_lsn, ?min_lsn, ?vid, "catalog is already up-to-date");
            return Ok(());
        }

        // acquire a permit to update the volume
        let _permit = self.limiter.acquire(vid).await;

        // check the catalog again in case another task has updated the volume
        // while we were waiting for a permit
        let catalog_lsn = catalog.latest_snapshot(vid).or_into_ctx()?.map(|s| s.lsn());
        if catalog_lsn >= Some(min_lsn) {
            tracing::debug!(latest_lsn=?catalog_lsn, ?min_lsn, ?vid, "reused concurrent catalog update from metastore");
            precept::expect_reachable!(
                "reused concurrent catalog update from metastore",
                {
                    "latest_lsn": catalog_lsn,
                    "min_lsn": min_lsn,
                    "vid": vid,
                }
            );
            return Ok(());
        }

        // we only need to reply commits that happened after the last snapshot
        let start_lsn = catalog_lsn.and_then(|lsn| lsn.next()).unwrap_or(LSN::FIRST);
        let lsns = start_lsn..;

        let span = tracing::debug_span!(
            "updating catalog from metastore",
            ?vid,
            ?lsns,
            latest_lsn = field::Empty,
        );

        // use an async block in order to leverage Future::instrument
        async move {
            // update the catalog from the client
            // TODO: switch this to an async client once one exists
            let commits = {
                let client = client.clone();
                let vid = vid.clone();
                spawn_blocking(move || client.pull_commits(&vid, lsns))
                    .await
                    .expect("spawn_blocking failed")
                    .or_into_ctx()?
            };

            if !commits.is_empty() {
                // only create a batch if we have commits to replay
                let mut batch = catalog.batch_insert();
                let mut latest_lsn = start_lsn;
                for commit in commits {
                    let snapshot = commit.snapshot.expect("missing snapshot");
                    let meta: CommitMeta = snapshot.try_into().expect("invalid snapshot");
                    latest_lsn = latest_lsn.max(meta.lsn());

                    batch
                        .insert_snapshot(vid.clone(), meta, commit.segments)
                        .or_into_ctx()?;
                }

                batch.commit().or_into_ctx()?;
                tracing::Span::current().record("latest_lsn", u64::from(latest_lsn));
            }

            // return; dropping the permit and allowing other updates to proceed
            Ok(())
        }
        .instrument(span)
        .await
    }
}
