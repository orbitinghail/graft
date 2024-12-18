use std::{collections::HashSet, sync::Arc, time::Duration};

use graft_core::VolumeId;
use job::{Job, PullJob, PushJob};
use tokio::{
    select,
    sync::broadcast::{
        self,
        error::{RecvError, TryRecvError},
    },
};
use tokio_util::sync::CancellationToken;
use tryiter::{TryIterator, TryIteratorExt};

use crate::{
    runtime::storage::snapshot::{SnapshotKind, SnapshotKindMask},
    ClientErr, ClientPair,
};

use super::storage::{volume::SyncDirection, Storage};

mod job;

/// A SyncTask is a background task which continuously syncs volumes to and from
/// a Graft service.
pub struct SyncTask {
    storage: Arc<Storage>,
    clients: ClientPair,
    ticker: tokio::time::Interval,
    commits_rx: broadcast::Receiver<VolumeId>,
    token: CancellationToken,
}

impl SyncTask {
    pub fn new(
        storage: Arc<Storage>,
        clients: ClientPair,
        refresh_interval: Duration,
    ) -> (CancellationToken, Self) {
        let ticker = tokio::time::interval(refresh_interval);
        let token = CancellationToken::new();
        let commits_rx = storage.subscribe_to_local_commits();
        (
            token.clone(),
            Self {
                storage,
                clients,
                ticker,
                commits_rx,
                token,
            },
        )
    }

    pub async fn run(mut self) {
        loop {
            match self.run_inner().await {
                Ok(()) => {
                    log::info!("sync task completed");
                    break;
                }
                Err(err) => {
                    log::error!("sync task error: {:?}", err);
                }
            }
        }
    }

    async fn run_inner(&mut self) -> Result<(), ClientErr> {
        loop {
            select! {
                _ = self.ticker.tick() => {
                    self.handle_tick().await?;
                }
                vids = Self::changed_vids(&mut self.commits_rx) => {
                    self.handle_commit(vids).await?;

                }
                _ = self.token.cancelled() => {
                    log::info!("sync task shutting down");
                    break;
                }
            }
        }
        Ok(())
    }

    /// Yields a set of recent vids that have been committed to, or None if
    /// the receiver lags. Panics if the channel is closed.
    async fn changed_vids(chan: &mut broadcast::Receiver<VolumeId>) -> Option<HashSet<VolumeId>> {
        // wait for the first commit; returning early if the channel lags
        let first_vid = match chan.recv().await {
            Ok(vid) => vid,
            Err(RecvError::Closed) => panic!("commits channel closed"),
            Err(RecvError::Lagged(_)) => {
                log::warn!("commits channel lagging");
                chan.resubscribe();
                return None;
            }
        };

        // optimistically drain the rest of the channel into a set; returning early if the channel lags
        let mut set = HashSet::new();
        set.insert(first_vid);
        loop {
            match chan.try_recv() {
                Ok(vid) => {
                    set.insert(vid);
                }
                Err(TryRecvError::Empty) => {
                    break;
                }
                Err(TryRecvError::Lagged(_)) => {
                    log::warn!("commits channel lagging");
                    chan.resubscribe();
                    return None;
                }
                Err(TryRecvError::Closed) => panic!("commits channel closed"),
            }
        }

        Some(set)
    }

    async fn handle_tick(&mut self) -> Result<(), ClientErr> {
        log::debug!("handle_tick");
        let mut jobs = self.jobs(SyncDirection::Both, None).await;
        while let Some(job) = jobs.try_next()? {
            job.run(&self.storage, &self.clients).await?;
        }
        Ok(())
    }

    async fn handle_commit(&mut self, vids: Option<HashSet<VolumeId>>) -> Result<(), ClientErr> {
        log::debug!("handle_commit: {:?}", vids);
        let mut jobs = self.jobs(SyncDirection::Push, vids).await;
        while let Some(job) = jobs.try_next()? {
            job.run(&self.storage, &self.clients).await?;
        }
        Ok(())
    }

    async fn jobs(
        &self,
        sync: SyncDirection,
        vids: Option<HashSet<VolumeId>>,
    ) -> impl TryIterator<Ok = Job, Err = ClientErr> + '_ {
        let kind_mask = SnapshotKindMask::default()
            .with(SnapshotKind::Local)
            .with(SnapshotKind::Sync)
            .with(SnapshotKind::Remote);
        self.storage
            .query_volumes(sync, kind_mask, vids)
            .err_into()
            .try_filter_map(|(vid, config, mut snapshots)| {
                // generate a push job if the volume is configured for push and has changed
                let can_push = config.sync().matches(SyncDirection::Push);
                let can_pull = config.sync().matches(SyncDirection::Pull);
                let has_changed = snapshots.sync() != snapshots.local();
                if can_push && has_changed {
                    Ok(Some(Job::push(
                        vid,
                        snapshots.take_sync(),
                        snapshots.take_local().expect(
                            "local snapshot should never be missing if sync snapshot is present",
                        ),
                    )))
                } else if can_pull {
                    Ok(Some(Job::pull(vid, snapshots.take_remote())))
                } else {
                    Ok(None)
                }
            })
    }
}
