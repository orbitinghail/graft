use std::{collections::HashSet, sync::Arc, time::Duration};

use culprit::{Culprit, Result};
use graft_core::VolumeId;
use job::Job;
use thiserror::Error;
use tokio::{
    select,
    sync::broadcast::{
        self,
        error::{RecvError, TryRecvError},
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tryiter::{TryIterator, TryIteratorExt};

use crate::{
    runtime::storage::snapshot::{SnapshotKind, SnapshotKindMask},
    ClientErr, ClientPair,
};

use super::storage::{volume::SyncDirection, Storage};

#[derive(Debug, Error)]
pub enum ShutdownErr {
    #[error("error while shutting down Sync task")]
    JoinError,

    #[error("timeout while waiting for Sync task to cleanly shutdown")]
    Timeout,
}

mod job;

pub struct SyncTaskHandle {
    token: CancellationToken,
    task: JoinHandle<()>,
}

impl SyncTaskHandle {
    pub async fn shutdown(self, timeout: Duration) -> Result<(), ShutdownErr> {
        self.token.cancel();

        // wait for either the task to complete or the timeout to elapse
        match tokio::time::timeout(timeout, self.task).await {
            Ok(Ok(())) => {
                log::debug!("sync task shutdown completed");
                Ok(())
            }
            Ok(Err(err)) => {
                log::error!("sync task shutdown error: {:?}", err);
                Err(Culprit::new_with_note(
                    ShutdownErr::JoinError,
                    format!("join error: {err}"),
                ))
            }
            Err(_) => {
                log::warn!("timeout waiting for sync task to shutdown");
                Err(Culprit::new(ShutdownErr::Timeout))
            }
        }
    }
}

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
    pub fn spawn(
        storage: Arc<Storage>,
        clients: ClientPair,
        refresh_interval: Duration,
    ) -> SyncTaskHandle {
        let ticker = tokio::time::interval(refresh_interval);
        let token = CancellationToken::new();
        let commits_rx = storage.subscribe_to_local_commits();
        let task = Self {
            storage,
            clients,
            ticker,
            commits_rx,
            token: token.clone(),
        };
        SyncTaskHandle { token, task: tokio::spawn(task.run()) }
    }

    pub async fn run(mut self) {
        loop {
            match self.run_inner().await {
                Ok(()) => {
                    log::trace!("sync task inner loop completed without error; shutting down");
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
                biased;

                _ = self.token.cancelled() => {
                    log::debug!("sync task received shutdown request");
                    break;
                }

                _ = self.ticker.tick() => {
                    self.handle_tick().await?;
                }

                vids = Self::changed_vids(&mut self.commits_rx) => {
                    self.handle_commit(vids).await?;
                }
            }
        }
        Ok(())
    }

    fn is_shutdown(&self) -> bool {
        self.token.is_cancelled()
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
        let jobs = self.jobs(SyncDirection::Both, None).await;
        for job in jobs.collect::<Result<Vec<Job>, _>>()? {
            job.run(&self.storage, &self.clients).await?;

            if self.is_shutdown() {
                log::debug!("shutdown detected during handle_tick");
                break;
            }
        }
        Ok(())
    }

    async fn handle_commit(&mut self, vids: Option<HashSet<VolumeId>>) -> Result<(), ClientErr> {
        log::debug!("handle_commit: {:?}", vids);
        let jobs = self.jobs(SyncDirection::Push, vids).await;
        for job in jobs.collect::<Result<Vec<Job>, _>>()? {
            job.run(&self.storage, &self.clients).await?;

            if self.is_shutdown() {
                log::debug!("shutdown detected during handle_commit");
                break;
            }
        }
        Ok(())
    }

    async fn jobs(
        &self,
        sync: SyncDirection,
        vids: Option<HashSet<VolumeId>>,
    ) -> impl TryIterator<Ok = Job, Err = Culprit<ClientErr>> + '_ {
        let kind_mask = SnapshotKindMask::default()
            .with(SnapshotKind::Local)
            .with(SnapshotKind::Sync)
            .with(SnapshotKind::Remote);
        self.storage
            .query_volumes(sync, kind_mask, vids)
            .map_err(|err| err.map_ctx(ClientErr::from))
            .try_filter_map(move |(vid, config, mut snapshots)| {
                let can_push = config.sync().matches(SyncDirection::Push);
                let can_pull = config.sync().matches(SyncDirection::Pull);
                let has_changed = snapshots.sync() != snapshots.local();
                if can_push && has_changed && sync.matches(SyncDirection::Push) {
                    // generate a push job if the volume is configured for push
                    // and has changed and we want to push
                    Ok(Some(Job::push(
                        vid,
                        snapshots.take_remote(),
                        snapshots.take_sync(),
                        snapshots.take_local().expect(
                            "local snapshot should never be missing if sync snapshot is present",
                        ),
                    )))
                } else if can_pull && sync.matches(SyncDirection::Pull) {
                    // generate a pull job if the volume is configured for pull
                    // and we want to pull
                    Ok(Some(Job::pull(vid, snapshots.take_remote())))
                } else {
                    Ok(None)
                }
            })
    }
}
