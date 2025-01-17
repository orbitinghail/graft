use std::{
    collections::HashSet,
    sync::Arc,
    thread::{self, sleep, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam::channel::{bounded, select_biased, Receiver, Sender};
use culprit::{Culprit, Result};
use graft_core::VolumeId;
use job::Job;
use thiserror::Error;
use tryiter::{TryIterator, TryIteratorExt};

use crate::{
    runtime::storage::snapshot::{SnapshotKind, SnapshotKindMask},
    ClientErr, ClientPair,
};

use super::storage::{changeset::SetSubscriber, volume_config::SyncDirection, Storage};

#[derive(Debug, Error)]
pub enum ShutdownErr {
    #[error("error while shutting down Sync task")]
    JoinError,

    #[error("timeout while waiting for Sync task to cleanly shutdown")]
    Timeout,
}

mod job;

pub struct SyncTaskHandle {
    handle: JoinHandle<()>,
    shutdown_signal: Sender<()>,
}

impl SyncTaskHandle {
    pub fn shutdown_timeout(self, timeout: Duration) -> Result<(), ShutdownErr> {
        self.shutdown(Instant::now() + timeout)
    }

    pub fn shutdown(self, deadline: Instant) -> Result<(), ShutdownErr> {
        if let Err(_) = self.shutdown_signal.send_deadline((), deadline) {
            log::warn!("timeout waiting for sync task to shutdown");
            return Err(Culprit::new(ShutdownErr::Timeout));
        }

        let (tx, rx) = bounded(0);
        std::thread::spawn(move || {
            tx.send(self.handle.join()).unwrap();
        });

        // wait for the thread to complete or the timeout to elapse
        match rx.recv_deadline(deadline) {
            Ok(Ok(())) => {
                log::debug!("sync task shutdown completed");
                Ok(())
            }
            Ok(Err(err)) => {
                log::error!("sync task shutdown error: {:?}", err);
                let msg = match err.downcast_ref::<&'static str>() {
                    Some(s) => *s,
                    None => match err.downcast_ref::<String>() {
                        Some(s) => &s[..],
                        None => "unknown panic",
                    },
                };
                Err(Culprit::new_with_note(
                    ShutdownErr::JoinError,
                    format!("sync task panic: {msg}"),
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
    refresh_interval: Duration,
    commits: SetSubscriber<VolumeId>,
    shutdown_signal: Receiver<()>,
}

impl SyncTask {
    pub fn spawn(
        storage: Arc<Storage>,
        clients: ClientPair,
        refresh_interval: Duration,
    ) -> SyncTaskHandle {
        let commits = storage.local_changeset().subscribe_all();
        let (shutdown_tx, shutdown_rx) = bounded(0);
        let task = Self {
            storage,
            clients,
            refresh_interval,
            commits,
            shutdown_signal: shutdown_rx,
        };
        SyncTaskHandle {
            handle: thread::spawn(|| task.run()),
            shutdown_signal: shutdown_tx,
        }
    }

    fn run(mut self) {
        loop {
            match self.run_inner() {
                Ok(()) => {
                    log::trace!("sync task inner loop completed without error; shutting down");
                    break;
                }
                Err(err) => {
                    log::error!("sync task error: {:?}", err);
                    log::trace!("sleeping for 1 second before restarting");
                    sleep(Duration::from_secs(1));
                }
            }
        }
    }

    fn run_inner(&mut self) -> Result<(), ClientErr> {
        loop {
            select_biased! {
                recv(self.shutdown_signal) -> _ => {
                    log::debug!("sync task received shutdown request");
                    break;
                }

                recv(self.commits.ready()) -> result => {
                    if let Err(err) = result {
                        panic!("commit subscriber error: {:?}", err);
                    }
                    let vids = self.commits.changed();
                    self.handle_commit(vids)?;
                }

                default(self.refresh_interval) => self.handle_tick()?,
            }
        }
        Ok(())
    }

    fn handle_tick(&mut self) -> Result<(), ClientErr> {
        log::debug!("handle_tick");
        let mut jobs = self.jobs(SyncDirection::Both, None);
        while let Some(job) = jobs.try_next()? {
            job.run(&self.storage, &self.clients)?;
        }
        Ok(())
    }

    fn handle_commit(&mut self, vids: HashSet<VolumeId>) -> Result<(), ClientErr> {
        log::debug!("handle_commit: {:?}", vids);
        let mut jobs = self.jobs(SyncDirection::Push, Some(vids));
        while let Some(job) = jobs.try_next()? {
            job.run(&self.storage, &self.clients)?;
        }
        Ok(())
    }

    fn jobs(
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
