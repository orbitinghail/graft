use std::{
    collections::HashSet,
    thread::{self, sleep, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam::channel::{bounded, select_biased, Receiver, Sender, TrySendError};
use culprit::{Culprit, Result, ResultExt};
use graft_core::VolumeId;
use job::Job;
use thiserror::Error;
use tryiter::{TryIterator, TryIteratorExt};

use crate::{ClientErr, ClientPair};

use super::{
    fetcher::Fetcher,
    shared::Shared,
    storage::{changeset::SetSubscriber, volume_state::SyncDirection},
};

#[derive(Debug, Error)]
pub enum ShutdownErr {
    #[error("error while shutting down Sync task")]
    JoinError,

    #[error("timeout while waiting for Sync task to cleanly shutdown")]
    Timeout,
}

#[derive(Debug)]
pub struct SyncControl {
    vid: VolumeId,
    direction: SyncDirection,
    complete: Sender<Result<(), ClientErr>>,
}

impl SyncControl {
    pub fn new(
        vid: VolumeId,
        direction: SyncDirection,
        complete: Sender<Result<(), ClientErr>>,
    ) -> Self {
        Self { vid, direction, complete }
    }
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
pub struct SyncTask<F> {
    shared: Shared<F>,
    clients: ClientPair,
    refresh_interval: Duration,
    commits: SetSubscriber<VolumeId>,
    control: Receiver<SyncControl>,
    shutdown_signal: Receiver<()>,
}

impl<F: Fetcher> SyncTask<F> {
    pub fn spawn(
        shared: Shared<F>,
        clients: ClientPair,
        refresh_interval: Duration,
        control: Receiver<SyncControl>,
    ) -> SyncTaskHandle {
        let commits = shared.storage().local_changeset().subscribe_all();
        let (shutdown_tx, shutdown_rx) = bounded(0);
        let task = Self {
            shared,
            clients,
            refresh_interval,
            commits,
            control,
            shutdown_signal: shutdown_rx,
        };
        SyncTaskHandle {
            handle: thread::spawn(move || task.run()),
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

                recv(self.control) -> control => {
                    let control = control.expect("sync task control channel closed");
                    self.handle_control(control)?;
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

    fn handle_control(&mut self, msg: SyncControl) -> Result<(), ClientErr> {
        let result = self.sync(msg.vid, msg.direction);
        // we try to send the error to the client first and then fallback to
        // reporting the error in the sync thread if the channel is disconnected
        if let Err(err) = msg.complete.try_send(result) {
            match err {
                TrySendError::Full(err) => {
                    panic!("SyncControl completion channel should never be full! {err:?}",)
                }
                TrySendError::Disconnected(err) => return err,
            }
        }
        Ok(())
    }

    /// Synchronously sync a volume with the remote
    fn sync(&mut self, vid: VolumeId, dir: SyncDirection) -> Result<(), ClientErr> {
        let state = self.shared.storage().volume_state(&vid).or_into_ctx()?;

        if dir.matches(SyncDirection::Pull) {
            Job::pull(state.clone())
                .run(self.shared.storage(), &self.clients)
                .or_into_culprit("error while pulling volume")?;
        }

        if dir.matches(SyncDirection::Push) {
            Job::push(state)
                .run(self.shared.storage(), &self.clients)
                .or_into_culprit("error while pushing volume")?;
        }

        Ok(())
    }

    fn handle_tick(&mut self) -> Result<(), ClientErr> {
        log::debug!("handle_tick");
        let mut jobs = self.jobs(SyncDirection::Both, None);
        while let Some(job) = jobs.try_next()? {
            job.run(self.shared.storage(), &self.clients)?;
        }
        Ok(())
    }

    fn handle_commit(&mut self, vids: HashSet<VolumeId>) -> Result<(), ClientErr> {
        log::debug!("handle_commit: {:?}", vids);
        let mut jobs = self.jobs(SyncDirection::Push, Some(vids));
        while let Some(job) = jobs.try_next()? {
            job.run(self.shared.storage(), &self.clients)?;
        }
        Ok(())
    }

    fn jobs(
        &self,
        sync: SyncDirection,
        vids: Option<HashSet<VolumeId>>,
    ) -> impl TryIterator<Ok = Job, Err = Culprit<ClientErr>> + '_ {
        self.shared
            .storage()
            .query_volumes(sync, vids)
            .map_err(|err| err.map_ctx(ClientErr::from))
            .try_filter_map(move |state| {
                let config = state.config();
                let can_push = config.sync().matches(SyncDirection::Push);
                let can_pull = config.sync().matches(SyncDirection::Pull);
                let has_pending_commits = state.has_pending_commits();
                if can_push && has_pending_commits && sync.matches(SyncDirection::Push) {
                    Ok(Some(Job::push(state)))
                } else if can_pull && sync.matches(SyncDirection::Pull) {
                    Ok(Some(Job::pull(state)))
                } else {
                    Ok(None)
                }
            })
    }
}
