use std::{
    collections::HashSet,
    fmt::Debug,
    sync::Arc,
    thread::{self, sleep, JoinHandle},
    time::{Duration, Instant},
};

use control::{SyncControl, SyncRpc};
use crossbeam::channel::{bounded, select_biased, Receiver, Sender, TrySendError};
use culprit::{Culprit, Result, ResultExt};
use graft_core::VolumeId;
use job::Job;
use parking_lot::RwLock;
use thiserror::Error;
use tryiter::{TryIterator, TryIteratorExt};

use crate::{ClientErr, ClientPair};

use super::{
    fetcher::Fetcher,
    shared::Shared,
    storage::{
        changeset::SetSubscriber,
        volume_state::{SyncDirection, VolumeStatus},
        StorageErr,
    },
};

pub mod control;
mod job;

#[derive(Debug, Error)]
pub enum StartupErr {
    #[error("the Sync task is already running")]
    AlreadyRunning,
}

#[derive(Debug, Error)]
pub enum ShutdownErr {
    #[error("error while shutting down Sync task")]
    JoinError,

    #[error("timeout while waiting for Sync task to cleanly shutdown")]
    Timeout,

    #[error("the Sync task is not running")]
    TaskNotRunning,
}

#[derive(Clone, Default)]
pub struct SyncTaskHandle {
    inner: Arc<RwLock<Option<SyncTaskHandleInner>>>,
}

struct SyncTaskHandleInner {
    handle: JoinHandle<()>,
    control: Sender<SyncControl>,
}

impl SyncTaskHandle {
    pub fn rpc(&self) -> SyncRpc {
        let control = self
            .inner
            .read()
            .as_ref()
            .map(|inner| inner.control.clone());
        SyncRpc::new(control)
    }

    pub fn spawn<F: Fetcher>(
        &self,
        shared: Shared<F>,
        clients: ClientPair,
        refresh_interval: Duration,
        control_channel_size: usize,
        autosync: bool,
    ) -> Result<(), StartupErr> {
        let mut inner = self.inner.write();
        if inner.is_some() {
            return Err(Culprit::new(StartupErr::AlreadyRunning));
        }

        let (control_tx, control_rx) = bounded(control_channel_size);
        let commits = shared.storage().local_changeset().subscribe_all();

        let task = SyncTask {
            shared,
            clients,
            refresh_interval,
            commits,
            control: control_rx,
            autosync,
        };

        let handle = thread::Builder::new()
            .name("graft-sync".into())
            .spawn(move || task.run())
            .expect("failed to spawn sync task");

        inner.replace(SyncTaskHandleInner { handle, control: control_tx });
        Ok(())
    }

    pub fn shutdown_timeout(&self, timeout: Duration) -> Result<(), ShutdownErr> {
        self.shutdown(Instant::now() + timeout)
    }

    pub fn shutdown(&self, deadline: Instant) -> Result<(), ShutdownErr> {
        if let Some(inner) = self.inner.write().take() {
            // drop the control channel to trigger shutdown
            if let Err(_) = inner.control.send_deadline(SyncControl::Shutdown, deadline) {
                return Err(Culprit::new_with_note(
                    ShutdownErr::Timeout,
                    "timeout while waiting to send Shutdown message to sync task",
                ));
            }

            let (tx, rx) = bounded(0);
            std::thread::spawn(move || {
                tx.send(inner.handle.join()).unwrap();
            });

            // wait for the thread to complete or the timeout to elapse
            match rx.recv_deadline(deadline) {
                Ok(Ok(())) => {
                    tracing::trace!("sync task shutdown completed");
                    Ok(())
                }
                Ok(Err(err)) => {
                    tracing::error!(?err, "sync task shutdown error");
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
                    tracing::warn!("timeout waiting for sync task to shutdown");
                    Err(Culprit::new(ShutdownErr::Timeout))
                }
            }
        } else {
            Err(Culprit::new(ShutdownErr::TaskNotRunning))
        }
    }
}

#[derive(Debug, Error)]
pub enum SyncTaskErr {
    #[error("client error: {0}")]
    Client(#[from] ClientErr),

    #[error("completion channel disconnected")]
    CompletionChannelDisconnected,
}

impl From<StorageErr> for SyncTaskErr {
    fn from(err: StorageErr) -> Self {
        Self::Client(err.into())
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

    /// when autosync is true, volumes will be automatically pushed and pulled
    /// to the server when they change or every `refresh_interval`.
    autosync: bool,
}

impl<F: Fetcher> SyncTask<F> {
    fn run(mut self) {
        loop {
            match self.run_inner() {
                Ok(()) => {
                    tracing::trace!("sync task inner loop completed without error; shutting down");
                    break;
                }
                Err(err) => {
                    tracing::error!("sync task error: {:?}", err);
                    // we want to explore system states that include sync task errors
                    precept::expect_reachable!("error occurred in sync task");
                    sleep(Duration::from_secs(1));
                }
            }
        }
    }

    fn run_inner(&mut self) -> Result<(), SyncTaskErr> {
        loop {
            select_biased! {
                recv(self.control) -> control => {
                    match control.ok() {
                        None| Some(SyncControl::Shutdown) => {
                            tracing::trace!("sync task shutting down");
                            break
                        }
                        Some(control) => self.handle_control(control)?,
                    }
                }

                recv(self.commits.ready()) -> result => {
                    if let Err(err) = result {
                        panic!("commit subscriber error: {:?}", err);
                    }
                    let vids = self.commits.changed();
                    if !vids.is_empty() {
                        self.handle_commit(vids)?;
                    }
                }

                default(self.refresh_interval) => self.handle_tick()?,
            }
        }
        Ok(())
    }

    fn handle_control(&mut self, msg: SyncControl) -> Result<(), SyncTaskErr> {
        macro_rules! reply {
            ($complete:ident, $result:expr) => {
                match $complete.try_send($result) {
                    Ok(()) => Ok(()),
                    Err(TrySendError::Full(_)) => {
                        unreachable!("SyncControl completion channel should never be full")
                    }
                    Err(TrySendError::Disconnected(err)) => Err(Culprit::new_with_note(
                        SyncTaskErr::CompletionChannelDisconnected,
                        format!("SyncControl completion channel disconnected: {err:?}"),
                    )),
                }
            };
        }

        match msg {
            SyncControl::GetAutosync { complete } => reply!(complete, self.autosync),
            SyncControl::SetAutosync { autosync, complete } => {
                self.autosync = autosync;
                reply!(complete, ())
            }
            SyncControl::Sync { vid, direction, complete } => {
                reply!(complete, self.sync_volume(vid, direction))
            }
            SyncControl::ResetToRemote { vid, complete } => {
                reply!(complete, self.reset_volume_to_remote(vid))
            }
            SyncControl::Shutdown => {
                unreachable!("shutdown message is handled in sync task select loop")
            }
        }
    }

    /// Synchronously sync a volume with the remote
    /// If dir is SyncDirection::Both, this function will push before it pulls
    fn sync_volume(&mut self, vid: VolumeId, dir: SyncDirection) -> Result<(), ClientErr> {
        if dir.matches(SyncDirection::Push) {
            let state = self.shared.storage().volume_state(&vid).or_into_ctx()?;
            if state.has_pending_commits() {
                Job::push(vid.clone(), self.shared.cid().clone())
                    .run(self.shared.storage(), &self.clients)
                    .or_into_culprit("error while pushing volume")?;
            }
        }

        if dir.matches(SyncDirection::Pull) {
            Job::pull(vid)
                .run(self.shared.storage(), &self.clients)
                .or_into_culprit("error while pulling volume")?;
        }

        Ok(())
    }

    /// Reset the volume to the remote. This will cause all pending commits to
    /// be rolled back and the volume status to be cleared.
    fn reset_volume_to_remote(&mut self, vid: VolumeId) -> Result<(), ClientErr> {
        Job::pull_and_reset(vid)
            .run(self.shared.storage(), &self.clients)
            .or_into_culprit("error while resetting volume to the remote")
    }

    fn handle_tick(&mut self) -> Result<(), SyncTaskErr> {
        if !self.autosync {
            return Ok(());
        }

        let mut jobs = self.jobs(SyncDirection::Both, None);
        while let Some(job) = jobs.try_next()? {
            job.run(self.shared.storage(), &self.clients)
                .or_into_ctx()?;
        }
        Ok(())
    }

    fn handle_commit(&mut self, vids: HashSet<VolumeId>) -> Result<(), SyncTaskErr> {
        if !self.autosync {
            return Ok(());
        }

        let mut jobs = self.jobs(SyncDirection::Push, Some(vids));
        while let Some(job) = jobs.try_next()? {
            job.run(self.shared.storage(), &self.clients)
                .or_into_ctx()?;
        }
        Ok(())
    }

    fn jobs(
        &self,
        sync: SyncDirection,
        vids: Option<HashSet<VolumeId>>,
    ) -> impl TryIterator<Ok = Job, Err = Culprit<SyncTaskErr>> + '_ {
        self.shared
            .storage()
            .query_volumes(sync, vids)
            .map_err(|err| err.map_ctx(SyncTaskErr::from))
            .try_filter_map(move |state| {
                if state.status() != VolumeStatus::Ok {
                    // volume must be healthy
                    return Ok(None);
                }

                let config = state.config();
                let can_push = config.sync().matches(SyncDirection::Push);
                let can_pull = config.sync().matches(SyncDirection::Pull);
                let has_pending_commits = state.has_pending_commits();
                if can_push && has_pending_commits && sync.matches(SyncDirection::Push) {
                    Ok(Some(Job::push(
                        state.vid().clone(),
                        self.shared.cid().clone(),
                    )))
                } else if can_pull && sync.matches(SyncDirection::Pull) && !state.is_syncing() {
                    Ok(Some(Job::pull(state.vid().clone())))
                } else {
                    Ok(None)
                }
            })
    }
}
