use std::{collections::HashSet, pin::Pin, sync::Arc};

use futures::{Stream, StreamExt};
use graft_core::VolumeId;
use tokio::time::Instant;

use crate::{
    err::KernelErr,
    local::fjall_storage::FjallStorage,
    remote::Remote,
    rt::{job::Job, rpc::Rpc},
};

#[derive(Debug, thiserror::Error)]
#[error("fatal runtime error")]
pub struct RuntimeFatalErr;

pub enum Event {
    Rpc(Rpc),
    Tick(Instant),
    Commits(HashSet<VolumeId>),
}

pub struct Runtime<S> {
    remote: Arc<Remote>,
    storage: Arc<FjallStorage>,
    events: Pin<Box<S>>,
}

impl<S: Stream<Item = Event>> Runtime<S> {
    pub fn new(remote: Arc<Remote>, storage: Arc<FjallStorage>, events: Pin<Box<S>>) -> Self {
        Runtime { remote, storage, events }
    }

    pub async fn start(mut self) -> Result<(), RuntimeFatalErr> {
        loop {
            match self.run().await {
                Ok(()) => {
                    tracing::debug!("runtime loop completed without error; shutting down");
                    return Ok(());
                }
                Err(err) => {
                    tracing::error!("runtime error: {:?}", err);
                    // we want to explore system states that include runtime errors
                    precept::expect_reachable!("graft-kernel runtime error", { "error": err.to_string() });
                }
            }
        }
    }

    async fn run(&mut self) -> culprit::Result<(), KernelErr> {
        while let Some(event) = self.events.next().await {
            match event {
                Event::Rpc(rpc) => rpc.run(&self.storage, &self.remote).await?,
                Event::Tick(_instant) => {
                    for job in Job::collect(&self.storage)? {
                        job.run(&self.storage, &self.remote).await?
                    }
                }
                Event::Commits(commits) => {
                    let jobs = commits.into_iter().map(Job::remote_commit);
                    for job in jobs {
                        job.run(&self.storage, &self.remote).await?
                    }
                }
            }
        }
        Ok(())
    }
}
