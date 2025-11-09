use std::{pin::Pin, sync::Arc};

use futures::{Stream, StreamExt};
use tokio::time::Instant;

use crate::{
    err::KernelErr,
    local::fjall_storage::FjallStorage,
    remote::Remote,
    rt::{job::Job, rpc::RpcWrapper},
};

#[derive(Debug, thiserror::Error)]
#[error("fatal runtime error")]
pub struct RuntimeFatalErr;

pub enum Event {
    Rpc(RpcWrapper),
    Tick(Instant),
}

pub struct Runtime<S> {
    remote: Arc<Remote>,
    storage: Arc<FjallStorage>,
    events: Pin<Box<S>>,
    autosync: bool,
}

impl<S: Stream<Item = Event>> Runtime<S> {
    pub fn new(
        remote: Arc<Remote>,
        storage: Arc<FjallStorage>,
        events: Pin<Box<S>>,
        autosync: bool,
    ) -> Self {
        Runtime { remote, storage, events, autosync }
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
                Event::Rpc(rpc) => rpc.run(&self.storage, &self.remote).await,
                Event::Tick(_instant) => {
                    if self.autosync {
                        for job in Job::collect(&self.storage)? {
                            job.run(&self.storage, &self.remote).await?
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
