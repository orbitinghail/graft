use std::{pin::Pin, sync::Arc, time::Duration};

use futures::{Stream, StreamExt};
use tokio::time::sleep;

use crate::{
    local::fjall_storage::{FjallStorage, FjallStorageErr},
    rt::rpc::Rpc,
};

#[derive(Debug, thiserror::Error)]
#[error("fatal runtime error")]
pub struct RuntimeFatalErr;

#[derive(Debug, thiserror::Error)]
enum RuntimeErr {
    #[error(transparent)]
    StorageError(#[from] FjallStorageErr),
}

pub enum Event {
    Rpc(Rpc),
    Tick,
}

pub struct Runtime<S> {
    storage: Arc<FjallStorage>,
    events: Pin<Box<S>>,
}

impl<S: Stream<Item = Event>> Runtime<S> {
    pub fn new(storage: Arc<FjallStorage>, events: Pin<Box<S>>) -> Self {
        Runtime { storage, events }
    }

    pub async fn start(mut self) -> Result<(), RuntimeFatalErr> {
        loop {
            match self.run().await {
                Ok(()) => {
                    tracing::debug!("runtime loop completed without error; shutting down");
                    return Ok(());
                }
                Err(err) => {
                    tracing::error!("sync task error: {:?}", err);
                    // we want to explore system states that include runtime errors
                    precept::expect_reachable!("graft-kernel runtime error", { "error": err.to_string() });
                    sleep(Duration::from_millis(100)).await
                }
            }
        }
    }

    async fn run(&mut self) -> Result<(), RuntimeErr> {
        while let Some(event) = self.events.next().await {
            match event {
                Event::Rpc(rpc) => {
                    todo!("handle rpc")
                }
                Event::Tick => {
                    todo!("handle tick")
                }
            }
        }
        Ok(())
    }
}
