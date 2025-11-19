use std::{fmt::Debug, sync::Arc};
use tracing::Instrument;

use crate::{KernelErr, local::fjall_storage::FjallStorage, remote::Remote};

pub mod autosync;

pub type Result<T> = culprit::Result<T, KernelErr>;

/// A long running stateful async background task.
pub trait Task: Debug {
    const NAME: &'static str;

    /// Run the task.
    async fn run(&mut self, storage: &FjallStorage, remote: &Remote) -> Result<()>;

    /// Decide whether or not to restart the task on error.
    #[allow(unused_variables)]
    fn should_restart(&self, err: &KernelErr) -> bool {
        true
    }
}

/// Supervise a long running task.
/// If the task completes, this function will also complete.
/// If the task fails, `T::should_restart` is consulted.
pub async fn supervise<T: Task>(
    storage: Arc<FjallStorage>,
    remote: Arc<Remote>,
    mut task: T,
) -> Result<()> {
    for restarts in 0usize.. {
        let span = tracing::debug_span!("task", t = T::NAME, r = restarts);
        match task.run(&storage, &remote).instrument(span).await {
            Ok(()) => {
                tracing::debug!("task {:?} completed without error; shutting down", task);
                break;
            }
            Err(err) => {
                tracing::error!("task {:?} failed: {:?}", task, err);
                if !task.should_restart(err.ctx()) {
                    return Err(err);
                }
            }
        }
        tracing::debug!("restarting task {:?}", task);
    }
    Ok(())
}
