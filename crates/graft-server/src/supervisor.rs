use std::{
    fmt::{Debug, Display, Formatter},
    future::Future,
    marker::Send,
    panic,
    time::Duration,
};

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use trackerr::{format_location_stack, LocationStack};

pub type Result<T> = std::result::Result<T, TaskErr>;

pub struct TaskErr {
    source: Box<dyn LocationStack + Send + Sync + 'static>,
}

impl std::error::Error for TaskErr {}

impl Debug for TaskErr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        format_location_stack(f, &self.source)
    }
}

impl Display for TaskErr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        format_location_stack(f, &self.source)
    }
}

impl<T: LocationStack + Send + Sync + 'static> From<T> for TaskErr {
    fn from(value: T) -> Self {
        Self { source: Box::new(value) }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ShutdownErr {
    #[error("task failed while shutting down Supervisor")]
    TaskFailed(#[from] TaskErr),

    #[error("timeout while waiting for Supervisor to cleanly shutdown")]
    Timeout,
}

#[derive(Clone, Debug)]
pub struct TaskCfg {
    pub name: &'static str,
}

pub struct TaskCtx {
    shutdown: CancellationToken,
}

impl TaskCtx {
    /// Returns true if the shutdown signal has been set and this task should exit at the next opportunity.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.is_cancelled()
    }

    /// A future that waits until the task has shutdown.
    /// CANCEL SAFETY: This task is cancel safe.
    pub async fn wait_shutdown(&self) {
        self.shutdown.cancelled().await
    }
}

pub trait SupervisedTask {
    fn cfg(&self) -> TaskCfg;
    fn run(self, ctx: TaskCtx) -> impl Future<Output = Result<()>> + Send;

    #[cfg(test)]
    fn testonly_spawn(self)
    where
        Self: Sized + Send + 'static,
    {
        let cfg = self.cfg();
        let ctx = TaskCtx { shutdown: CancellationToken::new() };
        tracing::info!("spawning task {:?}", cfg);
        tokio::spawn(async move { self.run(ctx).await.unwrap() });
    }
}

#[derive(Default)]
pub struct Supervisor {
    shutdown: CancellationToken,
    tasks: JoinSet<(TaskCfg, Result<()>)>,
}

impl Supervisor {
    pub fn spawn<S: SupervisedTask + Send + 'static>(&mut self, task: S) {
        let cfg = task.cfg();
        let ctx = TaskCtx { shutdown: self.shutdown.child_token() };
        tracing::info!("spawning task {:?}", cfg);
        self.tasks.spawn(async move { (cfg, task.run(ctx).await) });
    }

    /// Supervise the tasks until they all complete.
    /// CANCEL SAFETY: This task is cancel safe.
    pub async fn supervise(&mut self) -> Result<()> {
        while let Some(res) = self.tasks.join_next().await {
            match res {
                Ok((cfg, Ok(()))) => {
                    tracing::info!("task {} completed successfully", cfg.name);
                }
                Ok((cfg, Err(e))) => {
                    tracing::error!("task {} failed: {:?}", cfg.name, e);
                    // for now, all failures are fatal
                    // eventually we may want to restart certain tasks in
                    // certain error conditions
                    return Err(e);
                }
                Err(e) => {
                    if e.is_panic() {
                        // propagate panics
                        panic::resume_unwind(e.into_panic());
                    } else {
                        // task was aborted, ideally this does not happen
                        // note: this is different than the task cooperatively
                        // cancelling when it's cancel token is closed
                        assert!(e.is_cancelled());
                        panic!("task was aborted");
                    }
                }
            }
        }

        // if we get here, then all tasks have closed
        Ok(())
    }

    pub async fn shutdown(
        &mut self,
        abort_timeout: Duration,
    ) -> std::result::Result<(), ShutdownErr> {
        self.shutdown.cancel();

        // wait for self.supervise up to the timeout
        tokio::select! {
            result = self.supervise() => Ok(result?),
            _ = tokio::time::sleep(abort_timeout) => {
                tracing::error!("tasks did not complete within timeout; initiating hard shutdown");
                self.tasks.abort_all();
                self.supervise().await.unwrap();
                Err(ShutdownErr::Timeout)
            }
        }
    }
}
