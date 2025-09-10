use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
    panic,
    time::Duration,
};

use culprit::{Context, Culprit, ResultExt};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct BoxedCtx(Box<dyn Context>);

impl BoxedCtx {
    pub fn new(ctx: impl Context) -> Self {
        Self(Box::new(ctx))
    }
}

impl Error for BoxedCtx {}

impl Display for BoxedCtx {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ShutdownErr {
    #[error("task failed while shutting down Supervisor")]
    TaskFailed(#[from] BoxedCtx),

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
    type Err: Context;

    fn cfg(&self) -> TaskCfg;
    fn run(self, ctx: TaskCtx) -> impl Future<Output = Result<(), Culprit<Self::Err>>> + Send;

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
    tasks: JoinSet<(TaskCfg, Result<(), Culprit<BoxedCtx>>)>,
}

impl Supervisor {
    pub fn spawn<S: SupervisedTask + Send + 'static>(&mut self, task: S) {
        let cfg = task.cfg();
        let ctx = TaskCtx { shutdown: self.shutdown.child_token() };
        tracing::info!("spawning task {:?}", cfg);
        self.tasks.spawn(async move {
            (
                cfg,
                task.run(ctx).await.or_ctx(|err| BoxedCtx(Box::new(err))),
            )
        });
    }

    /// Supervise the tasks until they all complete.
    /// CANCEL SAFETY: This task is cancel safe.
    pub async fn supervise(&mut self) -> Result<(), Culprit<BoxedCtx>> {
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
    ) -> std::result::Result<(), Culprit<ShutdownErr>> {
        self.shutdown.cancel();

        // wait for self.supervise up to the timeout
        tokio::select! {
            result = self.supervise() => Ok(result.or_into_ctx()?),
            _ = tokio::time::sleep(abort_timeout) => {
                tracing::error!("tasks did not complete within timeout; initiating hard shutdown");
                self.tasks.abort_all();
                self.supervise().await.unwrap();
                Err(Culprit::new(ShutdownErr::Timeout))
            }
        }
    }
}
