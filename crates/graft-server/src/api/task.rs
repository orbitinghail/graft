use std::io;

use axum::Router;
use culprit::Culprit;
use tokio::net::TcpListener;

use crate::supervisor::{SupervisedTask, TaskCfg, TaskCtx};

#[derive(Debug, thiserror::Error)]
#[error("API server error: {0}")]
pub struct ApiServerErr(io::ErrorKind);

impl From<io::Error> for ApiServerErr {
    fn from(err: io::Error) -> Self {
        Self(err.kind())
    }
}

pub struct ApiServerTask {
    name: &'static str,
    listener: TcpListener,
    router: Router,
}

impl ApiServerTask {
    pub fn new(name: &'static str, listener: TcpListener, router: Router) -> Self {
        Self { name, listener, router }
    }
}

impl SupervisedTask for ApiServerTask {
    type Err = ApiServerErr;

    fn cfg(&self) -> TaskCfg {
        TaskCfg { name: self.name }
    }

    async fn run(self, ctx: TaskCtx) -> Result<(), Culprit<ApiServerErr>> {
        axum::serve(self.listener, self.router)
            .with_graceful_shutdown(async move {
                ctx.wait_shutdown().await;
            })
            .await?;
        Ok(())
    }
}
