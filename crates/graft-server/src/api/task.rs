use std::io;

use axum::Router;
use tokio::net::TcpListener;
use trackerr::{CallerLocation, LocationStack};

use crate::supervisor::{self, SupervisedTask, TaskCfg, TaskCtx};

#[derive(Debug, thiserror::Error)]
#[error("API server error: {source}")]
pub struct ApiServerErr {
    #[from]
    source: io::Error,
    #[implicit]
    location: CallerLocation,
}

impl LocationStack for ApiServerErr {
    fn location(&self) -> &CallerLocation {
        &self.location
    }

    fn next(&self) -> Option<&dyn LocationStack> {
        None
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
    fn cfg(&self) -> TaskCfg {
        TaskCfg { name: self.name }
    }

    async fn run(self, ctx: TaskCtx) -> supervisor::Result<()> {
        axum::serve(self.listener, self.router)
            .with_graceful_shutdown(async move {
                ctx.wait_shutdown().await;
            })
            .await
            .map_err(ApiServerErr::from)?;
        Ok(())
    }
}
