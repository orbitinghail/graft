use axum::Router;
use tokio::net::TcpListener;

use crate::supervisor::{SupervisedTask, TaskCfg, TaskCtx};

pub struct ApiServerTask {
    listener: TcpListener,
    router: Router,
}

impl ApiServerTask {
    pub fn new(listener: TcpListener, router: Router) -> Self {
        Self { listener, router }
    }
}

impl SupervisedTask for ApiServerTask {
    fn cfg(&self) -> TaskCfg {
        TaskCfg { name: "api-server" }
    }

    async fn run(self, ctx: TaskCtx) -> anyhow::Result<()> {
        axum::serve(self.listener, self.router)
            .with_graceful_shutdown(async move {
                ctx.wait_shutdown().await;
            })
            .await?;
        Ok(())
    }
}
