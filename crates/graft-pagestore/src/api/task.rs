use std::sync::Arc;

use tokio::{net::TcpListener, sync::mpsc};

use crate::{
    segment::bus::{Bus, CommitSegmentReq, WritePageReq},
    supervisor::{SupervisedTask, TaskCfg, TaskCtx},
};

use super::{router::router, state::ApiState};

pub struct ApiServerTask {
    listener: TcpListener,
    state: Arc<ApiState>,
}

impl ApiServerTask {
    pub fn new(
        listener: TcpListener,
        page_tx: mpsc::Sender<WritePageReq>,
        commit_bus: Bus<CommitSegmentReq>,
    ) -> Self {
        Self {
            listener,
            state: Arc::new(ApiState::new(page_tx, commit_bus)),
        }
    }
}

impl SupervisedTask for ApiServerTask {
    fn cfg(&self) -> TaskCfg {
        TaskCfg { name: "api-server" }
    }

    async fn run(self, ctx: TaskCtx) -> anyhow::Result<()> {
        let app = router().with_state(self.state.clone());
        axum::serve(self.listener, app)
            .with_graceful_shutdown(async move {
                ctx.wait_shutdown().await;
            })
            .await?;
        Ok(())
    }
}
