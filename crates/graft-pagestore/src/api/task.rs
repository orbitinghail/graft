use std::sync::Arc;

use graft_core::supervisor::{SupervisedTask, TaskCfg, TaskCtx};
use object_store::ObjectStore;
use tokio::{net::TcpListener, sync::mpsc};

use crate::{
    segment::{
        bus::{Bus, CommitSegmentReq, WritePageReq},
        loader::Loader,
    },
    storage::cache::Cache,
    volume::catalog::VolumeCatalog,
};

use super::{router::router, state::ApiState};

pub struct ApiServerTask<O, C> {
    listener: TcpListener,
    state: Arc<ApiState<O, C>>,
}

impl<O, C> ApiServerTask<O, C> {
    pub fn new(
        listener: TcpListener,
        page_tx: mpsc::Sender<WritePageReq>,
        commit_bus: Bus<CommitSegmentReq>,
        catalog: VolumeCatalog,
        loader: Loader<O, C>,
    ) -> Self {
        Self {
            listener,
            state: Arc::new(ApiState::new(page_tx, commit_bus, catalog, loader)),
        }
    }
}

impl<O, C> SupervisedTask for ApiServerTask<O, C>
where
    O: ObjectStore + Sync + Send + 'static,
    C: Cache + Sync + Send + 'static,
{
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
