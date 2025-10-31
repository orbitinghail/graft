use graft_core::{PageIdx, commit::SegmentIdx, page::Page};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
};

use crate::GraftErr;

type Result<T> = culprit::Result<T, GraftErr>;

#[derive(Debug)]
pub enum Rpc {
    RemoteReadPage {
        idx: SegmentIdx,
        pageidx: PageIdx,
        complete: oneshot::Sender<Result<Page>>,
    },
}

#[derive(Clone, Debug)]
pub struct RpcHandle {
    tx: mpsc::Sender<Rpc>,
}

impl RpcHandle {
    pub fn new(tx: mpsc::Sender<Rpc>) -> Self {
        Self { tx }
    }

    pub fn remote_read_page(&self, idx: SegmentIdx, pageidx: PageIdx) -> Result<Page> {
        let (tx, rx) = oneshot::channel();
        self.rpc(Rpc::RemoteReadPage { idx, pageidx, complete: tx }, rx)
    }

    fn rpc<T>(&self, msg: Rpc, recv: oneshot::Receiver<T>) -> T {
        if let Ok(tokio_rt) = Handle::try_current() {
            tokio_rt.block_on(async move {
                self.tx
                    .send(msg)
                    .await
                    .expect("BUG: runtime RPC channel closed");
                recv.await.expect("BUG: RPC response channel closed")
            })
        } else {
            self.tx
                .blocking_send(msg)
                .expect("SyncRpc: control channel closed");
            recv.blocking_recv()
                .expect("SyncRpc: response channel closed")
        }
    }
}
