use graft_core::{SegmentId, VolumeId, commit::SegmentRangeRef, lsn::LSN};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
};

use crate::GraftErr;

type Result<T> = culprit::Result<T, GraftErr>;

#[derive(Debug)]
pub enum Rpc {
    FetchSegmentRange {
        sid: SegmentId,
        range: SegmentRangeRef,
        complete: oneshot::Sender<Result<()>>,
    },
    HydrateVolume {
        vid: VolumeId,
        max_lsn: Option<LSN>,
        complete: oneshot::Sender<Result<()>>,
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

    pub fn fetch_segment_range(&self, sid: SegmentId, range: SegmentRangeRef) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.rpc(Rpc::FetchSegmentRange { sid, range, complete: tx }, rx)
    }

    pub fn hydrate_volume(&self, vid: VolumeId, max_lsn: Option<LSN>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.rpc(Rpc::HydrateVolume { vid, max_lsn, complete: tx }, rx)
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
