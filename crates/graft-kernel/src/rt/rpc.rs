use culprit::ResultExt;
use graft_core::{VolumeId, commit::SegmentRangeRef, lsn::LSN};
use tokio::{
    runtime::Handle,
    sync::{mpsc, oneshot},
};

use crate::{KernelErr, local::fjall_storage::FjallStorage, remote::Remote, rt::job::Job};

type Result<T> = culprit::Result<T, KernelErr>;

#[derive(Debug)]
pub struct RpcWrapper {
    rpc: Rpc,
    complete: oneshot::Sender<Result<()>>,
}

impl RpcWrapper {
    pub async fn run(self, storage: &FjallStorage, remote: &Remote) {
        let _ = self.complete.send(self.rpc.run(storage, remote).await);
    }
}

#[derive(Debug)]
pub enum Rpc {
    FetchSegmentRange { range: SegmentRangeRef },
    HydrateVolume { vid: VolumeId, max_lsn: Option<LSN> },
    FetchVolume { vid: VolumeId },
    PullGraft { graft: VolumeId },
    PushGraft { graft: VolumeId },
}

impl Rpc {
    pub async fn run(
        self,
        storage: &FjallStorage,
        remote: &Remote,
    ) -> culprit::Result<(), KernelErr> {
        match self {
            Rpc::FetchSegmentRange { range } => {
                Job::fetch_segment(range).run(storage, remote).await
            }
            Rpc::HydrateVolume { vid, max_lsn } => {
                Job::hydrate_volume(vid, max_lsn).run(storage, remote).await
            }
            Rpc::FetchVolume { vid } => Job::fetch_volume(vid, None).run(storage, remote).await,
            Rpc::PullGraft { graft } => {
                let graft = storage.read().graft(&graft).or_into_ctx()?;
                Job::fetch_volume(graft.remote, None)
                    .run(storage, remote)
                    .await?;
                Job::sync_remote_to_local(graft.local)
                    .run(storage, remote)
                    .await
            }
            Rpc::PushGraft { graft } => Job::remote_commit(graft).run(storage, remote).await,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RpcHandle {
    tx: mpsc::Sender<RpcWrapper>,
}

impl RpcHandle {
    pub fn new(tx: mpsc::Sender<RpcWrapper>) -> Self {
        Self { tx }
    }

    pub fn fetch_segment_range(&self, range: SegmentRangeRef) -> Result<()> {
        self.rpc(Rpc::FetchSegmentRange { range })
    }

    pub fn hydrate_volume(&self, vid: VolumeId, max_lsn: Option<LSN>) -> Result<()> {
        self.rpc(Rpc::HydrateVolume { vid, max_lsn })
    }

    pub fn fetch_volume(&self, vid: VolumeId) -> Result<()> {
        self.rpc(Rpc::FetchVolume { vid })
    }

    pub fn pull_graft(&self, graft: VolumeId) -> Result<()> {
        self.rpc(Rpc::PullGraft { graft })
    }

    pub fn push_graft(&self, graft: VolumeId) -> Result<()> {
        self.rpc(Rpc::PushGraft { graft })
    }

    fn rpc(&self, msg: Rpc) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        let msg = RpcWrapper { rpc: msg, complete: tx };
        if let Ok(tokio_rt) = Handle::try_current() {
            tokio_rt.block_on(async move {
                self.tx
                    .send(msg)
                    .await
                    .expect("BUG: runtime RPC channel closed");
                rx.await.expect("BUG: RPC response channel closed")
            })
        } else {
            self.tx
                .blocking_send(msg)
                .expect("SyncRpc: control channel closed");
            rx.blocking_recv()
                .expect("SyncRpc: response channel closed")
        }
    }
}
