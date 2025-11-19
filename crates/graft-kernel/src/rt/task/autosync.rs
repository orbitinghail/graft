use std::fmt::Debug;

use culprit::ResultExt;
use futures::stream::FuturesUnordered;
use tokio::time::Interval;
use tokio_stream::StreamExt;
use tryiter::TryIteratorExt;

use crate::{
    KernelErr,
    local::fjall_storage::FjallStorage,
    remote::Remote,
    rt::{
        action::{Action, FetchVolume, RemoteCommit},
        task::{Result, Task},
    },
};

pub struct AutosyncTask {
    ticker: Interval,
}

impl AutosyncTask {
    pub fn new(ticker: Interval) -> Self {
        Self { ticker }
    }
}

impl Debug for AutosyncTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutosyncTask")
            .field("interval", &self.ticker.period())
            .finish()
    }
}

impl Task for AutosyncTask {
    const NAME: &'static str = "autosync";

    async fn run(&mut self, storage: &FjallStorage, remote: &Remote) -> Result<()> {
        loop {
            // wait for the next tick
            self.ticker.tick().await;

            let mut actions = FuturesUnordered::new();

            // collect actions
            {
                let reader = storage.read();
                let mut grafts = reader
                    .iter_grafts()
                    .map_err(|err| err.map_ctx(KernelErr::from));
                while let Some(graft) = grafts.try_next()? {
                    let latest_local = reader.latest_lsn(&graft.local).or_into_ctx()?;
                    let latest_remote = reader.latest_lsn(&graft.remote).or_into_ctx()?;
                    let local_changes = graft.local_changes(latest_local).is_some();
                    let remote_changes = graft.remote_changes(latest_remote).is_some();

                    actions.push(async move {
                        if remote_changes && local_changes {
                            // graft has diverged and requires user/app intervention
                            Ok(())
                        } else if remote_changes {
                            storage
                                .sync_remote_to_local(graft.local)
                                .or_into_culprit("syncing changes from remote")
                        } else if local_changes {
                            RemoteCommit { graft: graft.local }
                                .run(storage, remote)
                                .await
                                .or_into_culprit("committing to remote")
                        } else {
                            FetchVolume { vid: graft.remote, max_lsn: None }
                                .run(storage, remote)
                                .await?;
                            storage
                                .sync_remote_to_local(graft.local)
                                .or_into_culprit("refreshing remote")
                        }
                    });
                }
            }

            // process actions
            while let Some(result) = actions.next().await {
                if let Err(err) = result {
                    tracing::error!("Autosync action failed: {:?}", err);
                }
            }
        }
    }
}
