use std::{collections::HashSet, fmt::Debug};

use culprit::ResultExt;
use futures::stream::FuturesUnordered;
use graft_core::VolumeId;
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

            enum Subtask {
                Push { graft: VolumeId },
                Pull { graft: VolumeId },
            }

            // a set of VolumeIDs to fetch
            let mut fetches = HashSet::new();
            // a set of actions to execute
            let mut actions = vec![];

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

                    if remote_changes && local_changes {
                        // graft has diverged and requires user/app intervention
                    } else if remote_changes {
                        actions.push(Subtask::Pull { graft: graft.local })
                    } else if local_changes {
                        actions.push(Subtask::Push { graft: graft.local })
                    } else {
                        fetches.insert(graft.remote);
                        actions.push(Subtask::Pull { graft: graft.local });
                    }
                }
            }

            // execute all scheduled fetches
            let mut futures: FuturesUnordered<_> = fetches
                .into_iter()
                .map(|vid| FetchVolume { vid, max_lsn: None }.run(storage, remote))
                .collect();
            while let Some(result) = futures.next().await {
                if let Err(err) = result {
                    tracing::error!("Autosync fetch failed: {:?}", err);
                }
            }

            // execute all scheduled actions
            let mut futures: FuturesUnordered<_> = actions
                .into_iter()
                .map(|action| async {
                    match action {
                        Subtask::Push { graft } => {
                            RemoteCommit { graft }.run(storage, remote).await
                        }
                        Subtask::Pull { graft } => storage
                            .sync_remote_to_local(graft)
                            .or_into_culprit("syncing changes from remote"),
                    }
                })
                .collect();
            while let Some(result) = futures.next().await {
                if let Err(err) = result {
                    tracing::error!("Autosync action failed: {:?}", err);
                }
            }
        }
    }
}
