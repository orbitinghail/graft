use std::{collections::HashSet, fmt::Debug};

use crate::core::VolumeId;
use culprit::ResultExt;
use futures::stream::FuturesUnordered;
use tokio::time::Interval;
use tokio_stream::StreamExt;
use tryiter::TryIteratorExt;

use crate::{
    GraftErr,
    local::fjall_storage::FjallStorage,
    remote::Remote,
    rt::{
        action::{Action, FetchLog, RemoteCommit},
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
                Push { vid: VolumeId },
                Pull { vid: VolumeId },
            }

            // a set of LogIds to fetch
            let mut fetches = HashSet::new();
            // a set of actions to execute
            let mut actions = vec![];

            // collect actions
            {
                let reader = storage.read();
                let mut volumes = reader
                    .iter_volumes()
                    .map_err(|err| err.map_ctx(GraftErr::from));
                while let Some(volume) = volumes.try_next()? {
                    let latest_local = reader.latest_lsn(&volume.local).or_into_ctx()?;
                    let latest_remote = reader.latest_lsn(&volume.remote).or_into_ctx()?;
                    let local_changes = volume.local_changes(latest_local).is_some();
                    let remote_changes = volume.remote_changes(latest_remote).is_some();

                    if remote_changes && local_changes {
                        // volume has diverged and requires user/app intervention
                    } else if remote_changes {
                        actions.push(Subtask::Pull { vid: volume.vid })
                    } else if local_changes {
                        actions.push(Subtask::Push { vid: volume.vid })
                    } else {
                        fetches.insert(volume.remote);
                        actions.push(Subtask::Pull { vid: volume.vid });
                    }
                }
            }

            // execute all scheduled fetches
            let mut futures: FuturesUnordered<_> = fetches
                .into_iter()
                .map(|log| FetchLog { log, max_lsn: None }.run(storage, remote))
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
                        Subtask::Push { vid } => RemoteCommit { vid }.run(storage, remote).await,
                        Subtask::Pull { vid } => storage
                            .read_write()
                            .sync_remote_to_local(vid)
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
