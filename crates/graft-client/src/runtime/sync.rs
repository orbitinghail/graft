use std::{sync::Arc, time::Duration};

use tokio::select;
use tokio_util::sync::CancellationToken;

use crate::{ClientErr, ClientPair};

use super::storage::Storage;

mod job;

/// A SyncTask is a background task which continuously syncs volumes to and from
/// a Graft service.
pub struct SyncTask {
    storage: Arc<Storage>,
    clients: ClientPair,
    ticker: tokio::time::Interval,
    token: CancellationToken,
}

impl SyncTask {
    pub fn new(
        storage: Arc<Storage>,
        clients: ClientPair,
        refresh_interval: Duration,
    ) -> (CancellationToken, Self) {
        let ticker = tokio::time::interval(refresh_interval);
        let token = CancellationToken::new();
        (token.clone(), Self { storage, clients, ticker, token })
    }

    pub async fn run(mut self) {
        loop {
            match self.run_inner().await {
                Ok(_) => {
                    log::info!("sync task completed");
                    break;
                }
                Err(err) => {
                    log::error!("sync task error: {:?}", err);
                }
            }
        }
    }

    async fn run_inner(&mut self) -> Result<(), ClientErr> {
        loop {
            select! {
                _ = self.ticker.tick() => {
                    // Refresh sync jobs
                    log::info!("refreshing sync jobs");
                }
                _ = self.storage.listen_for_commit() => {
                    // Push changed volumes
                    log::info!("commit detected, pushing volumes");

                }
                _ = self.token.cancelled() => {
                    log::info!("sync task shutting down");
                    break;
                }
            }

            /*
            The sync task is responsible for syncing all volumes up and down from the server.

            We can organize the code into jobs:
            Pull{vid, snapshot}
            Push{vid, sync_snapshot, snapshot, lsn_range}

            To generate pull jobs:
            - iterate through all volumes
                - snapshot = storage.snapshot(vid, remote)
                - Pull{vid, snapshot}

            To generate push jobs:
            - iterate through all volumes
                - sync_snapshot = storage.snapshot(vid, sync)
                - snapshot = storage.snapshot(vid, local)
                - lsn_range = sync_snapshot.lsn()..snapshot.lsn()
                - Push{vid, sync_snapshot, snapshot, lsn_range}

            When the sync task runs it will generate Push jobs for each locally
            changed volume, and pull jobs for any volume we haven't pulled
            recently.

            Rather than polling continuously, it would be nice to put the sync
            task to sleep until either a local volume changes or the next pull
            timeout occurs. For this we'd like a signal mechanism that can
            select for a specific volume or all volumes.
            */
        }
        Ok(())
    }
}
