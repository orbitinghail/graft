use std::{collections::HashSet, pin::Pin, sync::Arc};

use culprit::ResultExt;
use futures::{Stream, StreamExt};
use graft_core::{PageIdx, commit::SegmentIdx, page::Page};
use tokio::time::Instant;

use crate::{
    err::GraftErr,
    local::fjall_storage::FjallStorage,
    remote::{Remote, segment::segment_frame_iter},
    rt::{job::Job, rpc::Rpc},
    volume_name::VolumeName,
};

#[derive(Debug, thiserror::Error)]
#[error("fatal runtime error")]
pub struct RuntimeFatalErr;

pub enum Event {
    Rpc(Rpc),
    Tick(Instant),
    Commits(HashSet<VolumeName>),
}

pub struct Runtime<S> {
    remote: Arc<Remote>,
    storage: Arc<FjallStorage>,
    events: Pin<Box<S>>,
}

impl<S: Stream<Item = Event>> Runtime<S> {
    pub fn new(remote: Arc<Remote>, storage: Arc<FjallStorage>, events: Pin<Box<S>>) -> Self {
        Runtime { remote, storage, events }
    }

    pub async fn start(mut self) -> Result<(), RuntimeFatalErr> {
        loop {
            match self.run().await {
                Ok(()) => {
                    tracing::debug!("runtime loop completed without error; shutting down");
                    return Ok(());
                }
                Err(err) => {
                    tracing::error!("runtime error: {:?}", err);
                    // we want to explore system states that include runtime errors
                    precept::expect_reachable!("graft-kernel runtime error", { "error": err.to_string() });
                }
            }
        }
    }

    async fn run(&mut self) -> culprit::Result<(), GraftErr> {
        while let Some(event) = self.events.next().await {
            match event {
                Event::Rpc(rpc) => match rpc {
                    Rpc::RemoteReadPage { idx, pageidx, complete } => {
                        let _ = complete.send(self.remote_read_page(idx, pageidx).await);
                    }
                },
                Event::Tick(_instant) => {
                    for job in Job::collect(&self.storage)? {
                        job.run(&self.storage, &self.remote).await?
                    }
                }
                Event::Commits(commits) => {
                    let jobs = commits.into_iter().map(Job::remote_commit);
                    for job in jobs {
                        job.run(&self.storage, &self.remote).await?
                    }
                }
            }
        }
        Ok(())
    }

    async fn remote_read_page(
        &self,
        idx: SegmentIdx,
        pageidx: PageIdx,
    ) -> culprit::Result<Page, GraftErr> {
        // download the corresponding frame and load all of it's pages into
        // storage
        let frame = idx
            .frame_for_pageidx(pageidx)
            .expect("BUG: SegmentIdx does not contain page");
        let bytes = self
            .remote
            .get_segment_range(idx.sid(), &frame.bytes)
            .await
            .or_into_ctx()?;
        let pages = segment_frame_iter(frame.graft.iter(), &bytes);
        let mut batch = self.storage.batch();
        let mut target_page = None;
        for (pidx, page) in pages {
            if pageidx == pidx {
                // found our target page
                target_page = Some(page.clone());
            }
            batch.write_page(idx.sid().clone(), pidx, page);
        }
        batch.commit().or_into_ctx()?;
        Ok(target_page.expect("BUG: target page not found in frame"))
    }
}
