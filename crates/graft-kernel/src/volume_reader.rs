use std::sync::Arc;

use graft_core::{PageCount, PageIdx, page::Page};
use tryiter::TryIteratorExt;

use crate::{
    local::fjall_storage::{FjallStorage, FjallStorageErr},
    rt::rpc::RpcHandle,
    snapshot::Snapshot,
};

/// A type which can read from a Volume
pub trait VolumeRead {
    fn page_count(&self) -> culprit::Result<PageCount, FjallStorageErr>;
    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, FjallStorageErr>;
}

#[derive(Debug, Clone)]
pub struct VolumeReader {
    storage: Arc<FjallStorage>,
    rpc: RpcHandle,
    snapshot: Snapshot,
}

impl VolumeReader {
    pub(crate) fn new(storage: Arc<FjallStorage>, rpc: RpcHandle, snapshot: Snapshot) -> Self {
        Self { storage, rpc, snapshot }
    }

    pub(crate) fn storage(&self) -> &FjallStorage {
        &self.storage
    }
}

impl VolumeRead for VolumeReader {
    fn page_count(&self) -> culprit::Result<PageCount, FjallStorageErr> {
        if let Some(lsn) = self.snapshot.lsn() {
            let commit = self
                .storage
                .read_commit(self.snapshot.vid(), lsn)?
                .expect("no commit found for snapshot");
            Ok(commit.page_count())
        } else {
            Ok(PageCount::ZERO)
        }
    }

    fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Page, FjallStorageErr> {
        let mut commits = self.storage.commits(self.snapshot.search_path());

        while let Some(commit) = commits.try_next()? {
            if !commit.page_count().contains(pageidx) {
                // the volume is smaller than the requested page idx.
                // this also handles the case that a volume is truncated and
                // then subsequently extended at a later time.
                break;
            }

            let Some(idx) = commit.segment_idx() else {
                // this commit contains no pages
                continue;
            };

            if !idx.contains(pageidx) {
                // this commit does not contain the requested pageidx
                continue;
            }

            if let Some(page) = self.storage.read_page(idx.sid().clone(), pageidx)? {
                return Ok(page);
            }

            // page is not available locally, fall back to loading the page from remote storage.
            // let frame = idx
            //     .frame_for_pageidx(pageidx)
            //     .expect("commit claims to contain pageidx but no frame found");

            todo!("load page from remote storage")
        }

        Ok(Page::EMPTY)
    }
}
