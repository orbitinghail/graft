use graft_core::{PageIdx, page::Page};
use tryiter::TryIteratorExt;

use crate::{
    local::fjall_storage::{FjallStorage, FjallStorageErr},
    rt::rpc::{RpcHandle, RuntimeRpc},
    snapshot::Snapshot,
};

#[derive(Debug, Clone)]
pub struct VolumeReader {
    storage: FjallStorage,
    rpc: RpcHandle,
    snapshot: Snapshot,
}

impl VolumeReader {
    pub fn read_page(&self, pageidx: PageIdx) -> culprit::Result<Option<Page>, FjallStorageErr> {
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
                return Ok(Some(page));
            }

            // page is not available locally, fall back to loading the page from remote storage.
            // let frame = idx
            //     .frame_for_pageidx(pageidx)
            //     .expect("commit claims to contain pageidx but no frame found");

            todo!("load page from remote storage")
        }

        Ok(None)
    }
}
