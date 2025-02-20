//! The bus module contains the messages that are sent between the different
//! components of the segment writing subsystem.

use std::sync::Arc;

use graft_core::{page::Page, PageIdx, SegmentId, VolumeId};
use tokio::sync::broadcast;

use super::{offsets_map::OffsetsMap, open::OpenSegment};

#[derive(Debug)]
pub struct WritePageReq {
    pub vid: VolumeId,
    pub offset: PageIdx,
    pub page: Page,
}

impl WritePageReq {
    pub fn new(vid: VolumeId, offset: PageIdx, page: Page) -> Self {
        Self { vid, offset, page }
    }
}

#[derive(Debug)]
pub struct StoreSegmentReq {
    pub segment: OpenSegment,
}

#[derive(Debug, Clone)]
pub struct CommitSegmentReq {
    pub sid: SegmentId,
    pub offsets: Arc<OffsetsMap>,
}

#[derive(Debug, Clone)]
pub struct Bus<T> {
    tx: broadcast::Sender<T>,
}

impl<T: Clone> Bus<T> {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<T> {
        self.tx.subscribe()
    }

    pub fn publish(&self, msg: T) {
        // An error here means there are no receivers, which is fine
        let _ = self.tx.send(msg);
    }
}
