//! The bus module contains the messages that are sent between the different
//! components of the segment writing subsystem.

use std::sync::Arc;

use graft_core::{PageIdx, SegmentId, VolumeId, page::Page};
use tokio::sync::broadcast;

use super::{multigraft::MultiGraft, open::OpenSegment};

#[derive(Debug)]
pub struct WritePageMsg {
    pub vid: VolumeId,
    pub pageidx: PageIdx,
    pub page: Page,
}

impl WritePageMsg {
    pub fn new(vid: VolumeId, pageidx: PageIdx, page: Page) -> Self {
        Self { vid, pageidx, page }
    }
}

#[derive(Debug)]
pub struct StoreSegmentMsg {
    pub segment: OpenSegment,
}

#[derive(Debug, Clone)]
pub struct SegmentUploadedMsg {
    pub sid: SegmentId,
    pub grafts: Arc<MultiGraft>,
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
