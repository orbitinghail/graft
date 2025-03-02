//! The bus module contains the messages that are sent between the different
//! components of the segment writing subsystem.

use std::sync::Arc;

use culprit::Culprit;
use graft_core::{PageIdx, SegmentId, VolumeId, page::Page};
use tokio::sync::broadcast;

use super::{multigraft::MultiGraft, open::OpenSegment, uploader::SegmentUploadErr};

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
pub enum SegmentUploadMsg {
    Success {
        grafts: Arc<MultiGraft>,
        sid: SegmentId,
    },
    Failure {
        grafts: Arc<MultiGraft>,
        err: Culprit<SegmentUploadErr>,
    },
}

impl SegmentUploadMsg {
    pub fn graft(&self, vid: &VolumeId) -> Option<&splinter::SplinterRef<bytes::Bytes>> {
        match self {
            Self::Success { grafts, .. } => grafts.get(vid),
            Self::Failure { grafts, .. } => grafts.get(vid),
        }
    }

    pub fn sid(&self) -> Result<&SegmentId, Culprit<SegmentUploadErr>> {
        match self {
            Self::Success { sid, .. } => Ok(sid),
            Self::Failure { err, .. } => Err(err.clone()),
        }
    }
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
