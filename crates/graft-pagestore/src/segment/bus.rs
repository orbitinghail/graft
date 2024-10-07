//! The bus module contains the messages that are sent between the different
//! components of the segment writing subsystem.

use ahash::HashMap;
use bytes::Bytes;
use graft_core::{guid::SegmentId, guid::VolumeId, offset::Offset, page::Page};
use splinter::Splinter;
use tokio::sync::broadcast::{self, error::SendError};

use super::open::OpenSegment;

#[derive(Debug)]
pub struct WritePageReq {
    pub(super) vid: VolumeId,
    pub(super) offset: Offset,
    pub(super) page: Page,
}

impl WritePageReq {
    pub fn new(vid: VolumeId, offset: Offset, page: Page) -> Self {
        Self { vid, offset, page }
    }
}

#[derive(Debug)]
pub struct StoreSegmentReq {
    pub(super) segment: OpenSegment,
}

#[derive(Debug, Clone)]
pub struct CommitSegmentReq {
    pub(super) sid: SegmentId,
    pub(super) offsets: HashMap<VolumeId, Splinter<Bytes>>,
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

    pub fn publish(&self, msg: T) -> Result<(), SendError<T>> {
        self.tx.send(msg)?;
        Ok(())
    }
}
