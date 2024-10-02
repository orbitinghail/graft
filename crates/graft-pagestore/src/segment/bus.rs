//! The bus module contains the messages that are sent between the different
//! components of the segment writing subsystem.

use std::sync::atomic::{AtomicU64, Ordering};

use ahash::HashMap;
use graft_core::{guid::SegmentId, guid::VolumeId, offset::Offset, page::Page};

use super::open::OpenSegment;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct RequestGroup(u64);

impl RequestGroup {
    pub fn next() -> Self {
        static NEXT_GROUP: AtomicU64 = AtomicU64::new(0);
        Self(NEXT_GROUP.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub struct RequestGroupAggregate(HashMap<RequestGroup, u32>);

impl RequestGroupAggregate {
    pub fn add(&mut self, group: RequestGroup) {
        *self.0.entry(group).or_insert(0) += 1;
    }

    pub fn count(&self, group: RequestGroup) -> u32 {
        self.0.get(&group).copied().unwrap_or(0)
    }

    pub fn total_count(&self) -> u32 {
        self.0.values().sum()
    }
}

#[derive(Debug)]
pub struct WritePageRequest {
    pub(super) group: RequestGroup,
    pub(super) vid: VolumeId,
    pub(super) offset: Offset,
    pub(super) page: Page,
}

impl WritePageRequest {
    pub fn new(group: RequestGroup, vid: VolumeId, offset: Offset, page: Page) -> Self {
        Self { group, vid, offset, page }
    }
}

#[derive(Debug)]
pub struct StoreSegmentRequest {
    pub(super) groups: RequestGroupAggregate,
    pub(super) segment: OpenSegment,
}

#[derive(Debug)]
pub struct CommitSegmentRequest {
    pub(super) groups: RequestGroupAggregate,
    pub(super) sid: SegmentId,
}
