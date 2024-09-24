//! The cache manages on disk and mem-mapped segments.
//! The cache needs to respect the following limits:
//!   - Disk space
//!   - Maximum open file descriptors (maximum mmap'ed segments)

use graft_core::guid::SegmentId;

pub trait CacheBackend {}

pub enum CacheEntry {
    Occupied,
    Vacant,
}

pub struct Cache<B> {
    backend: B,
}

impl<B: CacheBackend> Cache<B> {
    pub fn entry(&mut self, sid: SegmentId) -> CacheEntry {
        CacheEntry::Vacant
    }
}
