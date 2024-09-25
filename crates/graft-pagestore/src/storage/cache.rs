//! The cache manages on disk and mem-mapped segments.
//! The cache needs to respect the following limits:
//!   - Disk space
//!   - Maximum open file descriptors (maximum mmap'ed segments)

use graft_core::{byte_unit::ByteUnit, guid::SegmentId};

pub trait CacheBackend {}

pub enum CacheEntry {
    Occupied,
    Vacant,
}

pub struct Cache<B> {
    backend: B,

    /// The maximum amount of space that the cache can use.
    space_limit: ByteUnit,

    /// The maximum number of segments that can be mmap'ed at the same time.
    open_limit: usize,
}

impl<B: CacheBackend> Cache<B> {
    pub fn entry(&mut self, sid: SegmentId) -> CacheEntry {
        CacheEntry::Vacant
    }
}
