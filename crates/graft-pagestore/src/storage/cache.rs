//! The cache manages on disk and mem-mapped segments.
//! The cache needs to respect the following limits:
//!   - Disk space
//!   - Maximum open file descriptors (maximum mmap'ed segments)

use std::{io, sync::Arc};

use bytes::Bytes;
use graft_core::{
    byte_unit::ByteUnit,
    guid::SegmentId,
    hash_table::{HTEntry, HashTable},
};
use tokio::sync::RwLock;

pub trait CacheBackend: Send + Sync {}

struct SegmentInfo {
    sid: SegmentId,
    size: ByteUnit,
}

impl HTEntry for SegmentInfo {
    type Key = SegmentId;

    fn key(&self) -> &Self::Key {
        &self.sid
    }
}

impl SegmentInfo {
    fn new(sid: SegmentId, size: ByteUnit) -> Self {
        Self { sid, size }
    }
}

pub type CacheRef<B> = Arc<Cache<B>>;

pub struct Cache<B> {
    backend: B,

    /// The maximum amount of space that the cache can use.
    space_limit: ByteUnit,

    /// The maximum number of segments that can be mmap'ed at the same time.
    open_limit: usize,

    /// In-memory map of cached segments
    segments: RwLock<HashTable<SegmentInfo>>,
}

impl<B: CacheBackend> Cache<B> {
    pub fn new(backend: B, space_limit: ByteUnit, open_limit: usize) -> Self {
        Self {
            backend,
            space_limit,
            open_limit,
            segments: Default::default(),
        }
    }

    pub async fn put(&self, sid: SegmentId, segment: Bytes) -> io::Result<()> {
        // let entry = self.segments.write().await.entry(&sid)
        // self.backend.put(sid, segment).await?;

        // ok need to think through this a bit more. the cache needs to handle
        // concurrent writers to the same segment in the event that we are
        // retrieving a segment from object storage rather than creating one

        // in this case, multiple tasks may all attempt to read a segment,
        // notice it's not cached, and try to retrieve it it's not clear if they
        // will run downloads themselves, or offload that to another task.
        // however if the cache is safe to multiple concurrent accesses to the
        // same segment it seems like the rest of the code would be simpler.

        todo!()
    }
}

pub struct CacheWriter {
    entry: Arc<SegmentInfo>,
}
