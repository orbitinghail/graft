use std::{ops::Deref, path::PathBuf};

use graft_core::{
    byte_unit::ByteUnit,
    guid::SegmentId,
    hash_table::{HTEntry, HashTable},
};
use tokio::{fs::File, io::AsyncWriteExt, sync::RwLock};

use crate::resource_pool::{ResourceHandle, ResourcePool, ResourcePoolGuard};
use crate::storage::atomic_file::AtomicFileWriter;

use super::cache::Cache;

struct Segment {
    sid: SegmentId,
    size: ByteUnit,
    mmap_handle: ResourceHandle,
}

impl HTEntry for Segment {
    type Key = SegmentId;

    fn key(&self) -> &Self::Key {
        &self.sid
    }
}

pub struct MappedSegment<'a> {
    mmap: ResourcePoolGuard<'a, memmap2::Mmap>,
}

impl Deref for MappedSegment<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.mmap
    }
}

pub struct DiskCache {
    /// The maximum amount of space that the cache can use.
    space_limit: ByteUnit,

    /// Index of cached segments.
    segments: RwLock<HashTable<Segment>>,

    /// Pool of mmap'ed segments.
    mmap_pool: ResourcePool<memmap2::Mmap>,
}

impl DiskCache {
    /// Create a new cache.
    ///
    /// **Parameters:**
    /// - `space_limit` The maximum amount of space that the cache can use.
    /// - `open_limit` The maximum number of mmap'ed segments.
    pub fn new(space_limit: ByteUnit, open_limit: usize) -> Self {
        Self {
            space_limit,
            segments: Default::default(),
            mmap_pool: ResourcePool::new(open_limit),
        }
    }
}

impl Cache for DiskCache {
    type Item<'a> = MappedSegment<'a>;

    async fn put(&self, sid: &SegmentId, data: bytes::Bytes) -> std::io::Result<()> {
        let path = PathBuf::from(sid.pretty());

        // write the data to disk
        let mut file = AtomicFileWriter::open(&path).await?;
        file.write_all(&data).await?;

        // optimistically commit the file, aborting if it already exists
        let size = match file.commit().await {
            Ok(size) => size,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
            Err(e) => return Err(e),
        };

        // insert the segment into the cache
        self.segments.write().await.insert(Segment {
            sid: sid.clone(),
            size,
            mmap_handle: Default::default(),
        });

        Ok(())
    }

    async fn get(&self, sid: &SegmentId) -> std::io::Result<Option<Self::Item<'_>>> {
        let segments = self.segments.read().await;

        if let Some(segment) = segments.find(sid) {
            let mmap = self
                .mmap_pool
                .get(&segment.mmap_handle, || async {
                    let path = PathBuf::from(sid.pretty());
                    let file = File::open(&path).await?;
                    let mmap = unsafe { memmap2::MmapOptions::new().map(&file) }?;
                    Ok::<_, std::io::Error>(mmap)
                })
                .await?;

            Ok(Some(MappedSegment { mmap }))
        } else {
            Ok(None)
        }
    }
}
