use std::{
    fs::canonicalize,
    ops::Deref,
    path::{Path, PathBuf},
};

use graft_core::{
    byte_unit::ByteUnit,
    hash_table::{HTEntry, HashTable},
    SegmentId,
};
use serde::{Deserialize, Serialize};
use tokio::{fs::File, sync::RwLock};

use super::atomic_file::write_file_atomic;
use crate::resource_pool::{ResourceHandle, ResourcePool, ResourcePoolGuard};

use super::Cache;

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

#[derive(Debug, Serialize, Deserialize)]
pub struct DiskCacheConfig {
    /// The path to the directory where the cache will be stored.
    pub path: PathBuf,

    /// The maximum amount of space that the cache can use.
    pub space_limit: ByteUnit,

    /// The maximum number of mmap'ed segments.
    pub open_limit: usize,
}

pub struct DiskCache {
    dir: PathBuf,

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
    pub fn new(config: DiskCacheConfig) -> Self {
        let dir = canonicalize(config.path).expect("failed to canonicalize cache directory");
        tracing::info!("Opening disk cache at {:?}", dir);
        Self {
            dir,
            space_limit: config.space_limit,
            segments: Default::default(),
            mmap_pool: ResourcePool::new(config.open_limit),
        }
    }
}

impl Cache for DiskCache {
    type Item<'a> = MappedSegment<'a>;

    async fn put(&self, sid: &SegmentId, data: bytes::Bytes) -> std::io::Result<()> {
        let path = self.dir.join(sid.pretty());

        tracing::debug!("writing segment {:?} to disk at path {:?}", sid, path);

        // optimistically write the file to disk, aborting if it already exists
        match write_file_atomic(&path, &data).await {
            Ok(()) => (),
            // we don't need to update self.segments if the file already exists
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
            Err(e) => return Err(e),
        }

        // insert the segment into the cache
        self.segments.write().await.insert(Segment {
            sid: sid.clone(),
            size: data.len().into(),
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
                    let path = self.dir.join(sid.pretty());
                    let file = File::open(&path).await?;
                    // SAFETY: This is safe as long as no other process or thread modifies the file while it is mapped.
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
