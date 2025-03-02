use std::{fs::canonicalize, io, ops::Deref, path::PathBuf};

use bytes::Buf;
use graft_core::{
    SegmentId,
    byte_unit::ByteUnit,
    hash_table::{HTEntry, HashTable},
};
use serde::{Deserialize, Serialize};
use tokio::{fs::File, sync::RwLock};

use super::atomic_file::write_file_atomic;
use crate::resource_pool::{ResourceHandle, ResourcePool, ResourcePoolGuard};

use super::Cache;

struct Segment {
    sid: SegmentId,
    _size: ByteUnit,
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
    /// if not provided, a temporary directory will be created
    pub path: Option<PathBuf>,

    /// The maximum amount of space that the cache can use.
    pub space_limit: ByteUnit,

    /// The maximum number of mmap'ed segments.
    pub open_limit: usize,
}

pub struct DiskCache {
    dir: PathBuf,

    /// The maximum amount of space that the cache can use.
    _space_limit: ByteUnit,

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
    pub fn new(config: DiskCacheConfig) -> io::Result<Self> {
        let dir = if let Some(path) = config.path {
            canonicalize(path)?
        } else {
            tempfile::tempdir()?.into_path()
        };
        tracing::info!("Opening disk cache at {:?}", dir);
        Ok(Self {
            dir,
            _space_limit: config.space_limit,
            segments: Default::default(),
            mmap_pool: ResourcePool::new(config.open_limit),
        })
    }
}

impl Cache for DiskCache {
    type Item<'a> = MappedSegment<'a>;

    async fn put<T: Buf + Send + 'static>(
        &self,
        sid: &SegmentId,
        data: T,
    ) -> culprit::Result<(), io::Error> {
        let path = self.dir.join(sid.pretty());

        tracing::trace!("writing segment {:?} to disk at path {:?}", sid, path);

        let data_size = data.remaining().into();

        // optimistically write the file to disk, aborting if it already exists
        match write_file_atomic(&path, data).await {
            Ok(()) => (),
            // we don't need to update self.segments if the file already exists
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(()),
            Err(e) => {
                tracing::error!("failed to write segment {:?} to disk: {:?}", sid, e);
                return Err(e.into());
            }
        }

        // insert the segment into the cache
        self.segments.write().await.insert(Segment {
            sid: sid.clone(),
            _size: data_size,
            mmap_handle: Default::default(),
        });

        Ok(())
    }

    async fn get(&self, sid: &SegmentId) -> culprit::Result<Option<Self::Item<'_>>, io::Error> {
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
