use std::sync::Arc;

use foldhash::fast::RandomState;
use graft_core::SegmentId;
use object_store::{path::Path, ObjectStore};

use crate::limiter::Limiter;
use crate::storage::cache::Cache;

pub struct Loader<O, C> {
    store: Arc<O>,
    cache: Arc<C>,

    download_limiter: Limiter<SegmentId, RandomState>,
}

impl<O: ObjectStore, C: Cache> Loader<O, C> {
    pub fn new(store: Arc<O>, cache: Arc<C>, download_concurrency: usize) -> Self {
        Self {
            store,
            cache,
            download_limiter: Limiter::new(download_concurrency),
        }
    }

    pub async fn load_segment(&self, sid: SegmentId) -> std::io::Result<C::Item<'_>> {
        // optimistically retrieve segment from cache
        if let Some(segment) = self.cache.get(&sid).await? {
            return Ok(segment);
        }

        // acquire a download permit for the segment
        let _permit = self.download_limiter.acquire(&sid).await;

        // check the cache again in case another task has downloaded the segment
        if let Some(segment) = self.cache.get(&sid).await? {
            return Ok(segment);
        }

        // download the segment
        let path = Path::from(sid.pretty());
        let obj = self.store.get(&path).await?;
        let data = obj.bytes().await?;

        // insert the segment into the cache
        self.cache.put(&sid, data).await?;

        // drop the permit; allowing other downloads to proceed
        drop(_permit);

        // return the segment
        Ok(self
            .cache
            .get(&sid)
            .await?
            .expect("segment not found after download"))
    }
}
