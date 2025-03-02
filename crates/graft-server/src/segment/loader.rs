use std::sync::Arc;

use culprit::ResultExt;
use graft_core::SegmentId;
use object_store::{ObjectStore, path::Path};
use thiserror::Error;

use super::cache::Cache;
use crate::{api::error::ApiErrCtx, limiter::Limiter};

#[derive(Debug, Error)]
pub enum SegmentLoaderErr {
    #[error("failed to load segment from cache")]
    Cache(std::io::ErrorKind),

    #[error("failed to download segment")]
    DownloadSegment(object_store::Error),
}

impl From<object_store::Error> for SegmentLoaderErr {
    fn from(err: object_store::Error) -> Self {
        Self::DownloadSegment(err)
    }
}

impl From<std::io::Error> for SegmentLoaderErr {
    fn from(err: std::io::Error) -> Self {
        Self::Cache(err.kind())
    }
}

impl From<SegmentLoaderErr> for ApiErrCtx {
    fn from(err: SegmentLoaderErr) -> Self {
        match err {
            SegmentLoaderErr::Cache(ioerr) => ApiErrCtx::IoErr(ioerr),
            SegmentLoaderErr::DownloadSegment(_) => ApiErrCtx::SegmentDownloadErr,
        }
    }
}

pub struct SegmentLoader<C> {
    store: Arc<dyn ObjectStore>,
    cache: Arc<C>,

    download_limiter: Limiter<SegmentId>,
}

impl<C: Cache> SegmentLoader<C> {
    pub fn new(store: Arc<dyn ObjectStore>, cache: Arc<C>, download_concurrency: usize) -> Self {
        Self {
            store,
            cache,
            download_limiter: Limiter::new(download_concurrency),
        }
    }

    pub async fn load_segment(
        &self,
        sid: SegmentId,
    ) -> culprit::Result<C::Item<'_>, SegmentLoaderErr> {
        // optimistically retrieve segment from cache
        if let Some(segment) = self.cache.get(&sid).await.or_into_ctx()? {
            return Ok(segment);
        }

        // acquire a download permit for the segment
        let _permit = self.download_limiter.acquire(&sid).await;

        // check the cache again in case another task has downloaded the segment
        if let Some(segment) = self.cache.get(&sid).await.or_into_ctx()? {
            return Ok(segment);
        }

        // download the segment
        let path = Path::from(sid.pretty());
        let obj = self.store.get(&path).await?;
        let data = obj.bytes().await?;

        // insert the segment into the cache
        self.cache.put(&sid, data).await.or_into_ctx()?;

        // drop the permit; allowing other downloads to proceed
        drop(_permit);

        // return the segment
        Ok(self
            .cache
            .get(&sid)
            .await
            .or_into_ctx()?
            .expect("segment not found after download"))
    }
}
