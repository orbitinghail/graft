use std::{
    fmt::{Debug, Display},
    ops::Range,
};

use async_trait::async_trait;
use bytes::Bytes;
use foldhash::HashMap;
use futures::stream::BoxStream;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result, memory::InMemory, path::Path,
};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectStoreOp {
    Put,
    Copy,
    Rename,
    Get,
    Head,
    Delete,
    DeleteStream,
    List,
}

#[derive(Debug, Default)]
pub struct TestObjectStore {
    inner: InMemory,
    hits: Mutex<HashMap<ObjectStoreOp, usize>>,
}

impl Display for TestObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self, f)
    }
}

impl TestObjectStore {
    async fn hit(&self, op: ObjectStoreOp) {
        let mut hits = self.hits.lock().await;
        *hits.entry(op).or_insert(0) += 1;
    }

    fn hit_blocking(&self, op: ObjectStoreOp) {
        let mut hits = self.hits.blocking_lock();
        *hits.entry(op).or_insert(0) += 1;
    }

    pub async fn all_hits(&self) -> HashMap<ObjectStoreOp, usize> {
        self.hits.lock().await.clone()
    }

    pub async fn count_hits(&self, op: ObjectStoreOp) -> usize {
        let hits = self.hits.lock().await;
        *hits.get(&op).unwrap_or(&0)
    }

    pub async fn reset_hits(&self) {
        let mut hits = self.hits.lock().await;
        hits.clear();
    }
}

#[async_trait]
impl ObjectStore for TestObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        self.hit(ObjectStoreOp::Put).await;
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        self.hit(ObjectStoreOp::Put).await;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        self.hit(ObjectStoreOp::Put).await;
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.hit(ObjectStoreOp::Put).await;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.hit(ObjectStoreOp::Get).await;
        self.inner.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.hit(ObjectStoreOp::Get).await;
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        self.hit(ObjectStoreOp::Get).await;
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.hit(ObjectStoreOp::Get).await;
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.hit(ObjectStoreOp::Head).await;
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.hit(ObjectStoreOp::Delete).await;
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        self.hit_blocking(ObjectStoreOp::DeleteStream);
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.hit_blocking(ObjectStoreOp::List);
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.hit_blocking(ObjectStoreOp::List);
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.hit(ObjectStoreOp::List).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.hit(ObjectStoreOp::Copy).await;
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.hit(ObjectStoreOp::Rename).await;
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.hit(ObjectStoreOp::Copy).await;
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.hit(ObjectStoreOp::Rename).await;
        self.inner.rename_if_not_exists(from, to).await
    }
}
