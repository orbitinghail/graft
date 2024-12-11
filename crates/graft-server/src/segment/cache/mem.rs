use bytes::{Buf, BufMut, Bytes, BytesMut};
use graft_core::{
    hash_table::{HTEntry, HashTable},
    SegmentId,
};
use tokio::sync::RwLock;

use super::Cache;

struct Segment {
    sid: SegmentId,
    data: Bytes,
}

impl HTEntry for Segment {
    type Key = SegmentId;

    fn key(&self) -> &Self::Key {
        &self.sid
    }
}

#[derive(Default)]
pub struct MemCache {
    /// Index of cached segments.
    segments: RwLock<HashTable<Segment>>,
}

impl Cache for MemCache {
    type Item<'a>
        = Bytes
    where
        Self: 'a;

    async fn put<T: Buf + Send + 'static>(&self, sid: &SegmentId, data: T) -> std::io::Result<()> {
        let mut segments = self.segments.write().await;
        let mut buf = BytesMut::with_capacity(data.remaining());
        buf.put(data);
        let data = buf.freeze();
        segments.insert(Segment { sid: sid.clone(), data });
        Ok(())
    }

    async fn get(&self, sid: &SegmentId) -> std::io::Result<Option<Self::Item<'_>>> {
        let segments = self.segments.read().await;
        Ok(segments.find(sid).map(|s| s.data.clone()))
    }
}
