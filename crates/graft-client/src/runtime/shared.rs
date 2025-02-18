use std::sync::Arc;

use graft_core::gid::ClientId;

use super::{fetcher::Fetcher, storage::Storage};

#[derive(Debug, Clone)]
pub struct Shared {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    cid: ClientId,
    fetcher: Box<dyn Fetcher>,
    storage: Storage,
}

impl Shared {
    #[inline]
    pub fn new(cid: ClientId, fetcher: Box<dyn Fetcher>, storage: Storage) -> Self {
        Self {
            inner: Arc::new(Inner { cid, fetcher, storage }),
        }
    }

    #[inline]
    pub fn cid(&self) -> &ClientId {
        &self.inner.cid
    }

    #[inline]
    pub fn fetcher(&self) -> &dyn Fetcher {
        self.inner.fetcher.as_ref()
    }

    #[inline]
    pub fn storage(&self) -> &Storage {
        &self.inner.storage
    }
}
