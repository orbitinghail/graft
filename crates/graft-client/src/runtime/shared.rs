use std::sync::Arc;

use super::storage::Storage;

#[derive(Debug)]
pub struct Shared<F> {
    inner: Arc<Inner<F>>,
}

#[derive(Debug)]
struct Inner<F> {
    fetcher: F,
    storage: Storage,
}

impl<F> Shared<F> {
    #[inline]
    pub fn new(fetcher: F, storage: Storage) -> Self {
        Self {
            inner: Arc::new(Inner { fetcher, storage }),
        }
    }

    #[inline]
    pub fn fetcher(&self) -> &F {
        &self.inner.fetcher
    }

    #[inline]
    pub fn storage(&self) -> &Storage {
        &self.inner.storage
    }
}

impl<F> Clone for Shared<F> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}
