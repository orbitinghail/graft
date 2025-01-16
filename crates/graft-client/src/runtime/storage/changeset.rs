use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use tokio::sync::{Notify, RwLock};

pub struct ChangeSet<K> {
    version: AtomicU64,
    inner: Arc<Inner<K>>,
}

impl<K> Default for ChangeSet<K> {
    fn default() -> Self {
        Self {
            version: AtomicU64::new(0),
            inner: Arc::new(Inner {
                set: Default::default(),
                notify: Notify::new(),
            }),
        }
    }
}

#[derive(Default)]
struct Inner<K> {
    set: RwLock<HashMap<K, AtomicU64>>,
    notify: Notify,
}

impl<K: Eq + Hash + Clone> ChangeSet<K> {
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    fn next_version(&self) -> u64 {
        self.version.fetch_add(1, Ordering::SeqCst)
    }

    /// Inserts a key into the set returning true if the set already contained the key
    pub fn insert(&self, key: K) -> bool {
        let version = self.next_version();
        let existed = self
            .inner
            .set
            .blocking_write()
            .insert(key, AtomicU64::new(version))
            .is_some();
        self.inner.notify.notify_waiters();
        existed
    }

    /// Removes a key from the set
    pub fn remove(&self, key: &K) {
        self.inner.set.blocking_write().remove(key);
        self.inner.notify.notify_waiters();
    }

    /// Marks a key as changed
    pub fn mark_changed(&self, key: &K) {
        // optimistically assume the key exists
        let found = {
            if let Some(val) = self.inner.set.blocking_read().get(&key) {
                val.store(self.next_version(), Ordering::SeqCst);
                self.inner.notify.notify_waiters();
                true
            } else {
                false
            }
        };

        // fallback to inserting the key
        if !found {
            self.insert(key.clone());
        }
    }

    pub fn subscribe(&self, key: K) -> Subscriber<K> {
        Subscriber {
            key,
            version: self.version(),
            inner: self.inner.clone(),
        }
    }

    pub fn subscribe_all(&self) -> SetSubscriber<K> {
        SetSubscriber {
            version: self.version(),
            inner: self.inner.clone(),
        }
    }
}

pub struct Subscriber<K> {
    key: K,
    version: u64,
    inner: Arc<Inner<K>>,
}

impl<K: Eq + Hash> Subscriber<K> {
    /// This future resolves when the subscriber's key has changed
    pub async fn changed(&mut self) {
        loop {
            // allocate a notify future to ensure we don't miss a notification
            let notify = self.inner.notify.notified();

            // load the key's version
            if let Some(version) = self.load_version().await {
                // if the version has changed, advance and return
                if version > self.version {
                    self.version = version;
                    break;
                }
            }

            // wait for the next change notification
            notify.await;
        }
    }

    async fn load_version(&self) -> Option<u64> {
        self.inner
            .set
            .read()
            .await
            .get(&self.key)
            .map(|v| v.load(Ordering::SeqCst))
    }
}

pub struct SetSubscriber<K> {
    version: u64,
    inner: Arc<Inner<K>>,
}

impl<K: Clone + Eq + Hash> SetSubscriber<K> {
    /// This future resolves when any key has changed, and returns a set of all
    /// changed keys. This future ignores deleted keys.
    pub async fn changed(&mut self) -> HashSet<K> {
        loop {
            // allocate a notify future to ensure we don't miss a notification
            let notify = self.inner.notify.notified();

            // load changed keys
            let (max_version, keys) = self.load_changed().await;
            if !keys.is_empty() {
                self.version = max_version;
                return keys;
            }

            // wait for the next change notification
            notify.await;
        }
    }

    async fn load_changed(&self) -> (u64, HashSet<K>) {
        let set = self.inner.set.read().await;
        let mut max_version = self.version;
        let set: HashSet<K> = set
            .iter()
            .filter_map(|(k, v)| {
                let version = v.load(Ordering::SeqCst);
                max_version = max_version.max(version);
                (version > self.version).then_some(k.clone())
            })
            .collect();
        (max_version, set)
    }
}
