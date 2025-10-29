use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    fmt::Debug,
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use async_stream::stream;
use event_listener::Event;
use futures::Stream;
use parking_lot::RwLock;

type InnerSet<K> = RwLock<HashMap<K, AtomicU64>>;

#[derive(Debug, Default)]
pub struct ChangeSet<K> {
    inner: Arc<Inner<K>>,
}

#[derive(Debug)]
struct Inner<K> {
    next_version: AtomicU64,
    event: Event,
    set: InnerSet<K>,
}

impl<K> Default for Inner<K> {
    fn default() -> Self {
        Self {
            next_version: AtomicU64::new(1),
            event: Default::default(),
            set: Default::default(),
        }
    }
}

impl<K: Eq + Hash + Clone + Debug> ChangeSet<K> {
    pub fn new() -> Self {
        Self { inner: Default::default() }
    }

    pub fn mark_changed(&self, key: &K) {
        // optimistically assume we've already seen this volume
        {
            let guard = self.inner.set.read();
            if let Some(version) = guard.get(key) {
                version.fetch_max(self.inner.next_version(), Ordering::SeqCst);
                self.inner.event.notify(usize::MAX);
                return;
            }
        }

        // fallback to inserting the key
        let mut guard = self.inner.set.write();
        match guard.entry(key.clone()) {
            Entry::Occupied(v) => {
                v.get()
                    .fetch_max(self.inner.next_version(), Ordering::SeqCst);
            }
            Entry::Vacant(v) => {
                v.insert(self.inner.next_version().into());
            }
        };
        self.inner.event.notify(usize::MAX);
    }

    pub fn subscribe_all(&self) -> impl Stream<Item = HashSet<K>> + use<K> {
        let inner = self.inner.clone();
        let mut version = 0;

        stream! {
            loop {
                // acquire a listener
                let listener = inner.event.listen();

                // if the version hasn't changed yet, then wait
                if inner.version() == version {
                    listener.await;
                }

                // build and yield the changes
                yield inner.changes_since(&mut version).await;
            }
        }
    }
}

impl<K: Eq + Hash + Clone + Debug> Inner<K> {
    fn version(&self) -> u64 {
        // SAFETY: self.next_version starts at 1
        self.next_version.load(Ordering::Acquire) - 1
    }

    fn next_version(&self) -> u64 {
        self.next_version.fetch_add(1, Ordering::SeqCst)
    }

    async fn changes_since(&self, version: &mut u64) -> HashSet<K> {
        let min_version = *version;
        self.set
            .read()
            .iter()
            .filter_map(|(k, v)| {
                let v = v.load(Ordering::Acquire);
                if v >= min_version {
                    *version = v.max(*version);
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use futures::{FutureExt, StreamExt};

    use super::*;

    #[graft_test::test]
    fn test_changeset() {
        let set = ChangeSet::new();
        let mut sub = Box::pin(set.subscribe_all());

        set.mark_changed(&1);

        // we use now_or_never rather than async/await to test exactly when and
        // how the changeset yields a new set
        assert_eq!(
            sub.next().now_or_never(),
            Some(Some(HashSet::from_iter([1])))
        );
        assert_eq!(sub.next().now_or_never(), None);

        set.mark_changed(&1);
        set.mark_changed(&2);

        assert_eq!(
            sub.next().now_or_never(),
            Some(Some(HashSet::from_iter([1, 2])))
        );
        assert_eq!(sub.next().now_or_never(), None);
    }
}
