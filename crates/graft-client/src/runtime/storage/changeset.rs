use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crossbeam::channel::{Receiver, Sender, TrySendError, bounded};
use parking_lot::{Mutex, RwLock};

type InnerSet<K> = Arc<RwLock<HashMap<K, AtomicU64>>>;

pub struct ChangeSet<K> {
    next_version: AtomicU64,
    subscribers: Mutex<Vec<(Option<K>, Sender<()>)>>,
    set: InnerSet<K>,
}

impl<K> Default for ChangeSet<K> {
    fn default() -> Self {
        Self {
            next_version: AtomicU64::new(0),
            subscribers: Default::default(),
            set: Default::default(),
        }
    }
}

impl<K: Eq + Hash + Clone> ChangeSet<K> {
    pub fn version(&self) -> u64 {
        self.next_version.load(Ordering::SeqCst)
    }

    fn next_version(&self) -> u64 {
        self.next_version.fetch_add(1, Ordering::SeqCst)
    }

    fn notify(&self, key: &K) {
        let mut subscribers = self.subscribers.lock();
        subscribers.retain(|(k, s)| {
            if k.as_ref().is_none_or(|k| k == key) {
                match s.try_send(()) {
                    Ok(()) => true,
                    Err(TrySendError::Full(())) => true,
                    Err(TrySendError::Disconnected(())) => false,
                }
            } else {
                true
            }
        });
    }

    /// Inserts a key into the set returning true if the set already contained the key
    pub fn insert(&self, key: K) -> bool {
        let version = self.next_version();
        let existed = self
            .set
            .write()
            .insert(key.clone(), AtomicU64::new(version))
            .is_some();
        self.notify(&key);
        existed
    }

    /// Removes a key from the set
    pub fn remove(&self, key: &K) {
        self.set.write().remove(key);
        self.notify(key);
    }

    /// Marks a key as changed
    pub fn mark_changed(&self, key: &K) {
        // optimistically assume the key exists
        let found = {
            if let Some(val) = self.set.read().get(key) {
                val.store(self.next_version(), Ordering::SeqCst);
                self.notify(key);
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

    pub fn subscribe(&self, key: K) -> Receiver<()> {
        let (tx, rx) = bounded(1);
        self.subscribers.lock().push((Some(key), tx));
        rx
    }

    pub fn subscribe_all(&self) -> SetSubscriber<K> {
        let (tx, rx) = bounded(1);
        self.subscribers.lock().push((None, tx));
        SetSubscriber {
            rx,
            version: self.version(),
            set: self.set.clone(),
        }
    }
}

pub struct SetSubscriber<K> {
    version: u64,
    rx: Receiver<()>,
    set: InnerSet<K>,
}

impl<K: Clone + Eq + Hash> SetSubscriber<K> {
    /// returns a receiver that will be notified when the set changes
    pub fn ready(&self) -> &Receiver<()> {
        &self.rx
    }

    /// returns a set of changed keys since the last time this function returned
    /// a non-empty set
    pub fn changed(&mut self) -> HashSet<K> {
        let set = self.set.read();
        let mut max_version = self.version;
        let set: HashSet<K> = set
            .iter()
            .filter_map(|(k, v)| {
                let version = v.load(Ordering::SeqCst);
                max_version = max_version.max(version);
                (version >= self.version).then_some(k.clone())
            })
            .collect();

        if !set.is_empty() {
            self.version = max_version;
        }
        set
    }
}
