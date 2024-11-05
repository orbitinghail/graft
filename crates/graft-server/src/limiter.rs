use std::{
    hash::{BuildHasher, Hash},
    marker::PhantomData,
};
use tokio::sync::{Mutex, MutexGuard};

pub struct LimiterPermit<'a> {
    _permit: MutexGuard<'a, ()>,
}

/// Limiter acts like a keyed semaphore. In addition to limiting the total
/// number of permits available, it also ensures that only one permit is
/// available per unique key.
///
/// The current implementation allocates a fixed number of permits which are
/// shared by all possible keys. This means that it's possible that two
/// concurrent acquires for different keys may attempt to acquire the same
/// permit. This decision may be revisited in the future.
pub struct Limiter<K, H> {
    permits: Box<[Mutex<()>]>,
    hasher: H,
    _phantom: PhantomData<K>,
}

impl<K: Hash, H: BuildHasher + Default> Limiter<K, H> {
    pub fn new(permits: usize) -> Self {
        Self::new_with_hasher(permits, Default::default())
    }
}

impl<K: Hash, H: BuildHasher> Limiter<K, H> {
    pub fn new_with_hasher(permits: usize, hasher: H) -> Self {
        let permits = (0..permits)
            .map(|_| Mutex::new(()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { permits, hasher, _phantom: PhantomData }
    }

    pub async fn acquire(&self, key: &K) -> LimiterPermit<'_> {
        let idx = self.hasher.hash_one(key) % self.permits.len() as u64;
        let _permit = self.permits[idx as usize].lock().await;
        LimiterPermit { _permit }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use foldhash::fast::FixedState;
    use tokio::time::Duration;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn test_limiter() {
        let hasher = FixedState::with_seed(12345);
        let limiter = Arc::new(Limiter::<usize, _>::new_with_hasher(10, hasher));
        let concurrent_count = Arc::new(AtomicUsize::new(0));

        async fn task(
            id: usize,
            key: usize,
            limiter: Arc<Limiter<usize, FixedState>>,
            concurrent_count: Arc<AtomicUsize>,
        ) {
            println!("task {} acquiring permit", id);
            let _permit = limiter.acquire(&key).await;
            println!("task {} acquired permit", id);
            concurrent_count.fetch_add(1, Ordering::Relaxed);
            assert!(concurrent_count.load(Ordering::Relaxed) <= 2);
            tokio::time::sleep(Duration::from_secs(1)).await;
            concurrent_count.fetch_sub(1, Ordering::Relaxed);
            println!("task {} released permit", id);
        }

        // run 10 tasks to completion
        let tasks = (0..10)
            .map(|i| task(i, i % 2, limiter.clone(), concurrent_count.clone()))
            .collect::<Vec<_>>();
        futures::future::join_all(tasks).await;
    }
}
