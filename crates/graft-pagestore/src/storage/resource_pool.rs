use std::{
    future::Future,
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

use tokio::sync::{Notify, RwLock, RwLockReadGuard};

pub struct ResourceHandle {
    slot: RwLock<SlotIdx>,
}

pub struct ResourcePoolGuard<'a, T> {
    guard: RwLockReadGuard<'a, SlotInner<T>>,
    notify_drop: Arc<Notify>,
}

impl<'a, T> Deref for ResourcePoolGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: we only return guards that have a resource
        self.guard.resource.as_ref().unwrap()
    }
}

impl<'a, T> Drop for ResourcePoolGuard<'a, T> {
    fn drop(&mut self) {
        self.notify_drop.notify_one();
    }
}

#[derive(Clone, PartialEq, Eq)]
struct SlotIdx {
    index: usize,
    generation: u64,
}

struct Slot<T> {
    recently_used: AtomicBool,
    inner: RwLock<SlotInner<T>>,
}

struct SlotInner<T> {
    resource: Option<T>,
    generation: u64,
}

pub struct ResourcePool<T> {
    slots: Box<[Slot<T>]>,

    cursor: AtomicUsize,
    slots_available: Arc<Notify>,
}

impl<T> ResourcePool<T> {
    pub fn new(size: usize) -> Self {
        let mut slots = Vec::with_capacity(size);
        for _ in 0..size {
            slots.push(Slot {
                recently_used: AtomicBool::new(false),
                inner: RwLock::new(SlotInner { resource: None, generation: 0 }),
            });
        }
        Self {
            slots: slots.into_boxed_slice(),
            cursor: AtomicUsize::new(0),
            slots_available: Default::default(),
        }
    }

    pub async fn init<F, Fut>(&self, init: F) -> (ResourceHandle, ResourcePoolGuard<'_, T>)
    where
        Fut: Future<Output = T>,
        F: FnOnce() -> Fut,
    {
        let (idx, guard) = self.init_slot(init).await;
        let handle = ResourceHandle { slot: RwLock::new(idx) };
        (handle, guard)
    }

    pub async fn get_or_init<F, Fut>(
        &self,
        handle: &ResourceHandle,
        init: F,
    ) -> ResourcePoolGuard<'_, T>
    where
        Fut: Future<Output = T>,
        F: FnOnce() -> Fut,
    {
        // take a snapshot of the handle's slot index
        let mut idx = handle.slot.read().await.clone();

        // loop until we retrieve the handle's resource or are able to init a new one
        let mut handle_guard = loop {
            // if the handle still points to the same resource, return it
            let slot = &self.slots[idx.index];
            let guard = slot.inner.read().await;
            if guard.generation == idx.generation && guard.resource.is_some() {
                slot.recently_used.store(true, Ordering::Relaxed);
                return ResourcePoolGuard {
                    guard,
                    notify_drop: self.slots_available.clone(),
                };
            }

            // the slot is not valid; we need to init a new slot
            // acquire a write lock on the handle
            let handle_guard = handle.slot.write().await;

            // if the handle has changed since we took a snapshot, retry
            if *handle_guard != idx {
                idx = handle_guard.clone();
                continue;
            }
            break handle_guard;
        };

        // at this point, we have exclusive access to the handle
        // find a slot and init the resource in it
        let (new_idx, guard) = self.init_slot(init).await;

        // update the handle to point to the new slot
        *handle_guard = new_idx;

        guard
    }

    async fn init_slot<F, Fut>(&self, init: F) -> (SlotIdx, ResourcePoolGuard<'_, T>)
    where
        Fut: Future<Output = T>,
        F: FnOnce() -> Fut,
    {
        let mut loops = 0;
        let (index, mut guard) = 'outer: loop {
            // search for an idle slot
            // if this is our first loop, we will skip recently used slots
            let mut attempts = 0;
            while attempts < self.slots.len() {
                let cursor = self.cursor.fetch_add(1, Ordering::AcqRel) % self.slots.len();
                let slot = &self.slots[cursor];

                if loops > 0 || !slot.recently_used.swap(false, Ordering::Release) {
                    if let Ok(guard) = slot.inner.try_write() {
                        // mark the slot as recently used since we are about to init it
                        slot.recently_used.store(true, Ordering::Relaxed);
                        break 'outer (cursor, guard);
                    }
                }
                attempts += 1;
            }

            loops += 1;
            self.slots_available.notified().await;
        };

        // drop the previous resource if any
        if let Some(previous) = guard.resource.take() {
            drop(previous);
        }

        // init the resource
        let resource = init().await;
        guard.resource = Some(resource);
        guard.generation += 1;

        (
            SlotIdx { index, generation: guard.generation },
            ResourcePoolGuard {
                guard: guard.downgrade(),
                notify_drop: self.slots_available.clone(),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::testutil::assert_would_timeout;

    use super::*;

    #[tokio::test]
    async fn resource_pool_sanity() {
        let pool = ResourcePool::new(2);
        let (handle1, guard1) = pool.init(|| async { 1 }).await;
        let (handle2, guard2) = pool.init(|| async { 2 }).await;

        assert_eq!(*guard1, 1);
        assert_eq!(*guard2, 2);

        // drop and reacquire the resources, ensuring they aren't reinitialized
        drop(guard1);
        drop(guard2);
        let guard1 = pool.get_or_init(&handle1, || async { 3 }).await;
        let guard2 = pool.get_or_init(&handle2, || async { 4 }).await;

        assert_eq!(*guard1, 1);
        assert_eq!(*guard2, 2);

        // drop the first guard and put a new resource in the pool
        drop(guard1);
        let (_, guard3) = pool.init(|| async { 5 }).await;

        assert_eq!(*guard3, 5);

        // attempting to acquire the first resource should timeout
        assert_would_timeout(pool.get_or_init(&handle1, || async { 1 })).await
    }
}
