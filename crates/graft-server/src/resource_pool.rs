use std::{
    future::Future,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use tokio::sync::{Notify, RwLock, RwLockReadGuard};

#[derive(Default)]
pub struct ResourceHandle {
    slot: RwLock<SlotIdx>,
}

#[derive(Clone, PartialEq, Eq, Default)]
enum SlotIdx {
    #[default]
    Uninit,
    Init {
        index: usize,
        generation: u64,
    },
}

struct Slot<T> {
    recently_used: AtomicBool,
    inner: RwLock<SlotInner<T>>,
}

struct SlotInner<T> {
    resource: Option<T>,
    generation: u64,
}

pub struct ResourcePoolGuard<'a, T> {
    guard: RwLockReadGuard<'a, SlotInner<T>>,
    notify_drop: Arc<Notify>,
}

impl<T> Deref for ResourcePoolGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: we only return guards that have a resource
        self.guard.resource.as_ref().unwrap()
    }
}

impl<T> Drop for ResourcePoolGuard<'_, T> {
    fn drop(&mut self) {
        self.notify_drop.notify_waiters();
    }
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

    pub async fn get<F, Fut, E>(
        &self,
        handle: &ResourceHandle,
        init: F,
    ) -> Result<ResourcePoolGuard<'_, T>, E>
    where
        Fut: Future<Output = Result<T, E>>,
        F: FnOnce() -> Fut,
    {
        // take a snapshot of the handle's slot index
        let mut idx = handle.slot.read().await.clone();

        // loop until we retrieve the handle's resource or are able to init a new one
        let mut handle_guard = loop {
            // if the handle points at a slot and the generation is valid, return the resource
            if let SlotIdx::Init { index, generation } = idx {
                let slot = &self.slots[index];
                let guard = slot.inner.read().await;
                if guard.generation == generation && guard.resource.is_some() {
                    slot.recently_used.store(true, Ordering::Relaxed);
                    return Ok(ResourcePoolGuard {
                        guard,
                        notify_drop: self.slots_available.clone(),
                    });
                }
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
        let (new_idx, guard) = self.init_slot(init).await?;

        // update the handle to point to the new slot
        *handle_guard = new_idx;

        // the handle should be initialized
        debug_assert!(matches!(*handle_guard, SlotIdx::Init { .. }));

        Ok(guard)
    }

    async fn init_slot<F, Fut, E>(&self, init: F) -> Result<(SlotIdx, ResourcePoolGuard<'_, T>), E>
    where
        Fut: Future<Output = Result<T, E>>,
        F: FnOnce() -> Fut,
    {
        // track the number of attempts to acquire a slot
        let mut attempts = 0;

        let (index, mut guard) = 'outer: loop {
            // acquire a notify guard to avoid a race between a slot becoming
            // available after we check it
            let notify_guard = self.slots_available.notified();

            let mut found_recently_used = false;

            // search for an idle slot
            for _ in 0..self.slots.len() {
                let cursor = self.cursor.fetch_add(1, Ordering::AcqRel) % self.slots.len();
                let slot = &self.slots[cursor];

                let recently_used = slot.recently_used.swap(false, Ordering::Release);
                found_recently_used |= recently_used;

                // on the first pass, only consider slots that are not recently used
                // on subsequent passes, consider all slots
                if (attempts > 0 || !recently_used)
                    && let Ok(guard) = slot.inner.try_write()
                {
                    // mark the slot as recently used since we are about to init it
                    slot.recently_used.store(true, Ordering::Relaxed);
                    break 'outer (cursor, guard);
                }
            }

            attempts += 1;

            // if we were blocked by a recently_used slot, immediately retry
            if found_recently_used {
                continue;
            }

            // wait for a slot to become available
            notify_guard.await;
        };

        // drop the previous resource if any
        if let Some(previous) = guard.resource.take() {
            drop(previous);
        }

        // init the resource
        let resource = init().await?;
        guard.resource = Some(resource);
        guard.generation += 1;

        Ok((
            SlotIdx::Init { index, generation: guard.generation },
            ResourcePoolGuard {
                guard: guard.downgrade(),
                notify_drop: self.slots_available.clone(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;

    use tokio::task::yield_now;

    use crate::testutil::assert_would_timeout;

    use super::*;

    fn must_ok<T>(v: T) -> Result<T, Infallible> {
        Ok(v)
    }

    #[graft_test::test]
    async fn resource_pool_sanity() {
        let pool = ResourcePool::new(2);
        let handle1 = ResourceHandle::default();
        let handle2 = ResourceHandle::default();

        let guard1 = pool.get(&handle1, || async { must_ok(1) }).await.unwrap();
        let guard2 = pool.get(&handle2, || async { must_ok(2) }).await.unwrap();

        assert_eq!(*guard1, 1);
        assert_eq!(*guard2, 2);

        // drop and reacquire the resources, ensuring they aren't reinitialized
        drop(guard1);
        drop(guard2);
        let guard1 = pool.get(&handle1, || async { must_ok(3) }).await.unwrap();
        let guard2 = pool.get(&handle2, || async { must_ok(4) }).await.unwrap();

        assert_eq!(*guard1, 1);
        assert_eq!(*guard2, 2);

        // drop the first guard and put a new resource in the pool
        drop(guard1);
        let handle3 = ResourceHandle::default();
        let guard3 = pool.get(&handle3, || async { must_ok(5) }).await.unwrap();

        assert_eq!(*guard3, 5);

        // attempting to acquire the first resource should timeout as there are
        // no slots available
        assert_would_timeout(pool.get(&handle1, || async { must_ok(1) })).await
    }

    #[graft_test::test]
    async fn resource_pool_concurrency() {
        let pool = Arc::new(ResourcePool::new(2));

        let tasks = (0..10)
            .map(|i| {
                let pool = pool.clone();
                tokio::spawn(async move {
                    for j in 0..1000 {
                        let v = format!("{i}-{j}");
                        let handle = ResourceHandle::default();

                        {
                            let guard = pool
                                .get(&handle, || async { must_ok(v.clone()) })
                                .await
                                .unwrap();
                            assert_eq!(*guard, v);
                        }

                        yield_now().await;

                        {
                            let guard = pool
                                .get(&handle, || async { must_ok(v.clone()) })
                                .await
                                .unwrap();
                            assert_eq!(*guard, v);
                        }

                        yield_now().await;
                    }
                })
            })
            .collect::<Vec<_>>();

        for task in tasks {
            task.await.unwrap();
        }
    }
}
