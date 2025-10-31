use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use culprit::ResultExt;
use fjall::{Keyspace, PartitionCreateOptions, Slice};

use crate::local::fjall_storage::{
    FjallStorageErr,
    fjall_repr::FjallRepr,
    keys::FjallKeyPrefix,
    typed_partition::{typed_key_iter::TypedKeyIter, typed_kv_iter::TypedKVIter},
};

pub mod fjall_batch_ext;
pub mod typed_key_iter;
pub mod typed_kv_iter;

#[derive(Clone)]
pub struct TypedPartition<K, V> {
    partition: fjall::Partition,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> TypedPartition<K, V>
where
    K: FjallRepr,
    V: FjallRepr,
{
    pub fn open(
        keyspace: &Keyspace,
        name: &str,
        opts: PartitionCreateOptions,
    ) -> culprit::Result<Self, FjallStorageErr> {
        Ok(Self {
            partition: keyspace.open_partition(name, opts)?,
            _phantom: PhantomData,
        })
    }

    #[inline]
    pub fn insert(&self, key: K, val: V) -> culprit::Result<(), FjallStorageErr> {
        self.partition.insert(key.into_slice(), val.into_slice())?;
        Ok(())
    }

    #[inline]
    pub fn remove(&self, key: K) -> culprit::Result<(), FjallStorageErr> {
        self.partition.remove(key.into_slice())?;
        Ok(())
    }

    #[inline]
    pub fn snapshot(&self) -> TypedPartitionSnapshot<K, V> {
        TypedPartitionSnapshot {
            snapshot: self.partition.snapshot(),
            _phantom: PhantomData,
        }
    }

    #[inline]
    pub fn snapshot_at(&self, seqno: fjall::Instant) -> TypedPartitionSnapshot<K, V> {
        TypedPartitionSnapshot {
            snapshot: self.partition.snapshot_at(seqno),
            _phantom: PhantomData,
        }
    }
}

pub struct TypedPartitionSnapshot<K, V> {
    snapshot: fjall::Snapshot,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> TypedPartitionSnapshot<K, V>
where
    K: FjallRepr,
    V: FjallRepr,
{
    /// Returns `true` if this snapshot contains the provided key
    pub fn contains(&self, key: &K) -> culprit::Result<bool, FjallStorageErr> {
        self.snapshot.contains_key(key.as_slice()).or_into_ctx()
    }

    /// Retrieve the value corresponding to the key
    pub fn get(&self, key: &K) -> culprit::Result<Option<V>, FjallStorageErr> {
        if let Some(slice) = self.snapshot.get(key.as_slice())? {
            return Ok(Some(V::try_from_slice(slice).or_into_ctx()?));
        }
        Ok(None)
    }

    /// An optimized version of get when key is owned
    pub fn get_owned(&self, key: K) -> culprit::Result<Option<V>, FjallStorageErr> {
        if let Some(slice) = self.snapshot.get(key.into_slice())? {
            return Ok(Some(V::try_from_slice(slice).or_into_ctx()?));
        }
        Ok(None)
    }

    pub fn range_keys<R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = culprit::Result<K, FjallStorageErr>> + use<R, K, V> {
        let r: (Bound<Slice>, Bound<Slice>) = (
            range.start_bound().map(|b| b.clone().into_slice()),
            range.end_bound().map(|b| b.clone().into_slice()),
        );
        TypedKeyIter::<K, _> {
            iter: self.snapshot.range(r),
            _phantom: PhantomData,
        }
    }

    pub fn range<R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = culprit::Result<(K, V), FjallStorageErr>> + use<R, K, V>
    {
        let r: (Bound<Slice>, Bound<Slice>) = (
            range.start_bound().map(|b| b.clone().into_slice()),
            range.end_bound().map(|b| b.clone().into_slice()),
        );
        TypedKVIter::<K, V, _> {
            iter: self.snapshot.range(r),
            _phantom: PhantomData,
        }
    }

    // not currently used, rename if you need it
    pub fn _prefix<'a, P>(
        &self,
        prefix: &'a P,
    ) -> impl DoubleEndedIterator<Item = culprit::Result<(K, V), FjallStorageErr>> + use<'a, P, K, V>
    where
        K: FjallKeyPrefix<Prefix = P>,
        P: AsRef<[u8]>,
    {
        TypedKVIter::<K, V, _> {
            iter: self.snapshot.prefix(prefix),
            _phantom: PhantomData,
        }
    }
}
