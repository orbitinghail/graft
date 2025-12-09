use std::{
    borrow::Borrow,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use fjall::{Keyspace, PartitionCreateOptions, Slice};
use tryiter::TryIteratorExt;

use crate::local::fjall_storage::{
    FjallStorageErr,
    fjall_repr::{FjallRepr, FjallReprRef},
    keys::FjallKeyPrefix,
};

pub mod fjall_batch_ext;

type Result<T> = std::result::Result<T, FjallStorageErr>;

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
    pub fn open(keyspace: &Keyspace, name: &str, opts: PartitionCreateOptions) -> Result<Self> {
        Ok(Self {
            partition: keyspace.open_partition(name, opts)?,
            _phantom: PhantomData,
        })
    }

    #[inline]
    pub fn insert(&self, key: K, val: V) -> Result<()> {
        self.partition.insert(key.into_slice(), val.into_slice())?;
        Ok(())
    }

    #[inline]
    pub fn remove(&self, key: K) -> Result<()> {
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
    pub fn contains<B>(&self, key: &B) -> Result<bool>
    where
        B: FjallReprRef + ?Sized,
        K: Borrow<B>,
    {
        Ok(self.snapshot.contains_key(key.as_slice())?)
    }

    /// Retrieve the value corresponding to the key
    pub fn get<B>(&self, key: &B) -> Result<Option<V>>
    where
        B: FjallReprRef + ?Sized,
        K: Borrow<B>,
    {
        if let Some(slice) = self.snapshot.get(key.as_slice())? {
            return Ok(Some(V::try_from_slice(slice)?));
        }
        Ok(None)
    }

    /// An optimized version of get when key is owned
    pub fn get_owned(&self, key: K) -> Result<Option<V>> {
        if let Some(slice) = self.snapshot.get(key.into_slice())? {
            return Ok(Some(V::try_from_slice(slice)?));
        }
        Ok(None)
    }

    pub fn range_keys<R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl Iterator<Item = Result<K>> + use<R, K, V> {
        let r: (Bound<Slice>, Bound<Slice>) = (
            range.start_bound().map(|b| b.clone().into_slice()),
            range.end_bound().map(|b| b.clone().into_slice()),
        );
        self.snapshot
            .range(r)
            .err_into::<FjallStorageErr>()
            .map_ok(|(k, _)| Ok(K::try_from_slice(k)?))
    }

    pub fn range<R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl Iterator<Item = Result<(K, V)>> + use<R, K, V> {
        let r: (Bound<Slice>, Bound<Slice>) = (
            range.start_bound().map(|b| b.clone().into_slice()),
            range.end_bound().map(|b| b.clone().into_slice()),
        );
        self.snapshot
            .range(r)
            .err_into::<FjallStorageErr>()
            .map_ok(|(k, v)| Ok((K::try_from_slice(k)?, V::try_from_slice(v)?)))
    }

    /// iterate all of the values in the partition
    pub fn values(&self) -> impl Iterator<Item = Result<V>> + use<K, V> {
        self.snapshot
            .values()
            .err_into::<FjallStorageErr>()
            .map_ok(|v| Ok(V::try_from_slice(v)?))
    }

    pub fn prefix<'a, P>(
        &self,
        prefix: &'a P,
    ) -> impl Iterator<Item = Result<(K, V)>> + use<'a, P, K, V>
    where
        K: FjallKeyPrefix<Prefix = P>,
        P: AsRef<[u8]>,
    {
        self.snapshot
            .prefix(prefix)
            .err_into::<FjallStorageErr>()
            .map_ok(|(k, v)| Ok((K::try_from_slice(k)?, V::try_from_slice(v)?)))
    }

    pub fn first<P>(&self, prefix: &P) -> Result<Option<(K, V)>>
    where
        K: FjallKeyPrefix<Prefix = P>,
        P: AsRef<[u8]>,
    {
        self.prefix(prefix).try_next()
    }
}
