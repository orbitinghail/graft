use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use culprit::ResultExt;
use fjall::{Keyspace, KvPair, PartitionCreateOptions, Slice};
use tryiter::TryIteratorExt;

use crate::local::fjall_storage::{FjallStorageErr, fjall_repr::FjallRepr, keys::FjallKeyPrefix};

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
    pub fn get(&self, key: &K) -> culprit::Result<Option<V>, FjallStorageErr> {
        if let Some(slice) = self.snapshot.get(key.as_slice())? {
            return Ok(Some(V::try_from_slice(slice).or_into_ctx()?));
        }
        return Ok(None);
    }

    pub fn range<R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = culprit::Result<(K, V), FjallStorageErr>> + use<R, K, V>
    {
        let r: (Bound<Slice>, Bound<Slice>) = (
            range.start_bound().map(|b| b.as_slice().as_ref().into()),
            range.end_bound().map(|b| b.as_slice().as_ref().into()),
        );
        TypedPartitionIter::<K, V, _> {
            iter: self.snapshot.range(r),
            _phantom: PhantomData,
        }
    }

    pub fn prefix<P>(
        &self,
        prefix: &P,
    ) -> impl DoubleEndedIterator<Item = culprit::Result<(K, V), FjallStorageErr>>
    where
        K: FjallKeyPrefix<Prefix = P>,
        P: AsRef<[u8]>,
    {
        TypedPartitionIter::<K, V, _> {
            iter: self.snapshot.prefix(prefix),
            _phantom: PhantomData,
        }
    }

    pub fn first<P>(&self, prefix: &P) -> culprit::Result<Option<V>, FjallStorageErr>
    where
        K: FjallKeyPrefix<Prefix = P>,
        P: AsRef<[u8]>,
    {
        if let Some((_, v)) = self.prefix(prefix).try_next()? {
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }
}

pub struct TypedPartitionIter<K, V, I> {
    iter: I,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V, I> TypedPartitionIter<K, V, I>
where
    K: FjallRepr,
    V: FjallRepr,
    I: DoubleEndedIterator<Item = Result<KvPair, lsm_tree::Error>>,
{
    fn try_next(&mut self) -> culprit::Result<Option<(K, V)>, FjallStorageErr> {
        if let Some((key, val)) = self.iter.try_next()? {
            Ok(Some((
                K::try_from_slice(key).or_into_ctx()?,
                V::try_from_slice(val).or_into_ctx()?,
            )))
        } else {
            Ok(None)
        }
    }

    fn try_next_back(&mut self) -> culprit::Result<Option<(K, V)>, FjallStorageErr> {
        if let Some((key, val)) = self.iter.next_back().transpose()? {
            Ok(Some((
                K::try_from_slice(key).or_into_ctx()?,
                V::try_from_slice(val).or_into_ctx()?,
            )))
        } else {
            Ok(None)
        }
    }
}

impl<K, V, I> Iterator for TypedPartitionIter<K, V, I>
where
    K: FjallRepr,
    V: FjallRepr,
    I: DoubleEndedIterator<Item = Result<KvPair, lsm_tree::Error>>,
{
    type Item = culprit::Result<(K, V), FjallStorageErr>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<K, V, I> DoubleEndedIterator for TypedPartitionIter<K, V, I>
where
    K: FjallRepr,
    V: FjallRepr,
    I: DoubleEndedIterator<Item = Result<KvPair, lsm_tree::Error>>,
{
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}
