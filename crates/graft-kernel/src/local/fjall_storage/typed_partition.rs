use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use bytes::Bytes;
use culprit::ResultExt;
use fjall::{Keyspace, KvPair, PartitionCreateOptions, Slice};
use graft_core::codec::Codec;
use tryiter::TryIteratorExt;

use crate::local::fjall_storage::{
    FjallStorageErr,
    keys::{FjallKey, FjallKeyPrefix},
};

pub struct TypedPartition<K, C> {
    partition: fjall::Partition,
    _phantom: PhantomData<(K, C)>,
}

impl<K, C> TypedPartition<K, C>
where
    K: FjallKey,
    C: Codec,
{
    pub fn new(
        keyspace: Keyspace,
        name: &str,
        opts: PartitionCreateOptions,
    ) -> culprit::Result<Self, FjallStorageErr> {
        Ok(Self {
            partition: keyspace.open_partition(name, opts)?,
            _phantom: PhantomData,
        })
    }

    #[inline]
    pub fn insert(&self, key: K, val: C::Message) -> culprit::Result<(), FjallStorageErr> {
        self.partition
            .insert(key.into_slice(), C::encode_to_bytes(val))?;
        Ok(())
    }

    #[inline]
    pub fn snapshot(&self) -> TypedPartitionSnapshot<K, C> {
        TypedPartitionSnapshot {
            snapshot: self.partition.snapshot(),
            _phantom: PhantomData,
        }
    }

    #[inline]
    pub fn snapshot_at(&self, seqno: fjall::Instant) -> TypedPartitionSnapshot<K, C> {
        TypedPartitionSnapshot {
            snapshot: self.partition.snapshot_at(seqno),
            _phantom: PhantomData,
        }
    }
}

pub struct TypedPartitionSnapshot<K, C> {
    snapshot: fjall::Snapshot,
    _phantom: PhantomData<(K, C)>,
}

impl<K, C> TypedPartitionSnapshot<K, C>
where
    K: FjallKey,
    C: Codec,
{
    pub fn get(&self, key: K) -> culprit::Result<Option<C::Message>, FjallStorageErr> {
        if let Some(slice) = self.snapshot.get(key.as_slice())? {
            let bytes = Bytes::from(slice);
            return Ok(Some(C::decode(bytes).or_into_ctx()?));
        }
        return Ok(None);
    }

    pub fn range<R: RangeBounds<K>>(
        &self,
        range: R,
    ) -> impl Iterator<Item = culprit::Result<(K, C::Message), FjallStorageErr>> {
        let r: (Bound<Slice>, Bound<Slice>) = (
            range.start_bound().map(|b| b.as_slice().as_ref().into()),
            range.end_bound().map(|b| b.as_slice().as_ref().into()),
        );
        TypedPartitionIter::<K, C, _> {
            iter: self.snapshot.range(r),
            _phantom: PhantomData,
        }
    }

    pub fn prefix<P>(
        &self,
        prefix: P,
    ) -> impl Iterator<Item = culprit::Result<(K, C::Message), FjallStorageErr>>
    where
        K: FjallKeyPrefix<Prefix = P>,
        P: AsRef<[u8]>,
    {
        TypedPartitionIter::<K, C, _> {
            iter: self.snapshot.prefix(prefix),
            _phantom: PhantomData,
        }
    }

    pub fn first<P>(&self, prefix: P) -> culprit::Result<Option<C::Message>, FjallStorageErr>
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

pub struct TypedPartitionIter<K, C, I> {
    iter: I,
    _phantom: PhantomData<(K, C)>,
}

impl<K, C, I> TypedPartitionIter<K, C, I>
where
    K: FjallKey,
    C: Codec,
    I: DoubleEndedIterator<Item = Result<KvPair, lsm_tree::Error>>,
{
    fn try_next(&mut self) -> culprit::Result<Option<(K, C::Message)>, FjallStorageErr> {
        if let Some((key, val)) = self.iter.try_next()? {
            Ok(Some((
                K::try_from_slice(key).or_into_ctx()?,
                C::decode(Bytes::from(val)).or_into_ctx()?,
            )))
        } else {
            Ok(None)
        }
    }

    fn try_next_back(&mut self) -> culprit::Result<Option<(K, C::Message)>, FjallStorageErr> {
        if let Some((key, val)) = self.iter.next_back().transpose()? {
            Ok(Some((
                K::try_from_slice(key).or_into_ctx()?,
                C::decode(Bytes::from(val)).or_into_ctx()?,
            )))
        } else {
            Ok(None)
        }
    }
}

impl<K, C, I> Iterator for TypedPartitionIter<K, C, I>
where
    K: FjallKey,
    C: Codec,
    I: DoubleEndedIterator<Item = Result<KvPair, lsm_tree::Error>>,
{
    type Item = culprit::Result<(K, C::Message), FjallStorageErr>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}

impl<K, C, I> DoubleEndedIterator for TypedPartitionIter<K, C, I>
where
    K: FjallKey,
    C: Codec,
    I: DoubleEndedIterator<Item = Result<KvPair, lsm_tree::Error>>,
{
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.try_next_back().transpose()
    }
}
