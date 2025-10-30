use std::marker::PhantomData;

use culprit::ResultExt;
use fjall::KvPair;
use tryiter::TryIteratorExt;

use crate::local::fjall_storage::{FjallStorageErr, fjall_repr::FjallRepr};

pub struct TypedKVIter<K, V, I> {
    pub(crate) iter: I,
    pub(crate) _phantom: PhantomData<(K, V)>,
}

impl<K, V, I> TypedKVIter<K, V, I>
where
    K: FjallRepr,
    V: FjallRepr,
    I: DoubleEndedIterator<Item = Result<KvPair, lsm_tree::Error>>,
{
    pub(crate) fn try_next(&mut self) -> culprit::Result<Option<(K, V)>, FjallStorageErr> {
        if let Some((key, val)) = self.iter.try_next()? {
            Ok(Some((
                K::try_from_slice(key).or_into_ctx()?,
                V::try_from_slice(val).or_into_ctx()?,
            )))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn try_next_back(&mut self) -> culprit::Result<Option<(K, V)>, FjallStorageErr> {
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

impl<K, V, I> Iterator for TypedKVIter<K, V, I>
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

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<K, V, I> DoubleEndedIterator for TypedKVIter<K, V, I>
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
