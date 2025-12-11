use std::{borrow::Borrow, marker::PhantomData, ops::RangeInclusive};

use fjall::{Database, Guard, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, Readable};

use crate::local::fjall_storage::{
    FjallStorageErr,
    fjall_repr::{FjallRepr, FjallReprRef},
    keys::FjallKeyPrefix,
};

type Result<T> = std::result::Result<T, FjallStorageErr>;

#[derive(Clone)]
pub struct TypedKeyspace<K, V>
where
    K: FjallRepr,
    V: FjallRepr,
{
    keyspace: Keyspace,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> TypedKeyspace<K, V>
where
    K: FjallRepr,
    V: FjallRepr,
{
    pub fn open(
        db: &Database,
        name: &str,
        opts: impl FnOnce() -> KeyspaceCreateOptions,
    ) -> Result<Self> {
        Ok(Self {
            keyspace: db.keyspace(name, opts)?,
            _phantom: PhantomData,
        })
    }

    fn inner(&self) -> &Keyspace {
        &self.keyspace
    }

    /// Insert a key and value into this Keyspace
    pub fn insert(&self, key: K, value: V) -> Result<()> {
        Ok(self.keyspace.insert(key.into_slice(), value.into_slice())?)
    }

    /// Remove a key from this Keyspace
    pub fn remove(&self, key: K) -> Result<()> {
        Ok(self.keyspace.remove(key.into_slice())?)
    }
}

pub trait WriteBatchExt<K, V>
where
    K: FjallRepr,
    V: FjallRepr,
{
    fn insert_typed(&mut self, ks: &TypedKeyspace<K, V>, key: K, val: V);
    fn remove_typed(&mut self, ks: &TypedKeyspace<K, V>, key: K);
}

impl<K, V> WriteBatchExt<K, V> for OwnedWriteBatch
where
    K: FjallRepr,
    V: FjallRepr,
{
    fn insert_typed(&mut self, ks: &TypedKeyspace<K, V>, key: K, val: V) {
        self.insert(ks.inner(), key.into_slice(), val.into_slice());
    }

    fn remove_typed(&mut self, ks: &TypedKeyspace<K, V>, key: K) {
        self.remove(ks.inner(), key.into_slice());
    }
}

/// Extension traits for Fjall Readable types to support typed keyspaces.
pub trait ReadableExt<K, V>: Readable
where
    K: FjallRepr,
    V: FjallRepr,
{
    /// Retrieve the value corresponding to the key
    fn get<B>(&self, ks: &TypedKeyspace<K, V>, key: &B) -> Result<Option<V>>
    where
        B: FjallReprRef + ?Sized,
        K: Borrow<B>,
    {
        if let Some(slice) = Readable::get(self, ks.inner(), key.as_slice())? {
            return Ok(Some(V::try_from_slice(slice)?));
        }
        Ok(None)
    }

    /// An optimized version of get when key is owned
    fn get_owned(&self, ks: &TypedKeyspace<K, V>, key: K) -> Result<Option<V>> {
        if let Some(slice) = Readable::get(self, ks.inner(), key.into_slice())? {
            return Ok(Some(V::try_from_slice(slice)?));
        }
        Ok(None)
    }

    /// Returns `true` if this Readable contains the provided key
    fn contains_key<B>(&self, ks: &TypedKeyspace<K, V>, key: &B) -> Result<bool>
    where
        B: FjallReprRef + ?Sized,
        K: Borrow<B>,
    {
        Ok(Readable::contains_key(self, ks.inner(), key.as_slice())?)
    }

    /// Returns an iterator over the entire keyspace
    fn iter(&self, ks: &TypedKeyspace<K, V>) -> TypedIter<K, V> {
        let inner = Readable::iter(self, ks.inner());
        TypedIter { inner, _phantom: PhantomData }
    }

    /// Returns an iterator over the provided range
    fn range(&self, ks: &TypedKeyspace<K, V>, range: RangeInclusive<K>) -> TypedIter<K, V> {
        let (start, end) = range.into_inner();
        let r = start.into_slice()..=end.into_slice();
        let inner = Readable::range(self, ks.inner(), r);
        TypedIter { inner, _phantom: PhantomData }
    }

    /// Returns an iterator over keys which start with the provided prefix
    fn prefix<P>(&self, ks: &TypedKeyspace<K, V>, prefix: &P) -> TypedIter<K, V>
    where
        K: FjallKeyPrefix<Prefix = P>,
        P: AsRef<[u8]>,
    {
        let inner = Readable::prefix(self, ks.inner(), prefix);
        TypedIter { inner, _phantom: PhantomData }
    }
}

/// Blanket implementation for all Readable types
impl<R: Readable, K: FjallRepr, V: FjallRepr> ReadableExt<K, V> for R {}

/// An iterator of Keys and Values in a Fjall Keyspace
#[must_use]
pub struct TypedIter<K, V> {
    inner: fjall::Iter,
    _phantom: PhantomData<(K, V)>,
}

/// An iterator of Keys in a Fjall Keyspace
#[must_use]
pub struct TypedKeyIter<K, V> {
    inner: fjall::Iter,
    _phantom: PhantomData<(K, V)>,
}

/// An iterator of Values in a Fjall Keyspace
#[must_use]
pub struct TypedValIter<K, V> {
    inner: fjall::Iter,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> TypedIter<K, V> {
    /// Convert this iterator into an iterator of only keys
    pub fn keys(self) -> TypedKeyIter<K, V> {
        TypedKeyIter { inner: self.inner, _phantom: PhantomData }
    }

    /// Convert this iterator into an iterator of only values
    pub fn values(self) -> TypedValIter<K, V> {
        TypedValIter { inner: self.inner, _phantom: PhantomData }
    }
}

macro_rules! impl_iter {
    ($iter:ident, $item:ty, |$guard:ident| $body:block) => {
        impl<K: FjallRepr, V: FjallRepr> Iterator for $iter<K, V> {
            type Item = Result<$item>;
            fn next(&mut self) -> Option<Self::Item> {
                let mapper = |$guard: Guard| $body;
                if let Some(guard) = self.inner.next() {
                    Some(mapper(guard))
                } else {
                    None
                }
            }
        }
        impl<K: FjallRepr, V: FjallRepr> DoubleEndedIterator for $iter<K, V> {
            fn next_back(&mut self) -> Option<Self::Item> {
                let mapper = |$guard: Guard| $body;
                if let Some(guard) = self.inner.next_back() {
                    Some(mapper(guard))
                } else {
                    None
                }
            }
        }
    };
}

impl_iter!(TypedIter, (K, V), |guard| {
    let (k, v) = guard.into_inner()?;
    Ok((K::try_from_slice(k)?, V::try_from_slice(v)?))
});

impl_iter!(TypedKeyIter, K, |guard| {
    let k = guard.key()?;
    Ok(K::try_from_slice(k)?)
});

impl_iter!(TypedValIter, V, |guard| {
    let v = guard.value()?;
    Ok(V::try_from_slice(v)?)
});
