use std::marker::PhantomData;

use zerocopy::{FromBytes, Immutable};

use crate::{bitmap::Bitmap, block::Block, index::IndexRef, Segment};

pub trait FromSuffix<'a> {
    fn from_suffix(data: &'a [u8], cardinality: usize) -> Self;
}

/// A custom version of ToOwned to get around a conflict with the standard
/// library's `impl<T> ToOwned for T where T: Clone` and BlockRef.
pub trait CopyToOwned {
    type Owned;

    fn copy_to_owned(&self) -> Self::Owned;
}

pub struct Partition<V> {
    index: Block,
    values: Vec<V>,
}

impl<V> Partition<V> {
    pub fn lookup(&self, segment: Segment) -> Option<&V> {
        if self.index.contains(segment) {
            let rank = self.index.rank(segment);
            self.values.get(rank - 1)
        } else {
            None
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &V> {
        self.values.iter()
    }
}

pub struct PartitionRef<'a, Offset, V> {
    values: &'a [u8],
    index: IndexRef<'a, Offset>,
    _phantom: PhantomData<V>,
}

impl<'a, Offset, V> FromSuffix<'a> for PartitionRef<'a, Offset, V>
where
    Offset: FromBytes + Immutable + Copy + Into<u32>,
{
    fn from_suffix(data: &'a [u8], cardinality: usize) -> Self {
        let (values, index) = IndexRef::from_suffix(data, cardinality);
        Self { values, index, _phantom: PhantomData }
    }
}

impl<'a, Offset, V> CopyToOwned for PartitionRef<'a, Offset, V>
where
    Offset: FromBytes + Immutable + Copy + Into<u32>,
    V: CopyToOwned + FromSuffix<'a>,
{
    type Owned = Partition<V::Owned>;

    fn copy_to_owned(&self) -> Self::Owned {
        let index = self.index.keys().copy_to_owned();
        let values = self.iter().map(|v| v.copy_to_owned()).collect::<Vec<_>>();
        Partition { index, values }
    }
}

impl<'a, Offset, V> PartitionRef<'a, Offset, V>
where
    Offset: FromBytes + Immutable + Copy + Into<u32>,
    V: FromSuffix<'a>,
{
    pub fn lookup(&self, segment: Segment) -> Option<V> {
        if let Some((cardinality, offset)) = self.index.lookup(segment) {
            assert!(self.values.len() >= offset, "offset out of range");
            let data = &self.values[..(self.values.len() - offset)];
            Some(V::from_suffix(data, cardinality))
        } else {
            None
        }
    }

    fn get(&self, index: usize) -> Option<V> {
        if let Some((cardinality, offset)) = self.index.get(index) {
            assert!(self.values.len() >= offset, "offset out of range");
            let data = &self.values[..(self.values.len() - offset)];
            Some(V::from_suffix(data, cardinality))
        } else {
            None
        }
    }

    /// returns the number of entries in the partition
    #[inline]
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// returns the cardinality of the partition by summing the cardinalities
    /// stored in the partition's index
    #[inline]
    pub fn cardinality(&self) -> usize {
        self.index.cardinality()
    }

    #[inline]
    pub fn iter(&self) -> PartitionRefIter<'_, 'a, Offset, V> {
        PartitionRefIter { inner: self, cursor: 0 }
    }
}

pub struct PartitionRefIter<'p, 'a, Offset, V> {
    inner: &'p PartitionRef<'a, Offset, V>,
    cursor: usize,
}

impl<'p, 'a, Offset, V> Iterator for PartitionRefIter<'p, 'a, Offset, V>
where
    V: FromSuffix<'a>,
    Offset: FromBytes + Immutable + Copy + Into<u32>,
{
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.inner.get(self.cursor);
        self.cursor += 1;
        result
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.inner.len().saturating_sub(self.cursor);
        (remaining, Some(remaining))
    }
}
