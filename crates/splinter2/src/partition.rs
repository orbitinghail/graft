use std::{convert::TryInto, fmt::Debug, marker::PhantomData, mem::size_of};

use bytes::BufMut;
use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::{
    bitmap::BitmapExt,
    block::Block,
    index::IndexRef,
    util::{CopyToOwned, FromSuffix, SerializeContainer},
    Segment,
};

pub struct Partition<Offset, V> {
    index: Block,
    values: Vec<V>,
    _phantom: PhantomData<Offset>,
}

impl<O, V> Partition<O, V> {
    pub fn iter(&self) -> impl Iterator<Item = &V> {
        self.values.iter()
    }

    pub fn get(&self, segment: Segment) -> Option<&V> {
        if self.index.contains(segment) {
            let rank = self.index.rank(segment);
            self.values.get(rank - 1)
        } else {
            None
        }
    }

    /// returns the number of entries in the partition
    #[inline]
    pub fn len(&self) -> usize {
        self.values.len()
    }
}

impl<O, V> Partition<O, V>
where
    V: Default,
{
    pub fn get_or_init(&mut self, segment: Segment) -> &mut V {
        let needs_init = self.index.insert(segment);
        let index = self.index.rank(segment) - 1;
        if needs_init {
            self.values.insert(index, V::default());
        }
        &mut self.values[index]
    }
}

impl<O, V> Default for Partition<O, V> {
    fn default() -> Self {
        Self {
            index: Default::default(),
            values: Default::default(),
            _phantom: Default::default(),
        }
    }
}

impl<O, V> SerializeContainer for Partition<O, V>
where
    V: SerializeContainer,
    O: TryFrom<u32, Error: Debug> + IntoBytes + Immutable,
{
    fn serialize<B: BufMut>(&self, out: &mut B) -> (usize, usize) {
        // keep track of cardinalities and offsets for each flushed value
        let mut cardinalities: Vec<u8> = Vec::with_capacity(self.values.len());
        let mut offsets = Vec::with_capacity(self.values.len());
        let mut offset: u32 = 0;

        for value in &self.values {
            let (cardinality, n) = value.serialize(out);
            cardinalities.push((cardinality - 1).try_into().expect("cardinality overflow"));
            offset += TryInto::<u32>::try_into(n).expect("offset overflow");
            offsets.push(offset);
        }

        // write out the index
        // index keys
        let (cardinality, keys_size) = self.index.serialize(out);
        assert_eq!(cardinality, self.values.len(), "cardinality mismatch");

        // index cardinalities
        let cardinalities_size = cardinalities.len();
        out.put_slice(&cardinalities);

        // index offsets
        let offsets_size = offsets.len() * size_of::<O>();
        for value_offset in offsets {
            let value_offset = O::try_from(offset - value_offset).expect("offset overflow");
            out.put_slice(value_offset.as_bytes());
        }

        (
            cardinality,
            (offset as usize) + keys_size + cardinalities_size + offsets_size,
        )
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
    type Owned = Partition<Offset, V::Owned>;

    fn copy_to_owned(&self) -> Self::Owned {
        let index = self.index.keys().copy_to_owned();
        let values = self.iter().map(|v| v.copy_to_owned()).collect::<Vec<_>>();
        Partition { index, values, _phantom: PhantomData }
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

// Equality Operations

// Partition == Partition
impl<O, V: PartialEq> PartialEq for Partition<O, V> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index && self.values == other.values
    }
}

// PartitionRef == PartitionRef
impl<'a, Offset, V> PartialEq for PartitionRef<'a, Offset, V>
where
    Offset: FromBytes + Immutable + Copy + Into<u32>,
    V: FromSuffix<'a> + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.index.keys() == other.index.keys()
            && self.len() == other.len()
            && self.iter().eq(other.iter())
    }
}

// PartitionRef == Partition
impl<'a, O, V, V2> PartialEq<Partition<O, V2>> for PartitionRef<'a, O, V>
where
    O: FromBytes + Immutable + Copy + Into<u32>,
    V: FromSuffix<'a> + PartialEq<V2>,
{
    fn eq(&self, other: &Partition<O, V2>) -> bool {
        if self.index.keys() != other.index || self.len() != other.values.len() {
            return false;
        }
        for (a, b) in self.iter().zip(other.values.iter()) {
            if a != *b {
                return false;
            }
        }
        true
    }
}

// Partition == PartitionRef
impl<'a, O, V, V2> PartialEq<PartitionRef<'a, O, V>> for Partition<O, V2>
where
    O: FromBytes + Immutable + Copy + Into<u32>,
    V: FromSuffix<'a> + PartialEq<V2>,
{
    fn eq(&self, other: &PartitionRef<'a, O, V>) -> bool {
        other == self
    }
}
