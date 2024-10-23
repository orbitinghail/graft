use std::{convert::TryInto, fmt::Debug, marker::PhantomData, mem::size_of};

use bytes::BufMut;
use zerocopy::{FromBytes, Immutable, IntoBytes};

use crate::{
    bitmap::BitmapExt,
    block::Block,
    index::IndexRef,
    relational::Relation,
    util::{CopyToOwned, FromSuffix, SerializeContainer},
    Segment,
};

#[derive(Clone)]
pub struct Partition<Offset, V> {
    index: Block,
    values: Vec<V>,
    _phantom: PhantomData<Offset>,
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

impl<O, V> Partition<O, V> {
    // insert a value into the partition; panics if the segment is already present
    pub fn insert(&mut self, segment: Segment, value: V) {
        let was_missing = self.index.insert(segment);
        assert!(was_missing, "segment already present in partition");
        let index = self.index.rank(segment) - 1;
        self.values.insert(index, value);
    }

    pub fn inner_join_mut<'a, R: Relation>(
        &'a mut self,
        right: &'a R,
    ) -> impl Iterator<Item = (Segment, &mut V, R::ValRef<'a>)> {
        self.index
            .segments()
            .zip(self.values.iter_mut())
            .filter_map(|(k, l)| right.get(k).map(|r| (k, l, r)))
    }
}

impl<O, V> Relation for Partition<O, V> {
    type ValRef<'a> = &'a V
    where
        Self: 'a;

    #[inline]
    fn len(&self) -> usize {
        self.values.len()
    }

    fn sorted_iter(&self) -> impl Iterator<Item = (Segment, Self::ValRef<'_>)> {
        self.index.segments().zip(self.values.iter())
    }

    fn sorted_values(&self) -> impl Iterator<Item = Self::ValRef<'_>> {
        self.values.iter()
    }

    fn get(&self, key: Segment) -> Option<Self::ValRef<'_>> {
        if self.index.contains(key) {
            let rank = self.index.rank(key);
            self.values.get(rank - 1)
        } else {
            None
        }
    }
}

impl<O, V> SerializeContainer for Partition<O, V>
where
    V: SerializeContainer,
    O: TryFrom<u32, Error: Debug> + IntoBytes + Immutable,
{
    fn should_serialize(&self) -> bool {
        self.values.iter().any(|v| v.should_serialize())
    }

    fn serialize<B: BufMut>(&self, out: &mut B) -> (usize, usize) {
        // keep track of cardinalities and offsets for each flushed value
        let mut cardinalities: Vec<u8> = Vec::with_capacity(self.values.len());
        let mut offsets = Vec::with_capacity(self.values.len());
        let mut offset: u32 = 0;

        for value in self.values.iter().filter(|v| v.should_serialize()) {
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
        let index = self.index.key_block().copy_to_owned();
        let values = self
            .sorted_values()
            .map(|v| v.copy_to_owned())
            .collect::<Vec<_>>();
        Partition { index, values, _phantom: PhantomData }
    }
}

impl<'a, Offset, V> PartitionRef<'a, Offset, V>
where
    Offset: FromBytes + Immutable + Copy + Into<u32>,
    V: FromSuffix<'a>,
{
    fn get_by_index(&self, index: usize) -> Option<V> {
        if let Some((cardinality, offset)) = self.index.get(index) {
            assert!(self.values.len() >= offset, "offset out of range");
            let data = &self.values[..(self.values.len() - offset)];
            Some(V::from_suffix(data, cardinality))
        } else {
            None
        }
    }

    /// returns the cardinality of the partition by summing the cardinalities
    /// stored in the partition's index
    #[inline]
    pub fn cardinality(&self) -> usize {
        self.index.cardinality()
    }
}

struct PartitionRefValuesIter<'a, 'b, O, V> {
    inner: &'a PartitionRef<'b, O, V>,
    cursor: usize,
}

impl<'a, 'b, O, V> Iterator for PartitionRefValuesIter<'a, 'b, O, V>
where
    O: FromBytes + Immutable + Copy + Into<u32>,
    V: FromSuffix<'b>,
{
    type Item = V;

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.inner.len() - self.cursor;
        (remaining, Some(remaining))
    }

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.inner.get_by_index(self.cursor);
        self.cursor += 1;
        result
    }
}

impl<'a, Offset, V> Relation for PartitionRef<'a, Offset, V>
where
    Offset: FromBytes + Immutable + Copy + Into<u32>,
    V: FromSuffix<'a>,
{
    type ValRef<'b> = V
    where
        Self: 'b;

    #[inline]
    fn len(&self) -> usize {
        self.index.len()
    }

    #[inline]
    fn sorted_iter(&self) -> impl Iterator<Item = (Segment, Self::ValRef<'_>)> {
        self.index.segments().zip(self.sorted_values())
    }

    #[inline]
    fn sorted_values(&self) -> impl Iterator<Item = Self::ValRef<'_>> {
        PartitionRefValuesIter { inner: self, cursor: 0 }
    }

    fn get(&self, key: Segment) -> Option<Self::ValRef<'_>> {
        if let Some((cardinality, offset)) = self.index.lookup(key) {
            assert!(self.values.len() >= offset, "offset out of range");
            let data = &self.values[..(self.values.len() - offset)];
            Some(V::from_suffix(data, cardinality))
        } else {
            None
        }
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
        self.index.key_block() == other.index.key_block()
            && self.len() == other.len()
            && self.sorted_values().eq(other.sorted_values())
    }
}

// PartitionRef == Partition
impl<'a, O, V, V2> PartialEq<Partition<O, V2>> for PartitionRef<'a, O, V>
where
    O: FromBytes + Immutable + Copy + Into<u32>,
    V: FromSuffix<'a> + PartialEq<V2>,
{
    fn eq(&self, other: &Partition<O, V2>) -> bool {
        if self.index.key_block() != other.index || self.len() != other.values.len() {
            return false;
        }
        for (a, b) in self.sorted_values().zip(other.values.iter()) {
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
