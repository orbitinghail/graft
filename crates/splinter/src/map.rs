use std::marker::PhantomData;

use zerocopy::{FromBytes, Immutable};

use crate::index::Index;

pub trait Container<'a> {
    type Value<'b>;

    fn from_suffix(data: &'a [u8], cardinality: usize) -> Self;
    fn lookup(&self, segment: u8) -> Option<Self::Value<'a>>;
}

pub struct Map<'a, Offset, V> {
    cardinality: usize,
    values: &'a [u8],
    index: Index<'a, Offset>,
    _phantom: PhantomData<V>,
}

impl<'a, Offset, V> Container<'a> for Map<'a, Offset, V>
where
    V: Container<'a>,
    Offset: FromBytes + Immutable + Copy + Into<u32>,
{
    type Value<'b> = V;

    fn from_suffix(data: &'a [u8], cardinality: usize) -> Self {
        let (values, index) = Index::from_suffix(data, cardinality);

        Self {
            cardinality,
            values,
            index,
            _phantom: PhantomData,
        }
    }

    fn lookup(&self, segment: u8) -> Option<V> {
        if let Some((cardinality, offset)) = self.index.lookup(segment) {
            assert!(self.values.len() >= offset, "offset out of range");
            let data = &self.values[..(self.values.len() - offset)];
            Some(V::from_suffix(data, cardinality))
        } else {
            None
        }
    }
}

impl<'a, Offset, V> Map<'a, Offset, V>
where
    V: Container<'a>,
    Offset: FromBytes + Immutable + Copy + Into<u32>,
{
    fn get(&self, index: usize) -> Option<V> {
        if let Some((cardinality, offset)) = self.index.get(index) {
            assert!(self.values.len() >= offset, "offset out of range");
            let data = &self.values[..(self.values.len() - offset)];
            Some(V::from_suffix(data, cardinality))
        } else {
            None
        }
    }

    #[inline]
    pub fn index(&self) -> &Index<'a, Offset> {
        &self.index
    }

    #[inline]
    pub fn iter(&self) -> MapIter<'_, Offset, V> {
        MapIter { map: self, cursor: 0 }
    }
}

pub struct MapIter<'a, Offset, V> {
    map: &'a Map<'a, Offset, V>,
    cursor: usize,
}

impl<'a, Offset, V> Iterator for MapIter<'a, Offset, V>
where
    V: Container<'a>,
    Offset: FromBytes + Immutable + Copy + Into<u32>,
{
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.map.get(self.cursor);
        self.cursor += 1;
        result
    }
}
