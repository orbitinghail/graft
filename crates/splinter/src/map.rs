use std::marker::PhantomData;

use zerocopy::{FromBytes, Ref, Unaligned};

use crate::{index_lookup, index_size};

pub trait Container<'a> {
    type Value<'b>;

    fn from_suffix(data: &'a [u8], cardinality: u8) -> Self;
    fn lookup(&self, segment: u8) -> Option<Self::Value<'a>>;
}

pub struct Map<'a, Offset, V> {
    cardinality: u8,
    values: &'a [u8],
    index: Ref<&'a [u8], [u8]>,
    _phantom: PhantomData<(Offset, V)>,
}

impl<'a, Offset, V> Container<'a> for Map<'a, Offset, V>
where
    V: Container<'a>,
    Offset: FromBytes + Into<u32> + Unaligned,
{
    type Value<'b> = V;

    fn from_suffix(data: &'a [u8], cardinality: u8) -> Self {
        let index_size = index_size::<Offset>(cardinality);
        assert!(data.len() >= index_size, "data too short");
        let (values, index) = Ref::new_slice_from_suffix(data, index_size).unwrap();

        Self {
            cardinality,
            values,
            index,
            _phantom: PhantomData,
        }
    }

    fn lookup(&self, segment: u8) -> Option<V> {
        if let Some((cardinality, offset)) =
            index_lookup::<Offset>(&self.index, self.cardinality, segment)
        {
            assert!(self.values.len() >= offset, "offset out of range");
            let data = &self.values[..(self.values.len() - offset)];
            Some(V::from_suffix(data, cardinality))
        } else {
            None
        }
    }
}
