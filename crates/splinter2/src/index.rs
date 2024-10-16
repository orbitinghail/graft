use zerocopy::{FromBytes, Immutable, Ref};

use crate::block::{block_size, BlockRef};

pub struct IndexRef<'a, Offset> {
    keys: BlockRef<&'a [u8]>,
    cardinalities: &'a [u8],
    offsets: Ref<&'a [u8], [Offset]>,
}

impl<'a, Offset> IndexRef<'a, Offset>
where
    Offset: FromBytes + Immutable + Copy + Into<u32>,
{
    #[inline]
    fn size(cardinality: usize) -> usize {
        let block_size = block_size(cardinality);
        block_size + cardinality + (size_of::<Offset>() * cardinality)
    }

    pub fn from_suffix(data: &'a [u8], cardinality: usize) -> (&'a [u8], Self) {
        let index_size = Self::size(cardinality);
        assert!(data.len() >= index_size, "data too short");
        let (data, index) = data.split_at(data.len() - index_size);
        (data, Self::from_bytes(index, cardinality))
    }

    fn from_bytes(index: &'a [u8], cardinality: usize) -> Self {
        let (keys, index) = index.split_at(block_size(cardinality));
        let (cardinalities, index) = index.split_at(cardinality);
        let (index, offsets) = Ref::from_suffix_with_elems(index, cardinality).unwrap();

        assert!(index.is_empty(), "index should be fully loaded");

        Self {
            keys: BlockRef::from_bytes(keys),
            cardinalities,
            offsets,
        }
    }

    #[inline]
    pub fn keys(&self) -> BlockRef<&'_ [u8]> {
        self.keys.clone()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.cardinalities.len()
    }

    /// returns the cardinality of the index by summing all of
    /// the index's entry cardinalities
    #[inline]
    pub fn cardinality(&self) -> usize {
        self.cardinalities.iter().map(|&x| x as usize + 1).sum()
    }

    /// Lookup the segment in the index
    /// Returns the segment's cardinality and offset
    pub fn lookup(&self, segment: u8) -> Option<(usize, usize)> {
        if self.keys.contains(segment) {
            let rank = self.keys.rank(segment);
            self.get(rank - 1)
        } else {
            None
        }
    }

    /// Get the cardinality and offset of the segment at the given index
    pub fn get(&self, index: usize) -> Option<(usize, usize)> {
        if index < self.len() {
            let cardinality = self.cardinalities[index] as usize + 1;
            let offset = self.offsets[index].into() as usize;
            Some((cardinality, offset))
        } else {
            None
        }
    }
}
