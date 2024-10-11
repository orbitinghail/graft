use std::ops::Deref;

use crate::map::Container;

pub struct Block<T> {
    data: T,
}

impl<'a> Block<&'a [u8]> {
    pub fn from_prefix(data: &'a [u8], cardinality: usize) -> (Self, &'a [u8]) {
        let size = block_size(cardinality);
        assert!(data.len() >= size, "data too short");
        assert!(size > 0, "empty block");
        let (data, rest) = data.split_at(size);
        (Self { data }, rest)
    }
}

impl<T> Block<T>
where
    T: Deref<Target = [u8]>,
{
    #[cfg(test)]
    pub fn new(data: T) -> Self {
        Self { data }
    }

    #[cfg(test)]
    #[inline]
    pub fn len(&self) -> usize {
        if self.data.len() == 32 {
            // block is a 32 byte bitmap
            self.data.iter().map(|&x| x.count_ones() as usize).sum()
        } else {
            // block is a list of segments
            self.data.len()
        }
    }

    #[inline]
    /// Count the number of 1-bits in the block up to and including the position `i`
    pub fn rank(&self, i: u8) -> usize {
        // TODO: implement SIMD/AVX versions
        if self.data.len() == 32 {
            // block is a 32 byte bitmap
            let key = block_key(i);

            // number of bits set up to the key-th byte
            let prefix_bits = self.data[0..key]
                .iter()
                .map(|&x| x.count_ones())
                .sum::<u32>();

            // number of bits set up to the bit-th bit in the key-th byte
            let bit = block_bit(i) as u32;
            let bits = (self.data[key] << (7 - bit)).count_ones();

            (prefix_bits + bits) as usize
        } else {
            // block is a list of segments
            match self.data.binary_search(&i) {
                Ok(i) => i + 1,
                Err(i) => i,
            }
        }
    }

    #[inline]
    pub fn contains(&self, segment: u8) -> bool {
        // TODO: implement SIMD/AVX versions

        if self.data.len() == 32 {
            // block is a 32 byte bitmap
            self.data[block_key(segment)] & (1 << block_bit(segment)) != 0
        } else {
            // block is a list of segments
            self.data.iter().any(|&x| x == segment)
        }
    }
}

impl<'a> Container<'a> for Block<&'a [u8]> {
    type Value<'b> = ();

    fn from_suffix(data: &'a [u8], cardinality: usize) -> Self {
        let size = block_size(cardinality);
        assert!(data.len() >= size, "data too short");
        assert!(size > 0, "empty block");
        Self { data: &data[data.len() - size..] }
    }

    fn lookup(&self, segment: u8) -> Option<Self::Value<'a>> {
        self.contains(segment).then_some(())
    }
}

#[inline]
pub fn block_size(cardinality: usize) -> usize {
    cardinality.min(32)
}

#[inline]
pub fn block_key(segment: u8) -> usize {
    segment as usize / 8
}

#[inline]
/// Return the bit position of the segment in the block
pub fn block_bit(segment: u8) -> u8 {
    segment % 8
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};

    use crate::writer::{BlockWriter, ContainerWriter};

    use super::*;

    fn mkblock(values: impl IntoIterator<Item = u8>) -> Block<Bytes> {
        let mut buf = BytesMut::default();
        let mut writer = BlockWriter::default();
        for i in values {
            writer.push(i);
        }
        writer.flush(&mut buf);
        Block::new(buf.freeze())
    }

    #[test]
    fn test_block_rank() {
        // empty block
        assert_eq!(mkblock(0..0).rank(0), 0);
        assert_eq!(mkblock(0..0).rank(128), 0);
        assert_eq!(mkblock(0..0).rank(255), 0);

        // block with 1 element
        assert_eq!(mkblock(0..1).rank(0), 1);
        assert_eq!(mkblock(0..1).rank(128), 1);
        assert_eq!(mkblock(128..129).rank(0), 0);

        // block with 31 elements; stored as a list
        let block = mkblock(0..31);
        assert_eq!(block.len(), 31);
        for i in 0..31 {
            assert_eq!(block.rank(i), (i + 1).into());
        }
        for i in 31..255 {
            assert_eq!(block.rank(i), 31);
        }

        // block with 32 elements; stored as a bitmap
        let block = mkblock(0..32);
        assert_eq!(block.len(), 32);
        for i in 0..32 {
            assert_eq!(block.rank(i), (i + 1).into());
        }
        for i in 32..255 {
            assert_eq!(block.rank(i), 32);
        }

        // full block
        let block = mkblock(0..=255);
        assert_eq!(block.len(), 256);
        for i in 0..255 {
            assert_eq!(block.rank(i), (i + 1).into());
        }
    }
}
