use std::fmt::Debug;

use map::{Container, Map};
use thiserror::Error;
use zerocopy::{
    little_endian::{U16, U32},
    FromBytes, Immutable, IntoBytes, KnownLayout, Ref, Unaligned,
};

mod map;
pub mod writer;

pub const SPLINTER_MAGIC: [u8; 2] = [0x57, 0x16];

const MAX_CARDINALITY: usize = u8::MAX as usize + 1;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
struct Header {
    magic: [u8; 2],
    unused: [u8; 2],
}

impl Header {
    const DEFAULT: Header = Header { magic: SPLINTER_MAGIC, unused: [0; 2] };
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable)]
#[repr(C)]
struct Footer {
    partitions: U16,
    unused: [u8; 2],
}

impl Footer {
    fn new(partitions: u16) -> Self {
        Self {
            partitions: partitions.into(),
            unused: [0; 2],
        }
    }
}

static_assertions::assert_eq_size!(Header, Footer, [u8; 4]);

pub struct Block<'a> {
    data: &'a [u8],
}

impl<'a> Container<'a> for Block<'a> {
    type Value<'b> = ();

    fn from_suffix(data: &'a [u8], cardinality: usize) -> Self {
        let size = block_size(cardinality);
        assert!(data.len() >= size, "data too short");
        Self { data: &data[data.len() - size..] }
    }

    fn lookup(&self, segment: u8) -> Option<Self::Value<'a>> {
        block_contains(self.data, segment).then_some(())
    }
}

#[derive(Debug, Error)]
pub enum DecodeErr {
    #[error("Unable to decode {section}")]
    InvalidSection { section: &'static str },

    #[error("Invalid magic number")]
    InvalidMagic,
}

type Partition<'a> = Map<'a, U16, Block<'a>>;

pub struct Splinter<T> {
    data: T,
    partitions: usize,
}

impl<T: Clone> Clone for Splinter<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            partitions: self.partitions,
        }
    }
}

impl<T> Splinter<T>
where
    T: AsRef<[u8]>,
{
    pub fn from_bytes(data: T) -> Result<Self, DecodeErr> {
        use DecodeErr::*;

        let (header, _): (Ref<_, Header>, _) =
            Ref::from_prefix(data.as_ref()).map_err(|_| InvalidSection { section: "header" })?;

        // Check the magic number
        if header.magic != SPLINTER_MAGIC {
            return Err(InvalidMagic);
        }

        let (_, footer): (_, Ref<_, Footer>) =
            Ref::from_suffix(data.as_ref()).map_err(|_| InvalidSection { section: "footer" })?;
        let partitions = footer.partitions.get() as usize;

        Ok(Splinter { data, partitions })
    }

    pub fn size(&self) -> usize {
        self.data.as_ref().len()
    }

    pub fn inner(&self) -> &T {
        &self.data
    }

    pub fn into_inner(self) -> T {
        self.data
    }

    fn load_partitions(&self) -> Map<'_, U32, Partition<'_>> {
        let data = self.data.as_ref();
        let size = data.len();
        let footer_size = size_of::<Footer>();
        Map::from_suffix(&data[..size - footer_size], self.partitions)
    }

    pub fn contains(&self, key: u32) -> bool {
        let (high, mid, low) = segments(key);

        if let Some(partition) = self.load_partitions().lookup(high) {
            if let Some(block) = partition.lookup(mid) {
                return block.lookup(low).is_some();
            }
        }

        false
    }
}

impl<T> Debug for Splinter<T>
where
    T: AsRef<[u8]>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Splinter")
            .field("num_partitions", &self.partitions)
            .finish()
    }
}

/// split the key into 3 8-bit segments
/// requires the key to be < 2^24
#[inline]
fn segments(key: u32) -> (u8, u8, u8) {
    assert!(key < 1 << 24, "key out of range: {}", key);

    let high = (key >> 16) as u8;
    let mid = (key >> 8) as u8;
    let low = key as u8;
    (high, mid, low)
}

#[inline]
fn block_size(cardinality: usize) -> usize {
    cardinality.min(32)
}

#[inline]
fn block_key(segment: u8) -> usize {
    segment as usize / 8
}

#[inline]
/// Return the bit position of the segment in the block
fn block_bit(segment: u8) -> u8 {
    segment % 8
}

fn block_contains(block: &[u8], segment: u8) -> bool {
    // TODO: implement SIMD/AVX versions

    if block.len() == 32 {
        // block is a 32 byte bitmap
        block[block_key(segment)] & (1 << block_bit(segment)) != 0
    } else {
        // block is a list of segments
        assert!(block.len() < 32, "block too large: {}", block.len());
        block.iter().any(|&x| x == segment)
    }
}

#[cfg(test)]
fn block_cardinality(block: &[u8]) -> usize {
    if block.len() == 32 {
        // block is a 32 byte bitmap
        block.iter().map(|&x| x.count_ones() as usize).sum()
    } else {
        // block is a list of segments
        assert!(block.len() < 32, "block too large: {}", block.len());
        block.len()
    }
}

/// Count the number of 1-bits in the block up to and including the position `i`
fn block_rank(block: &[u8], i: u8) -> usize {
    // TODO: implement SIMD/AVX versions

    if block.len() == 32 {
        // block is a 32 byte bitmap
        let key = block_key(i);
        assert!(key < 32, "key out of range: {}", key);

        // number of bits set up to the key-th byte
        let prefix_bits = block[0..key].iter().map(|&x| x.count_ones()).sum::<u32>();

        // number of bits set up to the bit-th bit in the key-th byte
        let bit = block_bit(i) as u32;
        let bits = (block[key] << (7 - bit)).count_ones();

        (prefix_bits + bits) as usize
    } else {
        // block is a list of segments
        assert!(block.len() < 32, "block too large: {}", block.len());
        match block.binary_search(&i) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    }
}

#[inline]
fn index_size<Offset>(cardinality: usize) -> usize {
    let block_size = block_size(cardinality);
    block_size + cardinality + (size_of::<Offset>() * cardinality)
}

fn index_get_offset<Offset>(index: &[u8], cardinality: usize, rank: usize) -> Offset
where
    Offset: FromBytes + Into<u32> + Unaligned,
{
    // calculate the size of the offset
    let offset_size = size_of::<Offset>();

    // calculate the length of the offset section
    let section_length = cardinality * offset_size;

    // calculate the offset's position in the section
    let offset_position = (rank - 1) * offset_size;

    let offset_start = index.len() - section_length + offset_position;
    let offset_end = offset_start + offset_size;

    Offset::read_from_bytes(&index[offset_start..offset_end])
        .expect("failed to lookup offset in index")
}

/// Lookup the segment in the index
/// Returns the segment's cardinality and offset
fn index_lookup<Offset>(index: &[u8], cardinality: usize, segment: u8) -> Option<(usize, usize)>
where
    Offset: FromBytes + Into<u32> + Unaligned,
{
    assert_eq!(
        index.len(),
        index_size::<Offset>(cardinality),
        "invalid index size"
    );
    let block_size = block_size(cardinality);
    assert!(block_size > 0, "index block should never be empty");

    let key_block = &index[0..block_size];
    let cardinality_block = &index[block_size..block_size + cardinality];

    if block_contains(key_block, segment) {
        let rank = block_rank(key_block, segment);
        let segment_cardinality = cardinality_block[rank - 1] as usize + 1;
        let offset: Offset = index_get_offset(index, cardinality, rank);
        Some((segment_cardinality, offset.into() as usize))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Bytes, BytesMut};
    use writer::{BlockWriter, ContainerWriter, SplinterBuilder};

    fn mkblock(values: impl IntoIterator<Item = u8>) -> Bytes {
        let mut buf = BytesMut::default();
        let mut writer = BlockWriter::default();
        for i in values {
            writer.push(i);
        }
        writer.flush(&mut buf);
        buf.freeze()
    }

    fn mksplinter(values: impl IntoIterator<Item = u32>) -> Splinter<Bytes> {
        let mut builder = SplinterBuilder::new(BytesMut::default());
        for i in values {
            builder.push(i);
        }
        Splinter::from_bytes(builder.build().freeze()).unwrap()
    }

    #[test]
    fn test_block_rank() {
        // empty block
        assert_eq!(block_rank(&[], 0), 0);
        assert_eq!(block_rank(&[], 128), 0);
        assert_eq!(block_rank(&[], 255), 0);

        // block with 1 element
        assert_eq!(block_rank(&[0], 0), 1);
        assert_eq!(block_rank(&[0], 128), 1);
        assert_eq!(block_rank(&[128], 0), 0);

        // block with 31 elements; stored as a list
        let block = mkblock(0..31);
        assert_eq!(block_cardinality(&block), 31);
        for i in 0..31 {
            assert_eq!(block_rank(&block, i), (i + 1).into());
        }
        for i in 31..255 {
            assert_eq!(block_rank(&block, i), 31);
        }

        // block with 32 elements; stored as a bitmap
        let block = mkblock(0..32);
        assert_eq!(block_cardinality(&block), 32);
        for i in 0..32 {
            assert_eq!(block_rank(&block, i), (i + 1).into());
        }
        for i in 32..255 {
            assert_eq!(block_rank(&block, i), 32);
        }

        // full block
        let block = mkblock(0..=255);
        assert_eq!(block_cardinality(&block), 256);
        for i in 0..255 {
            assert_eq!(block_rank(&block, i), (i + 1).into());
        }
    }

    // sanity test
    #[test]
    fn test_splinter_sanity() {
        // fill up the first partition and sparse fill up the second partition
        let values = (0..65535)
            .chain((65536..85222).step_by(7))
            .collect::<Vec<_>>();

        // build a splinter from the values
        let splinter = mksplinter(values.iter().copied());

        // check that all expected keys are present
        for &i in &values {
            if !splinter.contains(i) {
                splinter.contains(i); // break here for debugging
                panic!("missing key: {}", i);
            }
        }

        // check that some keys are not present
        assert!(!splinter.contains(65535), "unexpected key: 65535");
        assert!(!splinter.contains(90999), "unexpected key: 90999");
    }

    #[test]
    fn test_expected_compression() {
        let elements = 4096;

        // fully dense splinter
        let data = mksplinter(0..elements);
        assert_eq!(data.size(), 590);

        // 1 element per block; dense partitions
        let data = mksplinter((0..).step_by(256).take(elements as usize));
        assert_eq!(data.size(), 17000);

        // 1 element per block; sparse partitions
        let data = mksplinter((0..).step_by(4096).take(elements as usize));
        assert_eq!(data.size(), 21800);
    }
}
