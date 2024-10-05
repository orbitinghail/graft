use std::{fmt::Debug, ops::Index};

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

type Partition<'a> = Map<'a, U16, Block<'a>>;

pub struct Splinter<'a> {
    partitions: Map<'a, U32, Partition<'a>>,
}

#[derive(Debug, Error)]
pub enum DecodeErr {
    #[error("Unable to decode {section}")]
    InvalidSection { section: &'static str },

    #[error("Invalid magic number")]
    InvalidMagic,
}

impl<'a> Splinter<'a> {
    pub fn from_bytes(data: &'a [u8]) -> Result<Self, DecodeErr> {
        use DecodeErr::*;

        let (header, data): (Ref<_, Header>, _) =
            Ref::from_prefix(data).map_err(|_| InvalidSection { section: "header" })?;

        // Check the magic number
        if header.magic != SPLINTER_MAGIC {
            return Err(InvalidMagic);
        }

        let (data, footer): (_, Ref<_, Footer>) =
            Ref::from_suffix(data).map_err(|_| InvalidSection { section: "footer" })?;

        let partitions = Map::from_suffix(data, footer.partitions.get() as usize);

        Ok(Splinter { partitions })
    }

    pub fn contains(&self, key: u32) -> bool {
        let (high, mid, low) = segments(key);

        if let Some(partition) = self.partitions.lookup(high) {
            if let Some(block) = partition.lookup(mid) {
                return block.lookup(low).is_some();
            }
        }

        false
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
        assert!(block.len() <= 32, "block too large: {}", block.len());
        block.iter().any(|&x| x == segment)
    }
}

fn block_rank(block: &[u8], segment: u8) -> usize {
    if block.len() == 32 {
        // block is a 32 byte bitmap
        let key = block_key(segment);
        assert!(key < 32, "key out of range: {}", key);

        // number of bits set up to the key-th byte
        let prefix_bits = block[0..key].iter().map(|&x| x.count_ones()).sum::<u32>();

        // number of bits set up to the bit-th bit in the key-th byte
        let bit = block_bit(segment) as u16;
        let bits = (block[key] << (7 - bit)).count_ones();

        (prefix_bits + bits) as usize
    } else {
        // block is a list of segments
        assert!(block.len() < 32, "block too large: {}", block.len());
        match block.binary_search(&segment) {
            Ok(i) => i + 1,
            Err(i) => i,
        }
    }
}

/// select the n-th set bit in the value
fn byte_select(mut value: u8, n: u16) -> u16 {
    // reset n of the least significant bits
    for _ in 0..n {
        value &= value - 1;
    }
    value.trailing_zeros() as u16
}

/// Select the segment at the given rank in the block
fn block_select(block: &[u8], rank: usize) -> Option<u8> {
    assert!(rank < u8::MAX as usize, "rank out of range: {}", rank);

    if block.len() == 32 {
        // block is a 32 byte bitmap
        let mut rank = rank as u16;
        for (i, &value) in block.iter().enumerate() {
            let len = value.count_ones() as u16;
            if rank < len {
                let offset = byte_select(value, rank);
                return Some(((8 * i as u16) + offset) as u8);
            }
            rank -= len;
        }
        None
    } else {
        // block is a list of segments
        assert!(block.len() < 32, "block too large: {}", block.len());
        block.get(rank).copied()
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
    // calculate the length of the offset section
    let section_length = cardinality * size_of::<Offset>();

    // calculate the size of the offset and it's position in the section
    let offset_size = size_of::<Offset>();
    let offset_position = rank * offset_size;

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
    use std::io;

    use super::*;
    use writer::SplinterWriter;

    // sanity test
    #[test]
    fn test_splinter_sanity() {
        // fill up the first partition and sparse fill up the second partition
        let values = (0..65535)
            .chain((65536..85222).step_by(7))
            .collect::<Vec<_>>();

        // write a splinter using SplinterWriter
        let buf = io::Cursor::new(vec![]);
        let (_, mut writer) = SplinterWriter::new(buf).unwrap();
        for &i in &values {
            writer.push(i).unwrap();
        }
        let (_, buf) = writer.finish().unwrap();

        // read the splinter using Splinter
        let data = buf.into_inner();
        let splinter = Splinter::from_bytes(&data).unwrap();

        // check that all expected keys are present
        for &i in &values {
            if i == 65279 {
                println!("here")
            }
            assert!(splinter.contains(i), "missing key: {}", i);
        }

        // check that some keys are not present
        assert!(!splinter.contains(65535), "unexpected key: 65535");
        assert!(!splinter.contains(90999), "unexpected key: 90999");
    }
}
