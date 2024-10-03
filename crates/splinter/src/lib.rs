use std::fmt::Debug;

use map::{Container, Map};
use thiserror::Error;
use zerocopy::{
    little_endian::{U16, U32},
    AsBytes, FromBytes, FromZeroes, Ref, Unaligned,
};

mod map;
pub mod writer;

pub const SPLINTER_MAGIC: U16 = U16::from_bytes([0x57, 0x11]);

#[derive(FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
struct Header {
    magic: U16,
    unused: [u8; 2],
}

impl Header {
    const DEFAULT: Header = Header { magic: SPLINTER_MAGIC, unused: [0; 2] };
}

#[derive(FromZeroes, FromBytes, AsBytes)]
#[repr(C)]
struct Footer {
    partitions: u8,
    unused: [u8; 3],
}

static_assertions::assert_eq_size!(Header, Footer, [u8; 4]);

pub struct Block<'a> {
    data: &'a [u8],
}

impl<'a> Container<'a> for Block<'a> {
    type Value<'b> = ();

    fn from_suffix(data: &'a [u8], cardinality: u8) -> Self {
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
            Ref::new_from_prefix(data).ok_or(InvalidSection { section: "header" })?;

        // Check the magic number
        if header.magic != SPLINTER_MAGIC {
            return Err(InvalidMagic);
        }

        let (data, footer): (_, Ref<_, Footer>) =
            Ref::new_from_suffix(data).ok_or(InvalidSection { section: "footer" })?;

        let partitions = Map::from_suffix(data, footer.partitions);

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
fn block_size(cardinality: u8) -> usize {
    (cardinality as usize).min(32)
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
        let mask = ((1u16 << (bit + 1)) - 1) as u8;
        let bits = (block[key] & mask).count_ones();

        (prefix_bits + bits) as usize
    } else {
        // block is a list of segments
        assert!(block.len() <= 32, "block too large: {}", block.len());
        block.iter().take_while(|&&x| x != segment).count()
    }
}

#[inline]
fn index_size<Offset>(cardinality: u8) -> usize {
    let block_size = block_size(cardinality);
    let cardinality = cardinality as usize;
    block_size + cardinality + (size_of::<Offset>() * cardinality)
}

fn index_get_offset<Offset>(index: &[u8], cardinality: u8, rank: usize) -> Offset
where
    Offset: FromBytes + Into<u32> + Unaligned,
{
    let offsets_length = (cardinality as usize) * size_of::<Offset>();
    let offset_size = size_of::<Offset>();
    Offset::read_from_prefix(&index[index.len() - offsets_length..][(rank * offset_size)..])
        .expect("invalid offset")
}

/// Lookup the segment in the index
/// Returns the segment's cardinality and offset
fn index_lookup<Offset>(index: &[u8], cardinality: u8, segment: u8) -> Option<(u8, usize)>
where
    Offset: FromBytes + Into<u32> + Unaligned,
{
    assert_eq!(
        index.len(),
        index_size::<Offset>(cardinality),
        "invalid index size"
    );
    let block_size = block_size(cardinality);
    let block = &index[0..block_size];

    if block_contains(block, segment) {
        let rank = block_rank(block, segment);
        let segment_cardinality = index[block_size + rank];
        let offset: Offset = index_get_offset(index, cardinality, rank);
        Some((segment_cardinality, offset.into() as usize))
    } else {
        None
    }
}
