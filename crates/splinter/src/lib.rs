use std::fmt::Debug;

use bytes::Bytes;
use decode::Ref;
use map::{Container, Map};
use thiserror::Error;
use zerocopy::{
    little_endian::{U16, U32},
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
};

mod block;
mod decode;
mod index;
mod map;

pub mod writer;

pub const SPLINTER_MAGIC: [u8; 2] = [0x57, 0x16];

const MAX_CARDINALITY: usize = u8::MAX as usize + 1;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Header {
    magic: [u8; 2],
    unused: [u8; 2],
}

impl Header {
    const DEFAULT: Header = Header { magic: SPLINTER_MAGIC, unused: [0; 2] };
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
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

#[derive(Debug, Error)]
pub enum DecodeErr {
    #[error("Unable to decode {ty}; needs {size} bytes")]
    InvalidLength { ty: &'static str, size: usize },

    #[error("Invalid magic number")]
    InvalidMagic,
}

type Partition<'a> = Map<'a, U16, block::Block<&'a [u8]>>;

pub struct Splinter {
    data: Bytes,
    partitions: usize,
}

impl Clone for Splinter {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            partitions: self.partitions,
        }
    }
}

impl Splinter {
    pub fn from_bytes(mut data: Bytes) -> Result<Self, DecodeErr> {
        use DecodeErr::*;

        let header: Ref<Header> = Ref::from_prefix(&mut data)?;
        if header.magic != SPLINTER_MAGIC {
            return Err(InvalidMagic);
        }

        let footer: Ref<Footer> = Ref::from_suffix(&mut data)?;
        let partitions = footer.partitions.get() as usize;

        Ok(Splinter { data, partitions })
    }

    pub fn size(&self) -> usize {
        self.data.as_ref().len() + size_of::<Header>() + size_of::<Footer>()
    }

    pub fn inner(&self) -> &Bytes {
        &self.data
    }

    pub fn into_inner(self) -> Bytes {
        self.data
    }

    fn load_partitions(&self) -> Map<'_, U32, Partition<'_>> {
        Map::from_suffix(&self.data, self.partitions)
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

    /// calculates the total number of values stored in the set
    pub fn cardinality(&self) -> usize {
        self.load_partitions()
            .iter()
            .map(|p| p.index().cardinality())
            .sum()
    }
}

impl Debug for Splinter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Splinter")
            .field("num_partitions", &self.partitions)
            .field("cardinality", &self.cardinality())
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use writer::SplinterBuilder;

    fn mksplinter(values: impl IntoIterator<Item = u32>) -> Splinter {
        let mut builder = SplinterBuilder::new(BytesMut::default());
        for i in values {
            builder.push(i);
        }
        Splinter::from_bytes(builder.build().freeze()).unwrap()
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
