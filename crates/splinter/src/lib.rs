use std::fmt::Debug;

use map::{Container, Map};
use thiserror::Error;
use zerocopy::{
    little_endian::{U16, U32},
    FromBytes, Immutable, IntoBytes, KnownLayout, Ref,
};

mod block;
mod index;
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

#[derive(Debug, Error)]
pub enum DecodeErr {
    #[error("Unable to decode {section}")]
    InvalidSection { section: &'static str },

    #[error("Invalid magic number")]
    InvalidMagic,
}

type Partition<'a> = Map<'a, U16, block::Block<&'a [u8]>>;

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

    /// calculates the total number of values stored in the set
    pub fn cardinality(&self) -> usize {
        self.load_partitions()
            .iter()
            .map(|p| p.index().cardinality())
            .sum()
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Bytes, BytesMut};
    use writer::SplinterBuilder;

    fn mksplinter(values: impl IntoIterator<Item = u32>) -> Splinter<Bytes> {
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
