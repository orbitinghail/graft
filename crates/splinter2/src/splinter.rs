use std::fmt::Debug;
use zerocopy::{
    little_endian::{U16, U32},
    FromBytes, Immutable, IntoBytes, KnownLayout, Ref, Unaligned,
};

use crate::{
    bitmap::Bitmap,
    block::{Block, BlockRef},
    partition::{CopyToOwned, FromSuffix, Partition, PartitionRef},
    DecodeErr,
};

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

pub struct Splinter {
    partitions: Partition<Partition<Block>>,
}

impl Splinter {
    pub fn contains(&self, key: u32) -> bool {
        let (high, mid, low) = segments(key);

        if let Some(partition) = self.partitions.lookup(high) {
            if let Some(block) = partition.lookup(mid) {
                return block.contains(low);
            }
        }

        false
    }

    /// calculates the total number of values stored in the set
    pub fn cardinality(&self) -> usize {
        self.partitions
            .iter()
            .flat_map(|p| p.iter())
            .map(|b| b.cardinality())
            .sum()
    }
}

pub struct SplinterRef<T> {
    data: T,
    partitions: usize,
}

impl<T> SplinterRef<T>
where
    T: AsRef<[u8]>,
{
    pub fn from_bytes(data: T) -> Result<Self, DecodeErr> {
        use DecodeErr::*;

        let (header, _) = Ref::<_, Header>::from_prefix(data.as_ref())
            .map_err(|_| InvalidLength { ty: "Header", size: size_of::<Header>() })?;
        if header.magic != SPLINTER_MAGIC {
            return Err(InvalidMagic);
        }

        let (_, footer) = Ref::<_, Footer>::from_suffix(data.as_ref())
            .map_err(|_| InvalidLength { ty: "Footer", size: size_of::<Footer>() })?;
        let partitions = footer.partitions.get() as usize;

        Ok(SplinterRef { data, partitions })
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

    fn load_partitions(&self) -> PartitionRef<'_, U32, PartitionRef<'_, U16, BlockRef<&'_ [u8]>>> {
        let data = self.data.as_ref();
        let slice = &data[..data.len() - size_of::<Footer>()];
        PartitionRef::from_suffix(slice, self.partitions)
    }

    pub fn contains(&self, key: u32) -> bool {
        let (high, mid, low) = segments(key);

        if let Some(partition) = self.load_partitions().lookup(high) {
            if let Some(block) = partition.lookup(mid) {
                return block.contains(low);
            }
        }

        false
    }

    /// calculates the total number of values stored in the set
    pub fn cardinality(&self) -> usize {
        self.load_partitions().iter().map(|p| p.cardinality()).sum()
    }
}

impl<T: AsRef<[u8]>> Debug for SplinterRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Splinter")
            .field("num_partitions", &self.partitions)
            .field("cardinality", &self.cardinality())
            .finish()
    }
}

impl<T: AsRef<[u8]>> CopyToOwned for SplinterRef<T> {
    type Owned = Splinter;

    fn copy_to_owned(&self) -> Self::Owned {
        let partitions = self.load_partitions().copy_to_owned();
        Splinter { partitions }
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
