use bytes::{Bytes, BytesMut};
use std::fmt::Debug;
use zerocopy::{
    little_endian::{U16, U32},
    FromBytes, Immutable, IntoBytes, KnownLayout, Ref, Unaligned,
};

use crate::{
    bitmap::BitmapExt,
    block::{Block, BlockRef},
    partition::{Partition, PartitionRef},
    util::{CopyToOwned, FromSuffix, Serialize},
    DecodeErr,
};

pub const SPLINTER_MAGIC: [u8; 2] = [0x57, 0x16];

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Header {
    magic: [u8; 2],
    unused: [u8; 2],
}

impl Header {
    const DEFAULT: Header = Header { magic: SPLINTER_MAGIC, unused: [0; 2] };

    fn serialize<B: bytes::BufMut>(&self, out: &mut B) -> usize {
        out.put_slice(self.as_bytes());
        size_of::<Header>()
    }
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

    fn serialize<B: bytes::BufMut>(&self, out: &mut B) -> usize {
        out.put_slice(self.as_bytes());
        size_of::<Header>()
    }
}

#[derive(Default)]
pub struct Splinter {
    partitions: Partition<U32, Partition<U16, Block>>,
}

impl Splinter {
    pub fn contains(&self, key: u32) -> bool {
        let (high, mid, low) = segments(key);

        if let Some(partition) = self.partitions.get(high) {
            if let Some(block) = partition.get(mid) {
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

    pub fn insert(&mut self, key: u32) {
        let (high, mid, low) = segments(key);
        let partition = self.partitions.get_or_init(high);
        let block = partition.get_or_init(mid);
        block.insert(low);
    }

    pub fn serialize<B: bytes::BufMut>(&self, out: &mut B) -> usize {
        let header_size = Header::DEFAULT.serialize(out);
        let (cardinality, partitions_size) = self.partitions.serialize(out);
        let footer_size =
            Footer::new(cardinality.try_into().expect("cardinality overflow")).serialize(out);
        header_size + partitions_size + footer_size
    }

    pub fn serialize_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.serialize(&mut buf);
        buf.freeze()
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

    pub(crate) fn load_partitions(
        &self,
    ) -> PartitionRef<'_, U32, PartitionRef<'_, U16, BlockRef<&'_ [u8]>>> {
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

// Equality Operations

// Splinter == Splinter
impl PartialEq for Splinter {
    fn eq(&self, other: &Self) -> bool {
        self.partitions == other.partitions
    }
}

// SplinterRef == Splinter
impl<T: AsRef<[u8]>> PartialEq<SplinterRef<T>> for Splinter {
    fn eq(&self, other: &SplinterRef<T>) -> bool {
        other.load_partitions() == self.partitions
    }
}

// Splinter == SplinterRef
impl<T: AsRef<[u8]>> PartialEq<Splinter> for SplinterRef<T> {
    fn eq(&self, other: &Splinter) -> bool {
        self.load_partitions() == other.partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn mksplinter(values: impl IntoIterator<Item = u32>) -> Splinter {
        let mut splinter = Splinter::default();
        for i in values {
            splinter.insert(i);
        }
        splinter
    }

    fn mksplinter_ref(values: impl IntoIterator<Item = u32>) -> SplinterRef<Bytes> {
        SplinterRef::from_bytes(mksplinter(values).serialize_to_bytes()).unwrap()
    }

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
    fn test_roundtrip_sanity() {
        let splinter = mksplinter_ref(0..0).copy_to_owned();
        assert_eq!(splinter.cardinality(), 0);

        let splinter = mksplinter_ref(0..10).copy_to_owned();
        assert_eq!(splinter.cardinality(), 10);
        for i in 0..10 {
            assert!(splinter.contains(i));
        }
    }

    #[test]
    fn test_splinter_ref_sanity() {
        // fill up the first partition and sparse fill up the second partition
        let values = (0..65535)
            .chain((65536..85222).step_by(7))
            .collect::<Vec<_>>();

        // build a splinter from the values
        let splinter = mksplinter_ref(values.iter().copied());

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
        let data = mksplinter(0..elements).serialize_to_bytes();
        assert_eq!(data.len(), 590);

        // 1 element per block; dense partitions
        let data = mksplinter((0..).step_by(256).take(elements as usize)).serialize_to_bytes();
        assert_eq!(data.len(), 17000);

        // 1 element per block; sparse partitions
        let data = mksplinter((0..).step_by(4096).take(elements as usize)).serialize_to_bytes();
        assert_eq!(data.len(), 21800);
    }
}
