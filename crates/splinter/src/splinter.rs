use bytes::{Bytes, BytesMut};
use std::fmt::Debug;
use zerocopy::{
    little_endian::{U16, U32},
    FromBytes, Immutable, IntoBytes, KnownLayout, Ref, Unaligned,
};

use crate::{
    bitmap::BitmapExt,
    block::{Block, BlockRef},
    ops::{Cut, Intersection},
    partition::{Partition, PartitionRef},
    relational::{Relation, RelationMut},
    util::{CopyToOwned, FromSuffix, SerializeContainer},
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
    pub fn from_bytes<T: AsRef<[u8]>>(data: T) -> Result<Self, DecodeErr> {
        SplinterRef::from_bytes(data).map(SplinterRef::into_splinter)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

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
            .sorted_values()
            .flat_map(|p| p.sorted_values())
            .map(|b| b.cardinality())
            .sum()
    }

    pub fn insert(&mut self, key: u32) {
        let (high, mid, low) = segments(key);
        let partition = self.partitions.get_or_init(high);
        let block = partition.get_or_init(mid);
        block.insert(low);
    }

    fn insert_block(&mut self, high: u8, mid: u8, block: Block) {
        let partition = self.partitions.get_or_init(high);
        partition.insert(mid, block);
    }

    /// Returns the intersection between self and other while removing the
    /// intersection from self
    // pub fn cut<T: AsRef<[u8]>>(&mut self, other: SplinterRef<T>) -> Splinter {
    //     let mut out = Splinter::default();
    //     for (high, left, right) in self.partitions.inner_join_mut(&other.load_partitions()) {
    //         for (mid, left, right) in left.inner_join_mut(&right) {
    //             out.insert_block(high, mid, left.cut(&right));
    //         }
    //     }
    //     out
    // }

    /// Returns the intersection between self and other
    // pub fn intersection<T: AsRef<[u8]>>(&mut self, other: SplinterRef<T>) -> Splinter {
    //     let mut out = Splinter::default();
    //     for (high, left, right) in self.partitions.inner_join(&other.load_partitions()) {
    //         for (mid, left, right) in left.inner_join(&right) {
    //             out.insert_block(high, mid, left.intersection(&right));
    //         }
    //     }
    //     out
    // }

    pub fn union(&mut self, other: ()) -> Splinter {
        // returns the union of self and other

        /*
        let mut out = Splinter::default();
        for (high, left, right) in self.partitions.full_outer_join(other.partitions) {
            match (left, right) {
                (Some(left), None) => out.partitions.insert(high, left.clone()),
                (None, Some(right)) => out.partitions.insert(high, right.clone()),
                (Some(left), Some(right)) => todo!("insert union..."),
            }
        }
        out
        */

        todo!()
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

    pub fn serialize_to_splinter_ref(&self) -> SplinterRef<Bytes> {
        SplinterRef::from_bytes(self.serialize_to_bytes()).expect("serialization roundtrip failed")
    }
}

impl Debug for Splinter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Splinter")
            .field("num_partitions", &self.partitions.len())
            .field("cardinality", &self.cardinality())
            .finish()
    }
}

#[derive(Clone)]
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

        if let Some(partition) = self.load_partitions().get(high) {
            if let Some(block) = partition.get(mid) {
                return block.contains(low);
            }
        }

        false
    }

    /// calculates the total number of values stored in the set
    pub fn cardinality(&self) -> usize {
        self.load_partitions()
            .sorted_values()
            .map(|p| p.cardinality())
            .sum()
    }

    pub fn into_splinter(self) -> Splinter {
        let partitions = self.load_partitions().copy_to_owned();
        Splinter { partitions }
    }
}

impl<T: AsRef<[u8]>> Debug for SplinterRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SplinterRef")
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

// SplinterRef == SplinterRef
impl<T1: AsRef<[u8]>, T2: AsRef<[u8]>> PartialEq<SplinterRef<T2>> for SplinterRef<T1> {
    fn eq(&self, other: &SplinterRef<T2>) -> bool {
        self.load_partitions() == other.load_partitions()
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use bytes::Bytes;
    use roaring::RoaringBitmap;

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
        let assert_round_trip = |splinter: Splinter| {
            let splinter_ref = SplinterRef::from_bytes(splinter.serialize_to_bytes()).unwrap();
            assert_eq!(
                splinter.cardinality(),
                splinter_ref.cardinality(),
                "cardinality equal"
            );
            assert_eq!(splinter, splinter_ref, "Splinter == SplinterRef");
            assert_eq!(
                splinter,
                splinter_ref.clone().into_splinter(),
                "Splinter == Splinter"
            );
            assert_eq!(
                splinter_ref.clone().into_splinter().serialize_to_bytes(),
                splinter.serialize_to_bytes(),
                "deterministic serialization"
            );
        };

        assert_round_trip(mksplinter(0..0));
        assert_round_trip(mksplinter(0..10));
        assert_round_trip(mksplinter(0..=255));
        assert_round_trip(mksplinter(0..=4096));
        assert_round_trip(mksplinter(0..=16384));
        assert_round_trip(mksplinter(1512..=3258));
        assert_round_trip(mksplinter((0..=16384).step_by(7)));
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
        let roaring_size = |set: Vec<u32>| {
            let mut buf = io::Cursor::new(Vec::new());
            RoaringBitmap::from_sorted_iter(set)
                .unwrap()
                .serialize_into(&mut buf)
                .unwrap();
            buf.into_inner().len()
        };

        struct Report {
            name: &'static str,
            ty: &'static str,
            size: usize,
            expected: usize,
        }

        let mut reports = vec![];

        let mut run_test = |name: &'static str,
                            set: Vec<u32>,
                            expected_splinter: usize,
                            expected_roaring: usize| {
            let data = mksplinter(set.clone()).serialize_to_bytes();
            reports.push(Report {
                name,
                ty: "Splinter",
                size: data.len(),
                expected: expected_splinter,
            });
            reports.push(Report {
                name,
                ty: "Roaring",
                size: roaring_size(set),
                expected: expected_roaring,
            });
        };

        // 1 element in set
        let set = (0..=0).collect::<Vec<_>>();
        run_test("1 element", set, 19, 18);

        // 1 fully dense block
        let set = (0..=255).collect::<Vec<_>>();
        run_test("1 dense block", set, 50, 528);

        // 8 sparse blocks
        let set = (0..=1024).skip(128).collect::<Vec<_>>();
        run_test("8 sparse blocks", set, 163, 1810);

        // 16 sparse blocks
        let set = (0..=2048).skip(128).collect::<Vec<_>>();
        run_test("16 sparse blocks", set, 307, 3858);

        // 128 sparse blocks
        let set = (0..=16384).skip(128).collect::<Vec<_>>();
        run_test("128 sparse blocks", set, 2290, 8208);

        // the rest of the compression tests use 4k elements
        let elements = 4096;

        // fully dense splinter
        let set = (0..elements).collect::<Vec<_>>();
        run_test("fully dense", set, 590, 8208);

        // 32 elements per block; dense partitions
        let set = (0..).step_by(8).take(elements as usize).collect::<Vec<_>>();
        run_test("32/block; dense", set, 4526, 8208);

        // 16 elements per block; dense partitions
        let set = (0..)
            .step_by(16)
            .take(elements as usize)
            .collect::<Vec<_>>();
        run_test("16/block; dense", set, 4910, 8208);

        // 1 element per block; dense partitions
        // second worse case scenario
        let set = (0..)
            .step_by(256)
            .take(elements as usize)
            .collect::<Vec<_>>();
        run_test("1/block; dense", set, 17000, 8328);

        // 1 element per block; sparse partitions
        // worse case scenario
        let set = (0..)
            .step_by(4096)
            .take(elements as usize)
            .collect::<Vec<_>>();
        run_test("1/block; sparse", set, 21800, 10248);

        let mut fail_test = false;

        println!(
            "{:20} {:12} {:>6} {:>10} {:>10}",
            "distribution", "bitmap", "size", "expected", "result"
        );
        for report in reports {
            println!(
                "{:20} {:12} {:6} {:10} {:>10}",
                report.name,
                report.ty,
                report.size,
                report.expected,
                if report.size == report.expected {
                    "ok"
                } else {
                    fail_test = true;
                    "FAIL"
                }
            );
        }

        assert!(!fail_test, "compression test failed");
    }
}
