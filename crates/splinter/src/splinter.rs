use bytes::{Bytes, BytesMut};
use culprit::Culprit;
use std::fmt::Debug;
use zerocopy::{
    ConvertError, FromBytes, Immutable, IntoBytes, KnownLayout, Ref, Unaligned,
    little_endian::{U16, U32},
};

use crate::{
    DecodeErr, Segment,
    bitmap::{BitmapExt, BitmapMutExt},
    block::{Block, BlockRef},
    partition::{Partition, PartitionRef},
    relational::Relation,
    util::{CopyToOwned, FromSuffix, SerializeContainer},
};

mod cmp;
mod cut;
mod intersection;
mod merge;
mod union;

pub const SPLINTER_MAGIC: [u8; 4] = [0xDA, 0xAE, 0x12, 0xDF];

pub const SPLINTER_MAX_VALUE: u32 = u32::MAX;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Header {
    magic: [u8; 4],
}

impl Header {
    const DEFAULT: Header = Header { magic: SPLINTER_MAGIC };

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

#[derive(Default, Clone)]
pub struct Splinter {
    partitions: Partition<U32, Partition<U32, Partition<U16, Block>>>,
}

impl Splinter {
    pub fn from_slice(data: &[u32]) -> Self {
        let mut splinter = Self::default();
        for &key in data {
            splinter.insert(key);
        }
        splinter
    }

    pub fn from_bytes<T: AsRef<[u8]>>(data: T) -> Result<Self, Culprit<DecodeErr>> {
        SplinterRef::from_bytes(data).map(Into::into)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }

    pub fn contains(&self, key: u32) -> bool {
        let (a, b, c, d) = segments(key);

        if let Some(partition) = self.partitions.get(a) {
            if let Some(partition) = partition.get(b) {
                if let Some(block) = partition.get(c) {
                    return block.contains(d);
                }
            }
        }

        false
    }

    /// calculates the total number of values stored in the set
    pub fn cardinality(&self) -> usize {
        self.partitions
            .sorted_iter()
            .flat_map(|(_, p)| p.sorted_iter())
            .flat_map(|(_, p)| p.sorted_iter())
            .map(|(_, b)| b.cardinality())
            .sum()
    }

    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.partitions
            .sorted_iter()
            .flat_map(|(a, p)| p.sorted_iter().map(move |(b, p)| (a, b, p)))
            .flat_map(|(a, b, p)| p.sorted_iter().map(move |(c, p)| (a, b, c, p)))
            .flat_map(|(a, b, c, p)| p.segments().map(move |d| combine_segments(a, b, c, d)))
    }

    /// attempts to insert a key into the Splinter, returning true if a key was inserted
    pub fn insert(&mut self, key: u32) -> bool {
        let (a, b, c, d) = segments(key);
        let partition = self.partitions.get_or_init(a);
        let partition = partition.get_or_init(b);
        let block = partition.get_or_init(c);
        block.insert(d)
    }

    fn insert_block(&mut self, a: u8, b: u8, c: u8, block: Block) {
        let partition = self.partitions.get_or_init(a);
        let partition = partition.get_or_init(b);
        partition.insert(c, block);
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

    /// returns the last key in the set
    pub fn last(&self) -> Option<u32> {
        let (a, p) = self.partitions.last()?;
        let (b, p) = p.last()?;
        let (c, p) = p.last()?;
        let d = p.last()?;
        Some(combine_segments(a, b, c, d))
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

impl<K: Into<u32>> FromIterator<K> for Splinter {
    fn from_iter<T: IntoIterator<Item = K>>(iter: T) -> Self {
        let mut splinter = Self::default();
        for key in iter {
            splinter.insert(key.into());
        }
        splinter
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
    pub fn from_bytes(data: T) -> Result<Self, Culprit<DecodeErr>> {
        use DecodeErr::*;

        let (header, _) = Ref::<_, Header>::from_prefix(data.as_ref()).map_err(|err| {
            debug_assert!(matches!(err, ConvertError::Size(_)));
            InvalidHeader
        })?;
        if header.magic != SPLINTER_MAGIC {
            return Err(InvalidMagic.into());
        }

        let (_, footer) = Ref::<_, Footer>::from_suffix(data.as_ref()).map_err(|err| {
            debug_assert!(matches!(err, ConvertError::Size(_)));
            InvalidFooter
        })?;
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
    ) -> PartitionRef<'_, U32, PartitionRef<'_, U32, PartitionRef<'_, U16, BlockRef<'_>>>> {
        let data = self.data.as_ref();
        let slice = &data[..data.len() - size_of::<Footer>()];
        PartitionRef::from_suffix(slice, self.partitions)
    }

    pub fn contains(&self, key: u32) -> bool {
        let (a, b, c, d) = segments(key);

        if let Some(partition) = self.load_partitions().get(a) {
            if let Some(partition) = partition.get(b) {
                if let Some(block) = partition.get(c) {
                    return block.contains(d);
                }
            }
        }

        false
    }

    /// calculates the total number of values stored in the set
    pub fn cardinality(&self) -> usize {
        let mut sum = 0;
        for (_, partition) in self.load_partitions().sorted_iter() {
            for (_, partition) in partition.sorted_iter() {
                sum += partition.cardinality();
            }
        }
        sum
    }

    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.load_partitions()
            .into_iter()
            .flat_map(|(a, p)| p.into_iter().map(move |(b, p)| (a, b, p)))
            .flat_map(|(a, b, p)| p.into_iter().map(move |(c, p)| (a, b, c, p)))
            .flat_map(|(a, b, c, p)| p.into_segments().map(move |d| combine_segments(a, b, c, d)))
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

impl<T: AsRef<[u8]>> From<SplinterRef<T>> for Splinter {
    fn from(value: SplinterRef<T>) -> Self {
        value.copy_to_owned()
    }
}

impl<T: AsRef<[u8]>> CopyToOwned for SplinterRef<T> {
    type Owned = Splinter;

    fn copy_to_owned(&self) -> Self::Owned {
        let partitions = self.load_partitions().copy_to_owned();
        Splinter { partitions }
    }
}

/// split the key into 4 8-bit segments
#[inline]
fn segments(key: u32) -> (Segment, Segment, Segment, Segment) {
    let [a, b, c, d] = key.to_be_bytes();
    (a, b, c, d)
}

#[inline]
fn combine_segments(a: Segment, b: Segment, c: Segment, d: Segment) -> u32 {
    u32::from_be_bytes([a, b, c, d])
}

#[cfg(test)]
mod tests {
    use std::{io, ops::RangeInclusive};

    use crate::testutil::{mksplinter, mksplinter_ref};

    use super::*;
    use itertools::iproduct;
    use roaring::RoaringBitmap;

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
                panic!("missing key: {i}");
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
                splinter_ref.copy_to_owned(),
                "Splinter == Splinter"
            );
            assert_eq!(
                splinter_ref.copy_to_owned().serialize_to_bytes(),
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
                panic!("missing key: {i}");
            }
        }

        // check that the splinter can enumerate all keys
        assert!(itertools::equal(values, splinter.iter()));

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
            baseline: usize,
            //        (actual, expected)
            splinter: (usize, usize),
            roaring: (usize, usize),
        }

        let mut reports = vec![];

        let mut run_test = |name: &'static str,
                            set: Vec<u32>,
                            expected_splinter: usize,
                            expected_roaring: usize| {
            let data = mksplinter(set.clone()).serialize_to_bytes();
            reports.push(Report {
                name,
                baseline: set.len() * std::mem::size_of::<u32>(),
                splinter: (data.len(), expected_splinter),
                roaring: (roaring_size(set), expected_roaring),
            });
        };

        #[track_caller]
        fn mkset(
            high: RangeInclusive<u8>,
            mid: RangeInclusive<u8>,
            low: RangeInclusive<u8>,
            block: RangeInclusive<u8>,
            expected_len: usize,
        ) -> Vec<u32> {
            let out: Vec<u32> = iproduct!(high, mid, low, block)
                .map(|(a, b, c, d)| u32::from_be_bytes([a, b, c, d]))
                .collect();
            assert_eq!(out.len(), expected_len);
            out
        }

        // empty splinter
        run_test("empty", vec![], 8, 8);

        // 1 element in set
        let set = mkset(0..=0, 0..=0, 0..=0, 0..=0, 1);
        run_test("1 element", set, 25, 18);

        // 1 fully dense block
        let set = mkset(0..=0, 0..=0, 0..=0, 0..=255, 256);
        run_test("1 dense block", set, 24, 528);

        // 1 half full block
        let set = mkset(0..=0, 0..=0, 0..=0, 0..=127, 128);
        run_test("1 half full block", set, 56, 272);

        // 1 sparse block
        let set = mkset(0..=0, 0..=0, 0..=0, 0..=15, 16);
        run_test("1 sparse block", set, 40, 48);

        // 8 half full blocks
        let set = mkset(0..=0, 0..=0, 0..=7, 0..=127, 1024);
        run_test("8 half full blocks", set, 308, 2064);

        // 8 sparse blocks
        let set = mkset(0..=0, 0..=0, 0..=7, 0..=1, 16);
        run_test("8 sparse blocks", set, 68, 48);

        // 64 half full blocks
        let set = mkset(0..=3, 0..=3, 0..=3, 0..=127, 8192);
        run_test("64 half full blocks", set, 2432, 16520);

        // 64 sparse blocks
        let set = mkset(0..=3, 0..=3, 0..=3, 0..=1, 128);
        run_test("64 sparse blocks", set, 512, 392);

        // 256 half full blocks
        let set = mkset(0..=3, 0..=7, 0..=7, 0..=127, 32768);
        run_test("256 half full blocks", set, 9440, 65800);

        // 256 sparse blocks
        let set = mkset(0..=3, 0..=7, 0..=7, 0..=1, 512);
        run_test("256 sparse blocks", set, 1760, 1288);

        // 512 half full blocks
        let set = mkset(0..=7, 0..=7, 0..=7, 0..=127, 65536);
        run_test("512 half full blocks", set, 18872, 131592);

        // 512 sparse blocks
        let set = mkset(0..=7, 0..=7, 0..=7, 0..=1, 1024);
        run_test("512 sparse blocks", set, 3512, 2568);

        // the rest of the compression tests use 4k elements
        let elements = 4096;

        // fully dense splinter
        let set = mkset(0..=0, 0..=0, 0..=15, 0..=255, elements);
        run_test("fully dense", set, 84, 8208);

        // 128 elements per block; dense partitions
        let set = mkset(0..=0, 0..=0, 0..=31, 0..=127, elements);
        run_test("128/block; dense", set, 1172, 8208);

        // 32 elements per block; dense partitions
        let set = mkset(0..=0, 0..=0, 0..=127, 0..=31, elements);
        run_test("32/block; dense", set, 4532, 8208);

        // 16 element per block; dense low partitions
        let set = mkset(0..=0, 0..=0, 0..=255, 0..=15, elements);
        run_test("16/block; dense", set, 4884, 8208);

        // 128 elements per block; sparse mid partitions
        let set = mkset(0..=0, 0..=31, 0..=0, 0..=127, elements);
        run_test("128/block; sparse mid", set, 1358, 8456);

        // 128 elements per block; sparse high partitions
        let set = mkset(0..=31, 0..=0, 0..=0, 0..=127, elements);
        run_test("128/block; sparse high", set, 1544, 8456);

        // 1 element per block; sparse mid partitions
        let set = mkset(0..=0, 0..=255, 0..=15, 0..=0, elements);
        run_test("1/block; sparse mid", set, 21774, 10248);

        // 1 element per block; sparse high partitions
        let set = mkset(0..=255, 0..=15, 0..=0, 0..=0, elements);
        run_test("1/block; sparse high", set, 46344, 40968);

        let mut fail_test = false;

        println!(
            "{:30} {:12} {:>6} {:>10} {:>10} {:>10}",
            "test", "bitmap", "size", "expected", "relative", "ok"
        );
        for report in reports {
            println!(
                "{:30} {:12} {:6} {:10} {:>10} {:>10}",
                report.name,
                "Splinter",
                report.splinter.0,
                report.splinter.1,
                "1.00",
                if report.splinter.0 == report.splinter.1 {
                    "ok"
                } else {
                    fail_test = true;
                    "FAIL"
                }
            );
            let diff = report.roaring.0 as f64 / report.splinter.0 as f64;
            println!(
                "{:30} {:12} {:6} {:10} {:>10.2} {:>10}",
                "",
                "Roaring",
                report.roaring.0,
                report.roaring.1,
                diff,
                if report.roaring.0 != report.roaring.1 {
                    fail_test = true;
                    "FAIL"
                } else if diff < 1.0 {
                    "<"
                } else {
                    "ok"
                }
            );
            let diff = report.baseline as f64 / report.splinter.0 as f64;
            println!(
                "{:30} {:12} {:6} {:10} {:>10.2} {:>10}",
                "",
                "Baseline",
                report.baseline,
                report.baseline,
                diff,
                if report.splinter.0 <= report.baseline {
                    "ok"
                } else {
                    // we don't fail the test, just report for informational purposes;
                    "<"
                }
            );
        }

        assert!(!fail_test, "compression test failed");
    }
}
