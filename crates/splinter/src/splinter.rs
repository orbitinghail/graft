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
    relational::Relation,
    util::{CopyToOwned, FromSuffix, SerializeContainer},
    DecodeErr,
};

mod cmp;
mod cut;
mod intersection;
mod union;

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

#[derive(Default, Clone)]
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

    pub fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.partitions
            .sorted_iter()
            .flat_map(|(h, p)| p.sorted_iter().map(move |(m, b)| (h, m, b)))
            .flat_map(|(h, m, b)| b.segments().map(move |l| combine_segments(h, m, l)))
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

impl<T: AsRef<[u8]>> From<SplinterRef<T>> for Splinter {
    fn from(value: SplinterRef<T>) -> Self {
        value.into_splinter()
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
fn combine_segments(high: u8, mid: u8, low: u8) -> u32 {
    (high as u32) << 16 | (mid as u32) << 8 | low as u32
}

#[cfg(test)]
mod tests {
    use std::io;

    use crate::testutil::{mksplinter, mksplinter_ref};

    use super::*;
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
        let set = (0..=1024).step_by(128).collect::<Vec<_>>();
        run_test("8 sparse blocks", set, 43, 34);

        // 128 sparse blocks
        let set = (0..=16384).step_by(128).collect::<Vec<_>>();
        run_test("128 sparse blocks", set, 370, 274);

        // 512 sparse blocks
        let set = (0..=65536).step_by(128).collect::<Vec<_>>();
        run_test("512 sparse blocks", set, 1337, 1050);

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
