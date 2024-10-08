use std::{fmt::Debug, marker::PhantomData, mem};

use bytes::BufMut;
use zerocopy::{
    little_endian::{U16, U32},
    Immutable, IntoBytes,
};

use crate::{block_bit, block_key, segments, Footer, Header, MAX_CARDINALITY};

type Segment = u8;
static_assertions::assert_eq_size!(Segment, u8);

type Cardinality = usize;

pub(super) trait ContainerWriter {
    /// Append a segment to the container returning the number of bytes written to the output
    fn push<B: BufMut>(&mut self, out: &mut B, segments: &[Segment]) -> usize;

    /// Flush the current container to the output returning the cardinality and
    /// number of bytes written
    fn flush<B: BufMut>(&mut self, out: &mut B) -> (Cardinality, usize);
}

pub(super) struct BlockWriter {
    keys: Vec<Segment>,
}

impl Default for BlockWriter {
    fn default() -> Self {
        Self {
            keys: Vec::with_capacity(Segment::MAX as usize),
        }
    }
}

impl BlockWriter {
    pub(super) fn push(&mut self, key: Segment) {
        assert!(
            self.keys.last().map_or(true, |&last| last < key),
            "keys must be appended in order"
        );
        assert!(self.keys.len() < MAX_CARDINALITY, "block overflow");
        self.keys.push(key);
    }
}

impl ContainerWriter for BlockWriter {
    fn push<B: BufMut>(&mut self, _out: &mut B, segments: &[Segment]) -> usize {
        let (key, rest) = segments.split_first().expect("empty segments");
        assert!(rest.is_empty(), "extra segments not allowed");
        self.push(*key);
        0
    }

    fn flush<B: BufMut>(&mut self, out: &mut B) -> (Cardinality, usize) {
        let cardinality = self.keys.len();
        assert!(cardinality <= MAX_CARDINALITY, "cardinality overflow");

        let bytes_written = if cardinality < 32 {
            out.put_slice(&self.keys);
            self.keys.len()
        } else {
            let mut bitmap = [0u8; 32];
            for &segment in &self.keys {
                let key = block_key(segment);
                let bit = block_bit(segment);
                bitmap[key] |= 1 << bit;
            }
            out.put_slice(&bitmap);
            bitmap.len()
        };

        // reset the buffer
        self.keys.clear();

        (cardinality, bytes_written)
    }
}

#[derive(Default)]
struct IndexWriter {
    keys: BlockWriter,
    cardinalities: Vec<u8>,
    offsets: Vec<u32>,
}

impl IndexWriter {
    fn append(&mut self, segment: Segment, cardinality: Cardinality, offset: u32) {
        self.keys.push(segment);
        self.cardinalities.push((cardinality - 1) as u8);
        self.offsets.push(offset);
    }

    fn flush<B, O>(&mut self, out: &mut B, offset_base: u32) -> (Cardinality, usize)
    where
        O: IntoBytes + TryFrom<u32, Error: Debug> + Immutable,
        B: BufMut,
    {
        let (cardinality, mut n) = self.keys.flush(out);

        let cardinalities_bytes = self.cardinalities.as_bytes();
        out.put_slice(cardinalities_bytes);
        n += cardinalities_bytes.len();
        self.cardinalities.clear();

        for offset in self.offsets.drain(..) {
            assert!(offset <= offset_base, "offset out of range");
            let offset = O::try_from(offset_base - offset).expect("offset overflow");
            let offset_bytes = offset.as_bytes();
            out.put_slice(offset_bytes);
            n += offset_bytes.len();
        }

        (cardinality, n)
    }
}

struct MapWriter<V, StoredOffset> {
    index: IndexWriter,

    // current key
    key: Segment,

    // value offset
    offset: u32,

    // value writer
    value_writer: V,

    _phantom: PhantomData<StoredOffset>,
}

impl<V, O> Default for MapWriter<V, O>
where
    V: Default,
{
    fn default() -> Self {
        Self {
            index: Default::default(),
            key: 0,
            offset: 0,
            value_writer: Default::default(),
            _phantom: PhantomData,
        }
    }
}

impl<V, O> MapWriter<V, O>
where
    V: ContainerWriter,
    O: IntoBytes + TryFrom<u32, Error: Debug> + Immutable,
{
    fn flush_value<B: BufMut>(&mut self, out: &mut B, key: Segment) -> usize {
        let (cardinality, n) = self.value_writer.flush(out);
        self.offset += n as u32;
        self.index.append(key, cardinality, self.offset);
        n
    }
}

impl<V, O> ContainerWriter for MapWriter<V, O>
where
    V: ContainerWriter,
    O: IntoBytes + TryFrom<u32, Error: Debug> + Immutable,
{
    fn push<B: BufMut>(&mut self, out: &mut B, segments: &[Segment]) -> usize {
        let (key, rest) = segments.split_first().expect("empty segments");

        let flush_n = if self.key != *key {
            assert!(self.key < *key, "keys must be appended in order");
            let key = mem::replace(&mut self.key, *key);
            self.flush_value(out, key)
        } else {
            0
        };

        let value_n = self.value_writer.push(out, rest);
        self.offset += value_n as u32;

        flush_n + value_n
    }

    fn flush<B: BufMut>(&mut self, out: &mut B) -> (Cardinality, usize) {
        let n = self.flush_value(out, self.key);
        let (cardinality, m) = self.index.flush::<_, O>(out, self.offset);

        // reset state
        self.key = 0;
        self.offset = 0;

        (cardinality, n + m)
    }
}

pub struct SplinterBuilder<B> {
    out: B,
    partitions: MapWriter<MapWriter<BlockWriter, U16>, U32>,
    count: usize,
}

impl<B: Default + BufMut> Default for SplinterBuilder<B> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<B: BufMut> SplinterBuilder<B> {
    pub fn new(mut out: B) -> Self {
        out.put_slice(Header::DEFAULT.as_bytes());
        Self {
            out,
            partitions: Default::default(),
            count: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn push(&mut self, key: u32) {
        let (high, mid, low) = segments(key);
        self.partitions.push(&mut self.out, &[high, mid, low]);
        self.count += 1;
    }

    pub fn build(mut self) -> B {
        let (cardinality, _) = self.partitions.flush(&mut self.out);
        assert!(cardinality <= MAX_CARDINALITY, "cardinality overflow");
        let footer = Footer::new(cardinality as u16);
        self.out.put_slice(footer.as_bytes());
        self.out
    }
}
