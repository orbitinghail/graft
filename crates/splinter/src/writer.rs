use std::{
    fmt::Debug,
    io::{self, Write},
    marker::PhantomData,
    mem,
};

use zerocopy::{
    little_endian::{U16, U32},
    Immutable, IntoBytes,
};

use crate::{block_bit, block_key, segments, Footer, Header, MAX_CARDINALITY};

type Segment = u8;
static_assertions::assert_eq_size!(Segment, u8);

type Cardinality = usize;

trait ContainerWriter {
    /// Append a segment to the container returning the number of bytes written to the writer
    fn push<W: Write>(&mut self, out: &mut W, segments: &[Segment]) -> io::Result<usize>;

    /// Flush the current container to the writer returning the cardinality and
    /// number of bytes written
    fn flush<W: Write>(&mut self, out: &mut W) -> io::Result<(Cardinality, usize)>;
}

struct BlockWriter {
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
    fn push(&mut self, key: Segment) {
        assert!(self.keys.len() < MAX_CARDINALITY, "block overflow");
        self.keys.push(key);
    }
}

impl ContainerWriter for BlockWriter {
    fn push<W: Write>(&mut self, _out: &mut W, segments: &[Segment]) -> io::Result<usize> {
        let (key, rest) = segments.split_first().expect("empty segments");
        assert!(rest.is_empty(), "extra segments not allowed");
        self.push(*key);
        Ok(0)
    }

    fn flush<W: Write>(&mut self, out: &mut W) -> io::Result<(Cardinality, usize)> {
        let cardinality = self.keys.len();
        assert!(cardinality <= MAX_CARDINALITY, "cardinality overflow");

        let bytes_written = if cardinality < 32 {
            out.write_all(&self.keys)?;
            self.keys.len()
        } else {
            let mut bitmap = [0u8; 32];
            for &segment in &self.keys {
                let key = block_key(segment);
                let bit = block_bit(segment);
                bitmap[key] |= 1 << bit;
            }
            out.write_all(&bitmap)?;
            bitmap.len()
        };

        // reset the buffer
        self.keys.clear();

        Ok((cardinality, bytes_written))
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

    fn flush<W, O>(&mut self, out: &mut W, offset_base: u32) -> io::Result<(Cardinality, usize)>
    where
        O: IntoBytes + TryFrom<u32, Error: Debug> + Immutable,
        W: Write,
    {
        let (cardinality, mut n) = self.keys.flush(out)?;

        let cardinalities_bytes = self.cardinalities.as_bytes();
        out.write_all(cardinalities_bytes)?;
        n += cardinalities_bytes.len();

        for offset in self.offsets.drain(..) {
            assert!(offset <= offset_base, "offset out of range");
            let offset = O::try_from(offset_base - offset).expect("offset overflow");
            let offset_bytes = offset.as_bytes();
            out.write_all(offset_bytes)?;
            n += offset_bytes.len();
        }

        Ok((cardinality, n))
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
    fn flush_value<W: Write>(&mut self, out: &mut W, key: Segment) -> io::Result<usize> {
        let (cardinality, n) = self.value_writer.flush(out)?;
        self.offset += n as u32;
        self.index.append(key, cardinality, self.offset);
        Ok(n)
    }
}

impl<V, O> ContainerWriter for MapWriter<V, O>
where
    V: ContainerWriter,
    O: IntoBytes + TryFrom<u32, Error: Debug> + Immutable,
{
    fn push<W: Write>(&mut self, out: &mut W, segments: &[Segment]) -> io::Result<usize> {
        let (key, rest) = segments.split_first().expect("empty segments");

        let flush_n = if self.key != *key {
            assert!(self.key < *key, "keys must be appended in order");
            let key = mem::replace(&mut self.key, *key);
            self.flush_value(out, key)?
        } else {
            0
        };

        let value_n = self.value_writer.push(out, rest)?;
        self.offset += value_n as u32;

        Ok(flush_n + value_n)
    }

    fn flush<W: Write>(&mut self, out: &mut W) -> io::Result<(Cardinality, usize)> {
        let n = self.flush_value(out, self.key)?;
        let (cardinality, m) = self.index.flush::<_, O>(out, self.offset)?;

        // reset state
        self.key = 0;
        self.offset = 0;

        Ok((cardinality, n + m))
    }
}

pub struct SplinterWriter<W> {
    out: W,
    partitions: MapWriter<MapWriter<BlockWriter, U16>, U32>,
}

impl<W: Write> SplinterWriter<W> {
    pub fn new(mut out: W) -> io::Result<(usize, Self)> {
        out.write_all(Header::DEFAULT.as_bytes())?;
        Ok((
            Header::DEFAULT.as_bytes().len(),
            Self { out, partitions: Default::default() },
        ))
    }

    pub fn push(&mut self, key: u32) -> io::Result<usize> {
        let (high, mid, low) = segments(key);
        self.partitions.push(&mut self.out, &[high, mid, low])
    }

    pub fn finish(mut self) -> io::Result<(usize, W)> {
        let (cardinality, n) = self.partitions.flush(&mut self.out)?;
        assert!(cardinality <= MAX_CARDINALITY, "cardinality overflow");
        let footer = Footer::new(cardinality as u16);
        self.out.write_all(footer.as_bytes())?;
        Ok((n + footer.as_bytes().len(), self.out))
    }
}
