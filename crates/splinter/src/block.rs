use std::array::TryFromSliceError;

use bytes::BufMut;
use either::Either;

use crate::{
    bitmap::{Bitmap, BitmapExt, BitmapMutExt, BITMAP_SIZE},
    util::{CopyToOwned, FromSuffix, SerializeContainer},
    Segment,
};

mod cmp;
mod cut;
mod intersection;
mod merge;
mod union;

#[derive(Clone)]
pub struct Block {
    bitmap: Bitmap,
}

impl From<Bitmap> for Block {
    fn from(bitmap: Bitmap) -> Self {
        Self { bitmap }
    }
}

impl Default for Block {
    fn default() -> Self {
        Self { bitmap: [0; BITMAP_SIZE] }
    }
}

impl TryFrom<&[u8]> for Block {
    type Error = TryFromSliceError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let bitmap: Bitmap = value.try_into()?;
        Ok(Self { bitmap })
    }
}

impl BitmapExt for Block {
    fn as_ref(&self) -> &Bitmap {
        &self.bitmap
    }
}

impl BitmapMutExt for Block {
    fn as_mut(&mut self) -> &mut Bitmap {
        &mut self.bitmap
    }
}

impl SerializeContainer for Block {
    fn should_serialize(&self) -> bool {
        self.bitmap.has_bits_set()
    }

    /// Serialize the block to the output buffer returning the block's cardinality
    /// and number of bytes written.
    fn serialize<B: BufMut>(&self, out: &mut B) -> (usize, usize) {
        let cardinality = self.cardinality();

        let bytes_written = if cardinality < 32 {
            for segment in self.bitmap.segments() {
                out.put_u8(segment);
            }
            cardinality
        } else {
            // write out the bitmap verbatim
            out.put_slice(&self.bitmap);
            BITMAP_SIZE
        };

        (cardinality, bytes_written)
    }
}

impl FromIterator<Segment> for Block {
    fn from_iter<T: IntoIterator<Item = Segment>>(iter: T) -> Self {
        let mut block = Block::default();
        for segment in iter {
            block.insert(segment);
        }
        block
    }
}

#[derive(Clone)]
pub struct BlockRef<'a> {
    segments: &'a [Segment],
}

impl<'a> BlockRef<'a> {
    #[inline]
    pub fn from_bytes(segments: &'a [Segment]) -> Self {
        assert!(segments.len() <= 32, "segments overflow");
        Self { segments }
    }

    /// If this block is a bitmap, return the bitmap, otherwise return None
    #[inline]
    pub(crate) fn bitmap(&self) -> Option<&Bitmap> {
        (*self.segments).try_into().ok()
    }

    #[inline]
    pub fn segments(&self) -> impl Iterator<Item = Segment> + '_ {
        if let Some(bitmap) = self.bitmap() {
            Either::Left(bitmap.segments())
        } else {
            Either::Right(self.segments.iter().copied())
        }
    }

    #[inline]
    pub fn into_segments(self) -> impl Iterator<Item = Segment> + 'a {
        if self.segments.len() == BITMAP_SIZE {
            let bitmap = TryInto::<&Bitmap>::try_into(self.segments).unwrap();
            Either::Left(bitmap.into_segments())
        } else {
            Either::Right(self.segments.iter().copied())
        }
    }

    #[cfg(test)]
    #[inline]
    pub fn cardinality(&self) -> usize {
        if let Some(bitmap) = self.bitmap() {
            bitmap.cardinality()
        } else {
            self.segments.len()
        }
    }

    #[cfg(test)]
    #[inline]
    pub fn last(&self) -> Option<Segment> {
        if let Some(bitmap) = self.bitmap() {
            bitmap.last()
        } else {
            self.segments.last().copied()
        }
    }

    /// Count the number of 1-bits in the block up to and including the `position``
    pub fn rank(&self, position: u8) -> usize {
        if let Some(bitmap) = self.bitmap() {
            bitmap.rank(position)
        } else {
            match self.segments.binary_search(&position) {
                Ok(i) => i + 1,
                Err(i) => i,
            }
        }
    }

    #[inline]
    pub fn contains(&self, segment: Segment) -> bool {
        if let Some(bitmap) = self.bitmap() {
            bitmap.contains(segment)
        } else {
            self.segments.iter().any(|&x| x == segment)
        }
    }
}

impl<'a> CopyToOwned for BlockRef<'a> {
    type Owned = Block;

    fn copy_to_owned(&self) -> Self::Owned {
        if let Some(bitmap) = self.bitmap() {
            bitmap.to_owned().into()
        } else {
            self.segments().collect()
        }
    }
}

impl<'a> FromSuffix<'a> for BlockRef<'a> {
    fn from_suffix(data: &'a [u8], cardinality: usize) -> Self {
        let size = block_size(cardinality);
        assert!(data.len() >= size, "data too short");
        let (_, block) = data.split_at(data.len() - size);
        Self::from_bytes(block)
    }
}

#[inline]
pub fn block_size(cardinality: usize) -> usize {
    cardinality.min(BITMAP_SIZE)
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    fn test_block(
        values: impl IntoIterator<Item = Segment>,
        cb_block: impl FnOnce(Block),
        cb_ref: impl FnOnce(BlockRef<'_>),
    ) {
        let block: Block = values.into_iter().collect();
        cb_block(block.clone());
        let mut segments = BytesMut::new();
        block.serialize(&mut segments);
        let block_ref = BlockRef::from_bytes(&segments);
        cb_ref(block_ref);
    }

    macro_rules! assert_block_fn_eq {
        ($values:expr, $expected:expr, |$b:ident| $fn:block) => {{
            test_block(
                $values,
                |$b| {
                    assert_eq!($fn, $expected);
                },
                |$b| {
                    assert_eq!($fn, $expected);
                },
            );
        }};
    }

    #[test]
    fn test_block_last() {
        // empty block
        assert_block_fn_eq!(0..0, None, |b| { b.last() });

        // block with 1 element
        assert_block_fn_eq!(0..1, Some(0), |b| { b.last() });
        assert_block_fn_eq!(33..34, Some(33), |b| { b.last() });
        assert_block_fn_eq!(128..129, Some(128), |b| { b.last() });

        // block with 31 elements; stored as a list
        assert_block_fn_eq!(0..31, Some(30), |b| { b.last() });
        assert_block_fn_eq!(1..32, Some(31), |b| { b.last() });
        assert_block_fn_eq!(100..131, Some(130), |b| { b.last() });

        // block with > 32 elements; stored as a bitmap
        assert_block_fn_eq!(0..32, Some(31), |b| { b.last() });
        assert_block_fn_eq!(1..33, Some(32), |b| { b.last() });
        assert_block_fn_eq!(21..131, Some(130), |b| { b.last() });
        assert_block_fn_eq!(0..=255, Some(255), |b| { b.last() });
    }

    #[test]
    fn test_block_rank() {
        // empty block
        assert_block_fn_eq!(0..0, 0, |b| { b.rank(0) });
        assert_block_fn_eq!(0..0, 0, |b| { b.rank(128) });
        assert_block_fn_eq!(0..0, 0, |b| { b.rank(255) });

        // block with 1 element
        assert_block_fn_eq!(0..1, 1, |b| { b.rank(0) });
        assert_block_fn_eq!(0..1, 1, |b| { b.rank(128) });
        assert_block_fn_eq!(128..129, 0, |b| { b.rank(0) });

        // block with 31 elements; stored as a list
        assert_block_fn_eq!(0..31, 31, |b| { b.cardinality() });
        for i in 0usize..31 {
            assert_block_fn_eq!(0..31, i + 1, |b| { b.rank(i as Segment) });
        }

        // block with 32 elements; stored as a bitmap
        assert_block_fn_eq!(0..32, 32, |b| { b.cardinality() });
        for i in 0usize..32 {
            assert_block_fn_eq!(0..32, i + 1, |b| { b.rank(i as Segment) });
        }
        for i in 32..255 {
            assert_block_fn_eq!(0..32, 32, |b| { b.rank(i as Segment) });
        }

        // full block
        assert_block_fn_eq!(0..=255, 256, |b| { b.cardinality() });
        for i in 0usize..255 {
            assert_block_fn_eq!(0..=255, i + 1, |b| { b.rank(i as Segment) });
        }
    }

    #[test]
    fn test_block_contains() {
        // empty block
        assert_block_fn_eq!(0..0, false, |b| { b.contains(0) });
        assert_block_fn_eq!(0..0, false, |b| { b.contains(128) });
        assert_block_fn_eq!(0..0, false, |b| { b.contains(255) });

        // block with 1 element
        assert_block_fn_eq!(0..1, true, |b| { b.contains(0) });
        assert_block_fn_eq!(0..1, false, |b| { b.contains(128) });
        assert_block_fn_eq!(128..129, false, |b| { b.contains(0) });

        // block with 31 elements; stored as a list
        assert_block_fn_eq!(0..31, 31, |b| { b.cardinality() });
        for i in 0..255 {
            assert_block_fn_eq!(0..31, i < 31, |b| { b.contains(i) });
        }

        // block with 32 elements; stored as a bitmap
        assert_block_fn_eq!(0..32, 32, |b| { b.cardinality() });
        for i in 0..255 {
            assert_block_fn_eq!(0..32, i < 32, |b| { b.contains(i) });
        }

        // full block
        assert_block_fn_eq!(0..=255, 256, |b| { b.cardinality() });
        for i in 0..255 {
            assert_block_fn_eq!(0..=255, true, |b| { b.contains(i) });
        }
    }
}
