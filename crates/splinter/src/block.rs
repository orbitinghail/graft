use core::slice;
use std::{array::TryFromSliceError, iter::Copied, ops::Deref};

use bytes::BufMut;
use either::Either;

use crate::{
    bitmap::{Bitmap, BitmapExt, BitmapSegmentsIter, BITMAP_SIZE},
    util::{CopyToOwned, FromSuffix, SerializeContainer},
    Segment,
};

mod cmp;
mod cut;
mod intersection;

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

#[derive(Clone)]
pub struct BlockRef<T> {
    segments: T,
}

impl<T: Deref<Target = [Segment]>> BlockRef<T> {
    #[inline]
    pub fn from_bytes(segments: T) -> Self {
        assert!(segments.len() <= 32, "segments overflow");
        Self { segments }
    }

    /// If this block is a bitmap, return the bitmap, otherwise return None
    #[inline]
    fn bitmap(&self) -> Option<&[u8; BITMAP_SIZE]> {
        (*self.segments).try_into().ok()
    }

    #[inline]
    pub fn segments(&self) -> Either<BitmapSegmentsIter<'_>, Copied<slice::Iter<'_, Segment>>> {
        if let Some(bitmap) = self.bitmap() {
            Either::Left(bitmap.segments())
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

impl<T> CopyToOwned for BlockRef<T>
where
    T: Deref<Target = [Segment]>,
{
    type Owned = Block;

    fn copy_to_owned(&self) -> Self::Owned {
        if let Some(bitmap) = self.bitmap() {
            bitmap.to_owned().into()
        } else {
            let mut block = Block::default();
            for &segment in self.segments.iter() {
                block.insert(segment);
            }
            block
        }
    }
}

impl<'a> FromSuffix<'a> for BlockRef<&'a [u8]> {
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
    use bytes::{Bytes, BytesMut};

    use super::*;

    fn mkblock(values: impl IntoIterator<Item = u8>) -> BlockRef<Bytes> {
        let mut block = Block::default();
        for i in values {
            block.insert(i);
        }
        let mut buf = BytesMut::new();
        block.serialize(&mut buf);
        BlockRef::from_bytes(buf.freeze())
    }

    #[test]
    fn test_block_last() {
        // empty block
        assert_eq!(mkblock(0..0).last(), None);
        assert_eq!(mkblock(0..0).last(), None);
        assert_eq!(mkblock(0..0).last(), None);

        // block with 1 element
        assert_eq!(mkblock(0..1).last(), Some(0));
        assert_eq!(mkblock(33..34).last(), Some(33));
        assert_eq!(mkblock(128..129).last(), Some(128));

        // block with 31 elements; stored as a list
        assert_eq!(mkblock(0..31).last(), Some(30));
        assert_eq!(mkblock(1..32).last(), Some(31));
        assert_eq!(mkblock(100..131).last(), Some(130));

        // block with > 32 elements; stored as a bitmap
        assert_eq!(mkblock(0..32).last(), Some(31));
        assert_eq!(mkblock(1..33).last(), Some(32));
        assert_eq!(mkblock(21..131).last(), Some(130));
        assert_eq!(mkblock(0..=255).last(), Some(255));
    }

    #[test]
    fn test_block_rank() {
        // empty block
        assert_eq!(mkblock(0..0).rank(0), 0);
        assert_eq!(mkblock(0..0).rank(128), 0);
        assert_eq!(mkblock(0..0).rank(255), 0);

        // block with 1 element
        assert_eq!(mkblock(0..1).rank(0), 1);
        assert_eq!(mkblock(0..1).rank(128), 1);
        assert_eq!(mkblock(128..129).rank(0), 0);

        // block with 31 elements; stored as a list
        let block = mkblock(0..31);
        assert_eq!(block.cardinality(), 31);
        for i in 0..31 {
            assert_eq!(block.rank(i), (i + 1).into());
        }
        for i in 31..255 {
            assert_eq!(block.rank(i), 31);
        }

        // block with 32 elements; stored as a bitmap
        let block = mkblock(0..32);
        assert_eq!(block.cardinality(), 32);
        for i in 0..32 {
            assert_eq!(block.rank(i), (i + 1).into());
        }
        for i in 32..255 {
            assert_eq!(block.rank(i), 32);
        }

        // full block
        let block = mkblock(0..=255);
        assert_eq!(block.cardinality(), 256);
        for i in 0..255 {
            assert_eq!(block.rank(i), (i + 1).into());
        }
    }

    #[test]
    fn test_block_contains() {
        // empty block
        assert!(!mkblock(0..0).contains(0));
        assert!(!mkblock(0..0).contains(128));
        assert!(!mkblock(0..0).contains(255));

        // block with 1 element
        assert!(mkblock(0..1).contains(0));
        assert!(!mkblock(0..1).contains(128));
        assert!(!mkblock(128..129).contains(0));

        // block with 31 elements; stored as a list
        let block = mkblock(0..31);
        assert_eq!(block.cardinality(), 31);
        for i in 0..31 {
            assert!(block.contains(i));
        }
        for i in 31..255 {
            assert!(!block.contains(i));
        }

        // block with 32 elements; stored as a bitmap
        let block = mkblock(0..32);
        assert_eq!(block.cardinality(), 32);
        for i in 0..32 {
            assert!(block.contains(i));
        }
        for i in 32..255 {
            assert!(!block.contains(i));
        }

        // full block
        let block = mkblock(0..=255);
        assert_eq!(block.cardinality(), 256);
        for i in 0..255 {
            assert!(block.contains(i));
        }
    }
}
