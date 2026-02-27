use crate::Segment;

pub const BITMAP_SIZE: usize = 32;
pub const BITMAP_FULL: Bitmap = [0xff; BITMAP_SIZE];

pub type Bitmap = [u8; BITMAP_SIZE];

pub trait BitmapMutExt {
    fn as_mut(&mut self) -> &mut Bitmap;

    /// Insert a segment into the bitmap, returning true if a bit was set
    #[inline]
    fn insert(&mut self, segment: Segment) -> bool {
        let key = bitmap_key(segment);
        let bit = bitmap_bit(segment);
        let key = &mut self.as_mut()[key];
        let was_missing = *key & (1 << bit) == 0;
        *key |= 1 << bit;
        was_missing
    }
}

pub trait BitmapExt
where
    Self: Sized,
{
    fn as_ref(&self) -> &Bitmap;

    #[inline]
    fn has_bits_set(&self) -> bool {
        self.as_ref().iter().any(|&x| x != 0)
    }

    #[inline]
    fn cardinality(&self) -> usize {
        self.as_ref().iter().map(|&x| x.count_ones() as usize).sum()
    }

    /// Return the last segment in the bitmap
    #[inline]
    fn last(&self) -> Option<Segment> {
        // Traverse the bitmap from the last byte to the first
        for (byte_idx, &byte) in self.as_ref().iter().enumerate().rev() {
            if byte != 0 {
                // If we found a non-zero byte, we need to find the most significant bit set
                // Find the position of the most significant set bit in this byte
                let last_bit_pos = 7 - byte.leading_zeros() as usize;
                // Return the absolute bit position in the 256-bit bitmap
                let pos = byte_idx * 8 + last_bit_pos;
                debug_assert!(pos < 256);
                return Some(pos as u8);
            }
        }
        None // If all bits are 0
    }

    /// Count the number of 1-bits in the block up to and including the `position``
    #[inline]
    fn rank(&self, position: u8) -> usize {
        let key = bitmap_key(position);

        // number of bits set up to the key-th byte
        let prefix_bits = self.as_ref()[0..key]
            .iter()
            .map(|&x| x.count_ones())
            .sum::<u32>();

        // number of bits set up to the bit-th bit in the key-th byte
        let bit = bitmap_bit(position) as u32;
        let bits = (self.as_ref()[key] << (7 - bit)).count_ones();

        (prefix_bits + bits) as usize
    }

    #[inline]
    fn contains(&self, segment: Segment) -> bool {
        self.as_ref()[bitmap_key(segment)] & (1 << bitmap_bit(segment)) != 0
    }

    #[inline]
    fn segments(&self) -> BitmapSegmentsIter<&Bitmap> {
        BitmapSegmentsIter::new(self.as_ref())
    }

    #[inline]
    fn into_segments(self) -> BitmapSegmentsIter<Self> {
        BitmapSegmentsIter::new(self)
    }
}

impl BitmapExt for Bitmap {
    #[inline]
    fn as_ref(&self) -> &Bitmap {
        self
    }
}

impl BitmapMutExt for Bitmap {
    #[inline]
    fn as_mut(&mut self) -> &mut Bitmap {
        self
    }
}

impl BitmapExt for &Bitmap {
    fn as_ref(&self) -> &Bitmap {
        self
    }
}

pub struct BitmapSegmentsIter<T> {
    bitmap: T,
    cursor: usize,
    current: u8,
}

impl<T: BitmapExt> BitmapSegmentsIter<T> {
    pub fn new(bitmap: T) -> Self {
        let current = bitmap.as_ref()[0];
        Self { bitmap, cursor: 0, current }
    }
}

impl<T: BitmapExt> Iterator for BitmapSegmentsIter<T> {
    type Item = Segment;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current == 0 {
            if self.cursor == BITMAP_SIZE - 1 {
                return None;
            }
            self.cursor += 1;
            self.current = self.bitmap.as_ref()[self.cursor];
        }
        let segment = (self.current.trailing_zeros() + (8 * self.cursor as u32)) as Segment;
        self.current &= self.current - 1;
        Some(segment)
    }
}

/// Return the byte position of the segment in the bitmap
#[inline]
fn bitmap_key(segment: u8) -> usize {
    segment as usize / 8
}

/// Return the bit position of the segment in the byte
#[inline]
fn bitmap_bit(segment: u8) -> u8 {
    segment % 8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitmap_segments() {
        let mut bmp = Bitmap::default();
        assert!(bmp.segments().next().is_none());

        bmp.insert(0);
        assert!(bmp.segments().eq(0..=0));

        bmp.insert(1);
        assert!(bmp.segments().eq(0..=1));

        for i in 0..=10 {
            bmp.insert(i);
        }
        assert!(bmp.segments().eq(0..=10));

        let mut bmp = Bitmap::default();
        for i in 250..=255 {
            bmp.insert(i);
        }
        assert!(bmp.segments().eq(250..=255));

        for i in 0..=255 {
            bmp.insert(i);
        }
        assert!(bmp.segments().eq(0..=255));
    }
}
