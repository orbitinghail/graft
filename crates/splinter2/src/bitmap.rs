use crate::Segment;

pub const BITMAP_SIZE: usize = 32;

pub type OwnedBitmap = [u8; BITMAP_SIZE];

pub trait Bitmap {
    fn as_ref(&self) -> &[u8; BITMAP_SIZE];

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
}

pub trait BitmapMut {
    fn as_mut(&mut self) -> &mut [u8; BITMAP_SIZE];

    #[inline]
    fn insert(&mut self, segment: Segment) {
        let key = bitmap_key(segment);
        let bit = bitmap_bit(segment);
        self.as_mut()[key] |= 1 << bit;
    }

    #[inline]
    fn clear(&mut self) {
        self.as_mut().fill(0);
    }
}

impl Bitmap for OwnedBitmap {
    #[inline]
    fn as_ref(&self) -> &[u8; BITMAP_SIZE] {
        self
    }
}

impl BitmapMut for OwnedBitmap {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8; BITMAP_SIZE] {
        self
    }
}

impl Bitmap for &[u8; BITMAP_SIZE] {
    #[inline]
    fn as_ref(&self) -> &[u8; BITMAP_SIZE] {
        self
    }
}

impl Bitmap for &mut [u8; BITMAP_SIZE] {
    #[inline]
    fn as_ref(&self) -> &[u8; BITMAP_SIZE] {
        self
    }
}

impl BitmapMut for &mut [u8; BITMAP_SIZE] {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8; BITMAP_SIZE] {
        self
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
