use std::ops::Deref;

use crate::{
    bitmap::{BitmapExt, BITMAP_SIZE},
    ops::Merge,
    Segment,
};

use super::{Block, BlockRef};

// This implementation covers Block and Bitmap
impl<L: BitmapExt, R: BitmapExt> Merge<R> for L {
    fn merge(&mut self, rhs: &R) {
        let l = self.as_mut();
        let r = rhs.as_ref();
        for i in 0..BITMAP_SIZE {
            l[i] |= r[i];
        }
    }
}

// Block <> BlockRef
impl<T: Deref<Target = [Segment]>> Merge<BlockRef<T>> for Block {
    fn merge(&mut self, rhs: &BlockRef<T>) {
        if let Some(rhs_bitmap) = rhs.bitmap() {
            self.bitmap.merge(rhs_bitmap);
        } else {
            for &segment in rhs.segments.iter() {
                self.insert(segment);
            }
        }
    }
}
