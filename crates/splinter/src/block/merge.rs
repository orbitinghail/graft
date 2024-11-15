use crate::{
    bitmap::{BitmapExt, BitmapMutExt, BITMAP_SIZE},
    ops::Merge,
};

use super::{Block, BlockRef};

// This implementation covers Block and Bitmap
impl<L: BitmapMutExt, R: BitmapExt> Merge<R> for L {
    fn merge(&mut self, rhs: &R) {
        let l = self.as_mut();
        let r = rhs.as_ref();
        for i in 0..BITMAP_SIZE {
            l[i] |= r[i];
        }
    }
}

// Block <> BlockRef
impl<'a> Merge<BlockRef<'a>> for Block {
    fn merge(&mut self, rhs: &BlockRef<'a>) {
        if let Some(rhs_bitmap) = rhs.bitmap() {
            self.bitmap.merge(rhs_bitmap);
        } else {
            for &segment in rhs.segments.iter() {
                self.insert(segment);
            }
        }
    }
}
