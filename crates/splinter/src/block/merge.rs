use either::Either::{Left, Right};

use crate::{
    bitmap::{BITMAP_SIZE, BitmapExt, BitmapMutExt},
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
        match rhs.resolve_bitmap() {
            Left(bitmap) => self.bitmap.merge(bitmap),
            Right(segments) => {
                for &segment in segments {
                    self.insert(segment);
                }
            }
        }
    }
}
