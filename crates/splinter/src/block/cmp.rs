use std::ops::Deref;

use crate::{bitmap::BitmapExt, Segment};

use super::{Block, BlockRef};

// Block == Block
impl PartialEq<Block> for Block {
    fn eq(&self, other: &Block) -> bool {
        self.bitmap == other.bitmap
    }
}

// BlockRef == BlockRef
impl<T1, T2> PartialEq<BlockRef<T2>> for BlockRef<T1>
where
    T1: Deref<Target = [Segment]>,
    T2: Deref<Target = [Segment]>,
{
    fn eq(&self, other: &BlockRef<T2>) -> bool {
        self.segments.deref() == other.segments.deref()
    }
}

// BlockRef == Block
impl<T: Deref<Target = [Segment]>> PartialEq<Block> for BlockRef<T> {
    fn eq(&self, other: &Block) -> bool {
        if let Some(bitmap) = self.bitmap() {
            bitmap == &other.bitmap
        } else {
            self.segments.iter().copied().eq(other.bitmap.segments())
        }
    }
}

// Block == BlockRef
impl<T: Deref<Target = [Segment]>> PartialEq<BlockRef<T>> for Block {
    #[inline]
    fn eq(&self, other: &BlockRef<T>) -> bool {
        other == self
    }
}
