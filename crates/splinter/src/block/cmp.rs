use either::Either::{Left, Right};

use crate::bitmap::BitmapExt;

use super::{Block, BlockRef};

// Block == Block
impl PartialEq<Block> for Block {
    fn eq(&self, other: &Block) -> bool {
        self.bitmap == other.bitmap
    }
}

// BlockRef == BlockRef
impl<'a> PartialEq<BlockRef<'a>> for BlockRef<'_> {
    fn eq(&self, other: &BlockRef<'a>) -> bool {
        use BlockRef::*;

        match (self, other) {
            (Partial { segments }, Partial { segments: other }) => segments == other,
            (Partial { .. }, Full) => false,
            (Full, Partial { .. }) => false,
            (Full, Full) => true,
        }
    }
}

// BlockRef == Block
impl PartialEq<Block> for BlockRef<'_> {
    fn eq(&self, other: &Block) -> bool {
        match self.resolve_bitmap() {
            Left(bitmap) => bitmap == &other.bitmap,
            Right(segments) => segments.iter().copied().eq(other.bitmap.segments()),
        }
    }
}

// Block == BlockRef
impl<'a> PartialEq<BlockRef<'a>> for Block {
    #[inline]
    fn eq(&self, other: &BlockRef<'a>) -> bool {
        other == self
    }
}
