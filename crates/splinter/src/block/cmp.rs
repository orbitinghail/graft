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
impl<'a, 'b> PartialEq<BlockRef<'b>> for BlockRef<'a> {
    fn eq(&self, other: &BlockRef<'b>) -> bool {
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
impl<'a> PartialEq<Block> for BlockRef<'a> {
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
