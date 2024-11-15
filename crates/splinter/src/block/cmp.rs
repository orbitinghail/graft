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
        self.segments == other.segments
    }
}

// BlockRef == Block
impl<'a> PartialEq<Block> for BlockRef<'a> {
    fn eq(&self, other: &Block) -> bool {
        if let Some(bitmap) = self.bitmap() {
            bitmap == &other.bitmap
        } else {
            self.segments.iter().copied().eq(other.bitmap.segments())
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
