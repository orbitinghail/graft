use crate::{bitmap::BITMAP_SIZE, ops::Union, util::CopyToOwned};

use super::{Block, BlockRef};

// Block <> Block
impl Union for Block {
    type Output = Block;

    #[inline]
    fn union(&self, rhs: &Self) -> Self::Output {
        let mut out = Block::default();
        for i in 0..BITMAP_SIZE {
            out.bitmap[i] = self.bitmap[i] | rhs.bitmap[i];
        }
        out
    }
}

// Block <> BlockRef
impl<'a> Union<BlockRef<'a>> for Block {
    type Output = Block;

    #[inline]
    fn union(&self, rhs: &BlockRef<'a>) -> Self::Output {
        let rhs = rhs.copy_to_owned();
        self.union(&rhs)
    }
}

// BlockRef <> Block
impl<'a> Union<Block> for BlockRef<'a> {
    type Output = Block;

    #[inline]
    fn union(&self, rhs: &Block) -> Self::Output {
        rhs.union(self)
    }
}

// BlockRef <> BlockRef
impl<'a, 'b> Union<BlockRef<'b>> for BlockRef<'a> {
    type Output = Block;

    #[inline]
    fn union(&self, rhs: &BlockRef<'b>) -> Self::Output {
        self.copy_to_owned().union(rhs)
    }
}
