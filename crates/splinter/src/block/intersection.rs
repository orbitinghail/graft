use crate::{bitmap::BITMAP_SIZE, ops::Intersection, util::CopyToOwned};

use super::{Block, BlockRef};

// Block <> Block
impl Intersection for Block {
    type Output = Block;

    #[inline]
    fn intersection(&self, rhs: &Self) -> Self::Output {
        let mut out = Block::default();
        for i in 0..BITMAP_SIZE {
            out.bitmap[i] = self.bitmap[i] & rhs.bitmap[i];
        }
        out
    }
}

// Block <> BlockRef
impl<'a> Intersection<BlockRef<'a>> for Block {
    type Output = Block;

    #[inline]
    fn intersection(&self, rhs: &BlockRef<'a>) -> Self::Output {
        let rhs = rhs.copy_to_owned();
        self.intersection(&rhs)
    }
}

// BlockRef <> Block
impl<'a> Intersection<Block> for BlockRef<'a> {
    type Output = Block;

    #[inline]
    fn intersection(&self, rhs: &Block) -> Self::Output {
        rhs.intersection(self)
    }
}

// BlockRef <> BlockRef
impl<'a, 'b> Intersection<BlockRef<'b>> for BlockRef<'a> {
    type Output = Block;

    #[inline]
    fn intersection(&self, rhs: &BlockRef<'b>) -> Self::Output {
        self.copy_to_owned().intersection(rhs)
    }
}
