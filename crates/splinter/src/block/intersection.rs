use std::ops::Deref;

use crate::{bitmap::BITMAP_SIZE, ops::Intersection, util::CopyToOwned, Segment};

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
impl<T: Deref<Target = [Segment]>> Intersection<BlockRef<T>> for Block {
    type Output = Block;

    #[inline]
    fn intersection(&self, rhs: &BlockRef<T>) -> Self::Output {
        let rhs = rhs.copy_to_owned();
        self.intersection(&rhs)
    }
}

// BlockRef <> Block
impl<T: Deref<Target = [Segment]>> Intersection<Block> for BlockRef<T> {
    type Output = Block;

    #[inline]
    fn intersection(&self, rhs: &Block) -> Self::Output {
        rhs.intersection(self)
    }
}

// BlockRef <> BlockRef
impl<T1, T2> Intersection<BlockRef<T2>> for BlockRef<T1>
where
    T1: Deref<Target = [Segment]>,
    T2: Deref<Target = [Segment]>,
{
    type Output = Block;

    #[inline]
    fn intersection(&self, rhs: &BlockRef<T2>) -> Self::Output {
        let rhs = rhs.copy_to_owned();
        rhs.intersection(self)
    }
}
