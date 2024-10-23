use std::ops::Deref;

use crate::{bitmap::BITMAP_SIZE, ops::Union, util::CopyToOwned, Segment};

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
impl<T: Deref<Target = [Segment]>> Union<BlockRef<T>> for Block {
    type Output = Block;

    #[inline]
    fn union(&self, rhs: &BlockRef<T>) -> Self::Output {
        let rhs = rhs.copy_to_owned();
        self.union(&rhs)
    }
}

// BlockRef <> Block
impl<T: Deref<Target = [Segment]>> Union<Block> for BlockRef<T> {
    type Output = Block;

    #[inline]
    fn union(&self, rhs: &Block) -> Self::Output {
        rhs.union(self)
    }
}

// BlockRef <> BlockRef
impl<T1, T2> Union<BlockRef<T2>> for BlockRef<T1>
where
    T1: Deref<Target = [Segment]>,
    T2: Deref<Target = [Segment]>,
{
    type Output = Block;

    #[inline]
    fn union(&self, rhs: &BlockRef<T2>) -> Self::Output {
        let rhs = rhs.copy_to_owned();
        rhs.union(self)
    }
}
