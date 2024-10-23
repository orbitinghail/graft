use std::ops::Deref;

use crate::{bitmap::BITMAP_SIZE, ops::Cut, util::CopyToOwned, Segment};

use super::{Block, BlockRef};

impl Cut for Block {
    type Output = Block;

    fn cut(&mut self, rhs: &Self) -> Self::Output {
        let mut intersection = [0u8; BITMAP_SIZE];
        (0..BITMAP_SIZE).for_each(|i| {
            intersection[i] = self.bitmap[i] & rhs.bitmap[i];
            self.bitmap[i] &= !rhs.bitmap[i];
        });
        intersection.into()
    }
}

impl<T: Deref<Target = [Segment]>> Cut<BlockRef<T>> for Block {
    type Output = Block;

    fn cut(&mut self, rhs: &BlockRef<T>) -> Self::Output {
        let rhs = rhs.copy_to_owned();
        self.cut(&rhs)
    }
}
