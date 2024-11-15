use crate::{bitmap::BITMAP_SIZE, ops::Cut, util::CopyToOwned};

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

impl<'a> Cut<BlockRef<'a>> for Block {
    type Output = Block;

    fn cut(&mut self, rhs: &BlockRef<'a>) -> Self::Output {
        let rhs = rhs.copy_to_owned();
        self.cut(&rhs)
    }
}
