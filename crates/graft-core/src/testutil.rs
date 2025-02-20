use bytes::BytesMut;
use rand::{distr::StandardUniform, prelude::Distribution, Rng};

use crate::{
    page::{Page, PAGESIZE},
    page_index::PageIdx,
};

impl Page {
    pub fn test_filled(value: u8) -> Self {
        Page::from(&[value; PAGESIZE.as_usize()])
    }
}

impl Distribution<Page> for StandardUniform {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Page {
        let mut data = BytesMut::zeroed(PAGESIZE.as_usize());
        rng.fill(data.as_mut());
        data.freeze().try_into().unwrap()
    }
}

impl PageIdx {
    #[inline]
    pub const fn new(n: u32) -> Self {
        Self::try_new(n).expect("page index must be non-zero")
    }

    /// generates a random page index in the range [1, max] (inclusive)
    pub fn test_random<R: Rng + ?Sized>(rng: &mut R, max: u32) -> Self {
        // ensure max is in PageOffset bounds
        let max = PageIdx::try_new(max).unwrap().to_u32();
        PageIdx::try_new(rng.random_range(1..max)).unwrap()
    }
}
