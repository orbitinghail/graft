use bytes::BytesMut;
use rand::{distr::StandardUniform, prelude::Distribution, Rng};

use crate::{
    page::{Page, PAGESIZE},
    page_offset::PageOffset,
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

impl PageOffset {
    #[inline]
    pub const fn new(n: u32) -> Self {
        assert!(n <= Self::MAX.to_u32(), "page offset out of bounds");
        Self::saturating_from_u32(n)
    }

    /// generates a random page offset in the range [0, max] (inclusive)
    pub fn test_random<R: Rng + ?Sized>(rng: &mut R, max: u32) -> Self {
        // ensure max is in PageOffset bounds
        let max = PageOffset::try_from_u32(max).unwrap().to_u32();
        PageOffset::saturating_from_u32(rng.random_range(0..max))
    }
}
