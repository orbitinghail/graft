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
    /// generates a random page offset in the range [0, max] (inclusive)
    pub fn test_random<R: Rng + ?Sized, O: Into<PageOffset>>(rng: &mut R, max: O) -> Self {
        rng.random_range(0..max.into().as_u32()).into()
    }
}
