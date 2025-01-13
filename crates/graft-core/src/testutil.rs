use bytes::BytesMut;
use rand::{
    distributions::{Standard, Uniform},
    prelude::Distribution,
    Rng,
};

use crate::{
    page::{Page, PAGESIZE},
    page_offset::PageOffset,
};

impl Page {
    pub fn test_filled(value: u8) -> Self {
        Page::from(&[value; PAGESIZE.as_usize()])
    }
}

impl Distribution<Page> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Page {
        let mut data = BytesMut::zeroed(PAGESIZE.as_usize());
        rng.fill(data.as_mut());
        data.freeze().try_into().unwrap()
    }
}

impl PageOffset {
    pub fn test_random<R: Rng + ?Sized>(rng: &mut R, max: u32) -> Self {
        assert!(
            max <= u32::from(PageOffset::MAX),
            "page offset out of bounds"
        );
        Self::new(Uniform::new(0, max).sample(rng))
    }
}
