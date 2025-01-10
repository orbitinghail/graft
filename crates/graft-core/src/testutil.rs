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
        Page::test_filled(rng.gen())
    }
}

impl Distribution<PageOffset> for Standard {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> PageOffset {
        Uniform::new(0, u32::from(PageOffset::MAX))
            .sample(rng)
            .into()
    }
}
