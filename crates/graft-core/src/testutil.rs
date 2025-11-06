use bytes::BytesMut;
use rand::{
    Rng,
    distr::{
        StandardUniform,
        uniform::{self, SampleBorrow, SampleUniform, UniformInt, UniformSampler},
    },
    prelude::Distribution,
};

use crate::{
    page::{PAGESIZE, Page},
    pageidx::PageIdx,
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

pub struct PageIdxSampler(UniformInt<u32>);

impl UniformSampler for PageIdxSampler {
    type X = PageIdx;

    fn new<B1, B2>(low: B1, high: B2) -> Result<Self, uniform::Error>
    where
        B1: SampleBorrow<Self::X> + Sized,
        B2: SampleBorrow<Self::X> + Sized,
    {
        let low = low.borrow().to_u32();
        let high = high.borrow().to_u32();
        Ok(Self(UniformInt::new(low, high)?))
    }

    fn new_inclusive<B1, B2>(low: B1, high: B2) -> Result<Self, uniform::Error>
    where
        B1: SampleBorrow<Self::X> + Sized,
        B2: SampleBorrow<Self::X> + Sized,
    {
        let low = low.borrow().to_u32();
        let high = high.borrow().to_u32();
        Ok(Self(UniformInt::new_inclusive(low, high)?))
    }

    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Self::X {
        PageIdx::must_new(self.0.sample(rng))
    }
}

impl SampleUniform for PageIdx {
    type Sampler = PageIdxSampler;
}

impl PageIdx {
    /// `must_new` is only defined for tests, regular code should use `try_new` and handle errors
    #[inline]
    pub const fn must_new(n: u32) -> Self {
        Self::try_new(n).expect("page index must be non-zero")
    }
}
