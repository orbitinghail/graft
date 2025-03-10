use rand::RngCore;

#[cfg(feature = "disabled")]
pub fn rng() -> impl RngCore {
    rand::rng()
}

#[cfg(not(feature = "disabled"))]
pub fn rng() -> impl RngCore {
    DispatchRng
}

#[cfg(not(feature = "disabled"))]
struct DispatchRng;

#[cfg(not(feature = "disabled"))]
impl RngCore for DispatchRng {
    #[inline]
    fn next_u32(&mut self) -> u32 {
        self.next_u64() as u32
    }

    #[inline]
    fn next_u64(&mut self) -> u64 {
        crate::dispatch::get_random()
    }

    #[inline]
    fn fill_bytes(&mut self, dst: &mut [u8]) {
        rand_core::impls::fill_bytes_via_next(self, dst)
    }
}
