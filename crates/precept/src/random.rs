use rand::RngCore;

#[cfg(feature = "antithesis")]
pub fn rng() -> impl RngCore {
    antithesis_sdk::random::AntithesisRng
}

#[cfg(not(feature = "antithesis"))]
pub fn rng() -> impl RngCore {
    rand::thread_rng()
}
