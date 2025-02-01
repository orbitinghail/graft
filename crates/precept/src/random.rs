use rand::RngCore;

use crate::dispatch::get_random;

#[cfg(feature = "antithesis")]
pub fn rng() -> impl RngCore {
    DispatchRng
}

#[cfg(not(feature = "antithesis"))]
pub fn rng() -> impl RngCore {
    rand::thread_rng()
}

/// A random number generator that generates random numbers using
/// dispatch::get_random.
///
/// This implements the `RngCore` trait from the `rand` crate, allowing it to be used
/// with any code that expects a random number generator from that ecosystem.
///
/// # Example
///
/// ```
/// use precept::random::DispatchRng;
/// use rand::{Rng, RngCore};
///
/// let mut rng = DispatchRng;
/// let random_u32: u32 = rng.gen();
/// let random_u64: u64 = rng.gen();
/// let random_char: char = rng.gen();
///
/// let mut bytes = [0u8; 16];
/// rng.fill_bytes(&mut bytes);
/// ```
pub struct DispatchRng;

impl RngCore for DispatchRng {
    fn next_u32(&mut self) -> u32 {
        get_random() as u32
    }

    fn next_u64(&mut self) -> u64 {
        get_random()
    }

    fn fill_bytes(&mut self, dest: &mut [u8]) {
        // Split the destination buffer into chunks of 8 bytes each
        // (since we'll fill each chunk with a u64/8 bytes of random data)
        let mut chunks = dest.chunks_exact_mut(8);

        // Fill each complete 8-byte chunk with random bytes
        for chunk in chunks.by_ref() {
            // Generate 8 random bytes from a u64 in native endian order
            let random_bytes = self.next_u64().to_ne_bytes();
            // Copy those random bytes into this chunk
            chunk.copy_from_slice(&random_bytes);
        }

        // Get any remaining bytes that didn't fit in a complete 8-byte chunk
        let remainder = chunks.into_remainder();

        if !remainder.is_empty() {
            // Generate 8 more random bytes
            let random_bytes = self.next_u64().to_ne_bytes();
            // Copy just enough random bytes to fill the remainder
            remainder.copy_from_slice(&random_bytes[..remainder.len()]);
        }
    }

    fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand::Error> {
        self.fill_bytes(dest);
        Ok(())
    }
}
