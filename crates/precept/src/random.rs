use rand::RngCore;

#[cfg(feature = "disabled")]
pub fn rng() -> impl RngCore {
    rand::thread_rng()
}

#[cfg(not(feature = "disabled"))]
pub fn rng() -> impl RngCore {
    DispatchRng
}

#[cfg(not(feature = "disabled"))]
struct DispatchRng;

#[cfg(not(feature = "disabled"))]
impl RngCore for DispatchRng {
    fn next_u32(&mut self) -> u32 {
        crate::dispatch::get_random() as u32
    }

    fn next_u64(&mut self) -> u64 {
        crate::dispatch::get_random()
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
}
