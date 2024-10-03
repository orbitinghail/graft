use bytes::BufMut;
use zerocopy::AsBytes;

use crate::Header;

pub struct SplinterWriter<B> {
    buf: B,
    last_key: u32,
}

impl<B: BufMut> SplinterWriter<B> {
    pub fn new(mut buf: B) -> Self {
        // write the header immediately
        buf.put(Header::DEFAULT.as_bytes());
        Self { buf, last_key: 0 }
    }

    pub fn append(&mut self, key: u32) {
        assert!(key < 1 << 24, "key out of range: {}", key);
        assert!(key > self.last_key, "keys must be appended in order");
    }
}
