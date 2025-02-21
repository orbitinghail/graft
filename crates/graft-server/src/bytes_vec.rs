use std::io::IoSlice;

use bytes::{Buf, BufMut, Bytes, BytesMut};

#[derive(Default, Clone)]
pub struct BytesVec {
    bufs: Vec<Bytes>,
}

impl BytesVec {
    pub fn with_capacity(capacity: usize) -> Self {
        Self { bufs: Vec::with_capacity(capacity) }
    }

    pub fn put(&mut self, bytes: Bytes) {
        self.bufs.push(bytes);
    }

    pub fn put_slice(&mut self, slice: &[u8]) {
        self.bufs.push(Bytes::copy_from_slice(slice));
    }

    pub fn append(&mut self, mut bytes_vec: BytesVec) {
        self.bufs.append(&mut bytes_vec.bufs);
    }

    pub fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.bufs.iter()
    }

    pub fn into_bytes(self) -> Bytes {
        let mut buf = BytesMut::with_capacity(self.remaining());
        for b in self.bufs {
            buf.extend_from_slice(&b);
        }
        buf.freeze()
    }
}

impl Buf for BytesVec {
    fn remaining(&self) -> usize {
        self.bufs.iter().map(|b| b.remaining()).sum()
    }

    fn chunk(&self) -> &[u8] {
        self.bufs.first().map_or(&[], |b| b.chunk())
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let n = dst.len().min(self.bufs.len());
        for (i, buf) in self.bufs.iter().take(n).enumerate() {
            dst[i] = IoSlice::new(buf.chunk());
        }
        n
    }

    fn advance(&mut self, mut cnt: usize) {
        let mut to_remove = 0;
        for buf in &mut self.bufs {
            if cnt < buf.remaining() {
                buf.advance(cnt);
                break;
            } else {
                cnt -= buf.remaining();
                to_remove += 1
            }
        }
        self.bufs.drain(0..to_remove);
    }

    fn copy_to_bytes(&mut self, len: usize) -> bytes::Bytes {
        if self.remaining() < len {
            panic!(
                "advance out of bounds: the len is {} but advancing by {}",
                self.remaining(),
                len
            )
        }

        // fast path if we can pull all the requested bytes from the first buffer
        if let Some(first) = self.bufs.first_mut() {
            if first.len() >= len {
                return first.copy_to_bytes(len);
            }
        }

        let mut ret = BytesMut::with_capacity(len);
        ret.put(self.take(len));
        ret.freeze()
    }
}

impl IntoIterator for BytesVec {
    type Item = Bytes;
    type IntoIter = std::vec::IntoIter<Bytes>;

    fn into_iter(self) -> Self::IntoIter {
        self.bufs.into_iter()
    }
}

impl FromIterator<Bytes> for BytesVec {
    fn from_iter<T: IntoIterator<Item = Bytes>>(iter: T) -> Self {
        Self { bufs: iter.into_iter().collect() }
    }
}

impl From<Vec<Bytes>> for BytesVec {
    fn from(bufs: Vec<Bytes>) -> Self {
        Self { bufs }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[graft_test::test]
    fn test_bytes_vec() {
        let mut bytes_vec = BytesVec { bufs: vec![] };
        let bytes = Bytes::from_static(b"hello");
        bytes_vec.bufs.push(bytes.clone());
        bytes_vec.bufs.push(bytes.clone());
        bytes_vec.bufs.push(bytes.clone());

        assert_eq!(bytes_vec.remaining(), 15);
        assert_eq!(bytes_vec.chunk(), b"hello");
        bytes_vec.advance(3);
        assert_eq!(bytes_vec.remaining(), 12);
        assert_eq!(bytes_vec.chunk(), b"lo");
        bytes_vec.advance(12);
        assert_eq!(bytes_vec.remaining(), 0);
    }
}
