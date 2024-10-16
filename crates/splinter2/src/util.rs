use bytes::{BufMut, Bytes, BytesMut};

pub trait FromSuffix<'a> {
    fn from_suffix(data: &'a [u8], cardinality: usize) -> Self;
}

/// A custom version of ToOwned to get around a conflict with the standard
/// library's `impl<T> ToOwned for T where T: Clone` and BlockRef.
pub trait CopyToOwned {
    type Owned;

    fn copy_to_owned(&self) -> Self::Owned;
}

pub trait Serialize {
    /// Serialize the object into the given buffer; returning the cardinality of
    /// the object and number of bytes written
    fn serialize<B: BufMut>(&self, out: &mut B) -> (usize, usize);

    fn serialize_to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();
        self.serialize(&mut buf);
        buf.freeze()
    }
}
