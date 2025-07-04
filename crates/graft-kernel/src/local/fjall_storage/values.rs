use bilrost::{Message, OwnedMessage};
use bytes::Bytes;
use culprit::{Result, ResultExt};
use fjall::Slice;
use graft_core::{
    codec::v1::{local, remote},
    page::Page,
};

use crate::local::fjall_storage::fjall_repr::{DecodeErr, FjallRepr};

macro_rules! bilrost_fjall_repr {
    ($($ty:ty),+) => {
        $(
            static_assertions::assert_impl_all!($ty: bilrost::Message, bilrost::OwnedMessage);

            impl FjallRepr for $ty {
                #[inline]
                fn as_slice(&self) -> Slice {
                    self.encode_to_bytes().into()
                }

                #[inline]
                fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr>
                {
                    Ok(<$ty>::decode(Bytes::from(slice))?)
                }
            }
        )+
    };
}

bilrost_fjall_repr!(remote::Commit, local::VolumeHandle, local::LocalControl);

impl FjallRepr for Page {
    #[inline]
    fn as_slice(&self) -> Slice {
        Bytes::from(self.clone()).into()
    }

    #[inline]
    fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr> {
        Ok(Self::try_from(Bytes::from(slice)).or_into_ctx()?)
    }
}

#[cfg(test)]
mod tests {
    use graft_core::page::PAGESIZE;

    use crate::local::fjall_storage::fjall_repr::testutil::{test_invalid, test_roundtrip};

    use super::*;

    #[graft_test::test]
    fn test_page() {
        test_roundtrip(Page::test_filled(123));
        test_roundtrip::<Page>(rand::random());
        test_invalid::<Page>(b"");
        test_invalid::<Page>(&b"a".repeat(PAGESIZE.as_usize() + 1));
    }

    #[graft_test::test]
    fn test_commit() {
        // test remote::Commit
    }

    #[graft_test::test]
    fn test_volume_handle() {
        // test local::VolumeHandle
    }

    #[graft_test::test]
    fn test_local_control() {
        // test local::LocalControl
    }
}
