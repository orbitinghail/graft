use crate::core::{commit::Commit, page::Page};
use bilrost::{Message, OwnedMessage};
use bytes::Bytes;

use crate::{
    local::fjall_storage::fjall_repr::{FjallRepr, FjallReprRef},
    volume::Volume,
};

use super::fjall_repr::DecodeErr;

impl FjallReprRef for Page {
    #[inline]
    fn as_slice(&self) -> impl AsRef<[u8]> {
        self
    }

    fn into_slice(self) -> fjall::Slice {
        self.into_bytes().into()
    }
}

impl FjallRepr for Page {
    fn try_from_slice(slice: fjall::Slice) -> Result<Self, DecodeErr> {
        Ok(Page::try_from(Bytes::from(slice))?)
    }
}

macro_rules! impl_fjallrepr_for_bilrost {
    ($($ty:ty),+) => {
        $(
            impl FjallReprRef for $ty {
                #[inline]
                fn as_slice(&self) -> impl AsRef<[u8]> {
                    self.encode_to_bytes()
                }

                #[inline]
                fn into_slice(self) -> fjall::Slice {
                    self.encode_to_bytes().into()
                }
            }

            impl FjallRepr for $ty {
                #[inline]
                fn try_from_slice(slice: fjall::Slice) -> Result<Self, DecodeErr> {
                    Ok(<$ty>::decode(Bytes::from(slice))?)
                }
            }
        )+
    };
}

impl_fjallrepr_for_bilrost!(Volume, Commit);

impl FjallReprRef for () {
    #[inline]
    fn as_slice(&self) -> impl AsRef<[u8]> {
        []
    }

    #[inline]
    fn into_slice(self) -> fjall::Slice
    where
        Self: Sized,
    {
        Bytes::new().into()
    }
}

impl FjallRepr for () {
    fn try_from_slice(slice: fjall::Slice) -> Result<Self, DecodeErr> {
        if slice.is_empty() {
            Ok(())
        } else {
            Err(DecodeErr::NonemptyValue(slice.len()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    use crate::core::VolumeId;
    use crate::core::{LogId, PageCount, page::PAGESIZE};
    use crate::lsn;

    use crate::local::fjall_storage::fjall_repr::testutil::{
        test_empty_default, test_invalid, test_roundtrip,
    };

    #[test]
    fn test_page() {
        test_roundtrip(Page::test_filled(123));
        test_roundtrip(Page::EMPTY);
        test_invalid::<Page>(&b"a".repeat(PAGESIZE.as_usize() + 1));
    }

    #[test]
    fn test_volume() {
        test_roundtrip(Volume::new(
            VolumeId::random(),
            LogId::random(),
            LogId::random(),
            None,
            None,
        ));
        test_empty_default::<Volume>();
        test_invalid::<Volume>(&b"abc".repeat(123));
    }

    #[test]
    fn test_commit() {
        test_roundtrip(Commit::new(LogId::random(), lsn!(123), PageCount::new(456)));
        test_empty_default::<Commit>();
        test_invalid::<Commit>(&b"abc".repeat(123));
    }
}
