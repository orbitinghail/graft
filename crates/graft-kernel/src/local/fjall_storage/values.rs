use bilrost::{Message, OwnedMessage};
use bytes::Bytes;
use culprit::ResultExt;
use graft_core::{checkpoints::CachedCheckpoints, commit::Commit, page::Page};

use crate::{
    graft::Graft,
    local::fjall_storage::fjall_repr::{FjallRepr, FjallReprRef},
};

use super::fjall_repr::DecodeErr;

impl FjallReprRef for Page {
    #[inline]
    fn as_slice(&self) -> impl AsRef<[u8]> {
        self
    }

    #[inline]
    fn into_slice(self) -> fjall::Slice {
        self.into_bytes().into()
    }
}

impl FjallRepr for Page {
    #[inline]
    fn try_from_slice(slice: fjall::Slice) -> culprit::Result<Self, DecodeErr> {
        Page::try_from(Bytes::from(slice)).or_into_ctx()
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
                fn try_from_slice(slice: fjall::Slice) -> culprit::Result<Self, DecodeErr> {
                    <$ty>::decode(Bytes::from(slice)).or_into_ctx()
                }
            }
        )+
    };
}

impl_fjallrepr_for_bilrost!(Graft, CachedCheckpoints, Commit);

#[cfg(test)]
mod tests {
    use super::*;

    use graft_core::checkpoints::Checkpoints;
    use graft_core::lsn;
    use graft_core::{LogId, PageCount, page::PAGESIZE};

    use crate::local::fjall_storage::fjall_repr::testutil::{
        test_empty_default, test_invalid, test_roundtrip,
    };

    #[graft_test::test]
    fn test_page() {
        test_roundtrip(Page::test_filled(123));
        test_roundtrip(Page::EMPTY);
        test_invalid::<Page>(&b"a".repeat(PAGESIZE.as_usize() + 1));
    }

    #[graft_test::test]
    fn test_volume() {
        test_roundtrip(Graft::new(LogId::random(), LogId::random(), None, None));
        test_empty_default::<Graft>();
        test_invalid::<Graft>(&b"abc".repeat(123));
    }

    #[graft_test::test]
    fn test_checkpoints() {
        test_roundtrip(CachedCheckpoints::new(
            Checkpoints::from([lsn!(123)].as_slice()),
            Some("asdf"),
        ));
        test_empty_default::<CachedCheckpoints>();
        test_invalid::<CachedCheckpoints>(&b"abc".repeat(123));
    }

    #[graft_test::test]
    fn test_commit() {
        test_roundtrip(Commit::new(LogId::random(), lsn!(123), PageCount::new(456)));
        test_empty_default::<Commit>();
        test_invalid::<Commit>(&b"abc".repeat(123));
    }
}
