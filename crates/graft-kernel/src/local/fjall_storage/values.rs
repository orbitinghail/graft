use bilrost::{Message, OwnedMessage};
use bytes::Bytes;
use culprit::ResultExt;
use graft_core::{
    commit::Commit, page::Page, volume_handle::VolumeHandle, volume_meta::VolumeMeta,
};

use crate::local::fjall_storage::fjall_repr::FjallRepr;

use super::fjall_repr::DecodeErr;

impl FjallRepr for Page {
    #[inline]
    fn as_slice(&self) -> impl AsRef<[u8]> {
        self
    }

    #[inline]
    fn into_slice(self) -> fjall::Slice {
        self.into_bytes().into()
    }

    #[inline]
    fn try_from_slice(slice: fjall::Slice) -> culprit::Result<Self, DecodeErr> {
        Page::try_from(Bytes::from(slice)).or_into_ctx()
    }
}

macro_rules! impl_fjallrepr_for_bilrost {
    ($($ty:ty),+) => {
        $(
            impl FjallRepr for $ty {
                #[inline]
                fn as_slice(&self) -> impl AsRef<[u8]> {
                    self.encode_to_bytes()
                }

                #[inline]
                fn try_from_slice(slice: fjall::Slice) -> culprit::Result<Self, DecodeErr> {
                    <$ty>::decode(Bytes::from(slice)).or_into_ctx()
                }

                #[inline]
                fn into_slice(self) -> fjall::Slice {
                    self.encode_to_bytes().into()
                }
            }
        )+
    };
}

impl_fjallrepr_for_bilrost!(VolumeHandle, VolumeMeta, Commit);

#[cfg(test)]
mod tests {
    use super::*;

    use graft_core::{
        PageCount, VolumeId, checkpoint_set::CheckpointSet, handle_id::HandleId, lsn::LSN,
        page::PAGESIZE, volume_ref::VolumeRef,
    };

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
    fn test_volume_handle() {
        test_roundtrip(VolumeHandle::new(
            HandleId::new("test-handle").unwrap(),
            VolumeRef::new(VolumeId::random(), LSN::new(123)),
            None,
            None,
        ));
        test_empty_default::<VolumeHandle>();
        test_invalid::<VolumeHandle>(&b"abc".repeat(123));
    }

    #[graft_test::test]
    fn test_volume_meta() {
        test_roundtrip(VolumeMeta::new(
            VolumeId::random(),
            Some(VolumeRef::new(VolumeId::random(), LSN::new(123))),
            Some(Bytes::from_static(b"asdf")),
            CheckpointSet::from([LSN::new(123)].as_ref()),
        ));
        test_empty_default::<VolumeMeta>();
        test_invalid::<VolumeMeta>(&b"abc".repeat(123));
    }

    #[graft_test::test]
    fn test_commit() {
        test_roundtrip(Commit::new(
            VolumeId::random(),
            LSN::new(123),
            PageCount::new(456),
        ));
        test_empty_default::<Commit>();
        test_invalid::<Commit>(&b"abc".repeat(123));
    }
}
