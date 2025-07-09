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
        Bytes::from(self).into()
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
