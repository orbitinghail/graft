use bytes::Bytes;
use culprit::{Result, ResultExt};
use fjall::Slice;
use graft_core::{
    PageIdx, SegmentId, VolumeId, cbe::CBE64, lsn::LSN, volume_ref::VolumeRef,
    zerocopy_ext::TryFromBytesExt,
};
use zerocopy::{BigEndian, Immutable, IntoBytes, KnownLayout, TryFromBytes, U32, Unaligned};

use crate::{
    local::fjall_storage::fjall_repr::{DecodeErr, FjallRepr},
    proxy_to_fjall_repr,
    volume_name::VolumeName,
};

pub trait FjallKeyPrefix {
    type Prefix: AsRef<[u8]>;
}

impl FjallRepr for VolumeName {
    #[inline]
    fn as_slice(&self) -> impl AsRef<[u8]> {
        self.as_bytes()
    }

    #[inline]
    fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr> {
        VolumeName::try_from(Bytes::from(slice)).or_into_ctx()
    }

    #[inline]
    fn into_slice(self) -> Slice {
        Bytes::from(self).into()
    }
}

impl FjallRepr for VolumeId {
    #[inline]
    fn as_slice(&self) -> impl AsRef<[u8]> {
        self.as_bytes()
    }

    #[inline]
    fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr> {
        VolumeId::try_from(Bytes::from(slice)).or_into_ctx()
    }
}

#[derive(IntoBytes, TryFromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SerializedVolumeRef {
    vid: VolumeId,
    lsn: CBE64,
}

impl AsRef<[u8]> for SerializedVolumeRef {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl FjallKeyPrefix for VolumeRef {
    type Prefix = VolumeId;
}

proxy_to_fjall_repr!(
    encode (VolumeRef) using proxy (SerializedVolumeRef)
    into_proxy(me) {
        SerializedVolumeRef {
            vid: me.vid,
            lsn: me.lsn.into(),
        }
    }
    from_proxy(proxy) {
        Ok(VolumeRef {
            vid: proxy.vid.clone(),
            lsn: LSN::try_from(proxy.lsn)?,
        })
    }
);

/// Key for the `pages` partition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PageKey {
    sid: SegmentId,
    pageidx: PageIdx,
}

impl PageKey {
    #[inline]
    pub fn new(sid: SegmentId, pageidx: PageIdx) -> Self {
        Self { sid, pageidx }
    }

    #[inline]
    pub fn sid(&self) -> &SegmentId {
        &self.sid
    }

    #[inline]
    pub fn pageidx(&self) -> &PageIdx {
        &self.pageidx
    }
}

#[derive(IntoBytes, TryFromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SerializedPageKey {
    sid: SegmentId,
    pageidx: U32<BigEndian>,
}

impl AsRef<[u8]> for SerializedPageKey {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl FjallKeyPrefix for PageKey {
    type Prefix = SegmentId;
}

proxy_to_fjall_repr!(
    encode (PageKey) using proxy (SerializedPageKey)
    into_proxy(me) {
        SerializedPageKey {
            sid: me.sid,
            pageidx: me.pageidx.into(),
        }
    }
    from_proxy(proxy) {
        Ok(PageKey {
            sid: proxy.sid.clone(),
            pageidx: PageIdx::try_from(proxy.pageidx)?,
        })
    }
);

#[cfg(test)]
mod tests {
    use graft_core::{lsn, pageidx};

    use crate::local::fjall_storage::fjall_repr::testutil::{
        test_invalid, test_roundtrip, test_serialized_order,
    };

    use super::*;

    #[graft_test::test]
    fn test_volume_name() {
        test_roundtrip(VolumeName::new("test-volume").unwrap());
        test_invalid::<VolumeName>(b"bad id");
        test_invalid::<VolumeName>(b"");
        test_invalid::<VolumeName>(&b"a".repeat(crate::volume_name::MAX_VOLUME_NAME_LEN + 1));
    }

    #[graft_test::test]
    fn test_volume_id() {
        test_roundtrip(VolumeId::random());
        test_roundtrip(VolumeId::ZERO);
        test_invalid::<VolumeId>(b"");
        test_invalid::<VolumeId>(b"asdf");
        test_invalid::<VolumeId>(SegmentId::random().as_bytes());
    }

    #[graft_test::test]
    fn test_commit_key() {
        test_roundtrip(VolumeRef::new(VolumeId::random(), lsn!(123)));

        // zero LSN is invalid
        test_invalid::<VolumeRef>(
            SerializedVolumeRef {
                vid: VolumeId::random(),
                lsn: CBE64::new(0),
            }
            .as_bytes(),
        );

        test_invalid::<VolumeRef>(b"short");
        test_invalid::<VolumeRef>(b"");

        // CommitKeys must naturally sort in descending order by LSN
        let vid1: VolumeId = "5rMJhdYXJ3-2e64STQSCVT8X".parse().unwrap();
        let vid2: VolumeId = "5rMJhdYYXB-2e2iX9AHva3xQ".parse().unwrap();
        test_serialized_order(&[
            VolumeRef::new(vid1.clone(), lsn!(4)),
            VolumeRef::new(vid1.clone(), lsn!(3)),
            VolumeRef::new(vid1.clone(), lsn!(2)),
            VolumeRef::new(vid1, lsn!(1)),
            VolumeRef::new(vid2.clone(), lsn!(2)),
            VolumeRef::new(vid2, lsn!(1)),
        ]);
    }

    #[graft_test::test]
    fn test_page_key() {
        test_roundtrip(PageKey::new(SegmentId::random(), pageidx!(42)));

        // zero page index is invalid
        test_invalid::<PageKey>(
            SerializedPageKey {
                sid: SegmentId::random(),
                pageidx: 0.into(),
            }
            .as_bytes(),
        );

        test_invalid::<PageKey>(b"short");
        test_invalid::<PageKey>(b"");

        // PageKeys must naturally sort in ascending order by page index
        let sid1: SegmentId = "74ggYyz4aX-33cEC1Bm7Gekh".parse().unwrap();
        let sid2: SegmentId = "74ggYyz7mA-33d6VHh4ENsxq".parse().unwrap();
        test_serialized_order(&[
            PageKey::new(sid1.clone(), pageidx!(1)),
            PageKey::new(sid1.clone(), pageidx!(2)),
            PageKey::new(sid1.clone(), pageidx!(3)),
            PageKey::new(sid1, pageidx!(4)),
            PageKey::new(sid2.clone(), pageidx!(1)),
            PageKey::new(sid2, pageidx!(2)),
        ]);
    }
}
