use bytes::Bytes;
use culprit::{Result, ResultExt};
use fjall::Slice;
use graft_core::{
    PageIdx, SegmentId, VolumeId, cbe::CBE64, handle_id::HandleId, lsn::LSN, volume_ref::VolumeRef,
    zerocopy_ext::TryFromBytesExt,
};
use zerocopy::{BigEndian, Immutable, IntoBytes, KnownLayout, TryFromBytes, U32, Unaligned};

use crate::{
    local::fjall_storage::fjall_repr::{DecodeErr, FjallRepr},
    proxy_to_fjall_repr,
};

pub trait FjallKeyPrefix {
    type Prefix: AsRef<[u8]>;
}

impl FjallRepr for HandleId {
    #[inline]
    fn as_slice(&self) -> impl AsRef<[u8]> {
        self.as_bytes()
    }

    #[inline]
    fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr> {
        HandleId::try_from(Bytes::from(slice)).or_into_ctx()
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

/// Key for the `log` partition
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommitKey {
    vid: VolumeId,
    lsn: LSN,
}

impl CommitKey {
    #[inline]
    pub fn new(vid: VolumeId, lsn: LSN) -> Self {
        Self { vid, lsn }
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    pub fn lsn(&self) -> &LSN {
        &self.lsn
    }
}

impl From<VolumeRef> for CommitKey {
    #[inline]
    fn from(volume_ref: VolumeRef) -> Self {
        let (vid, lsn) = volume_ref.into();
        Self { vid, lsn }
    }
}

#[derive(IntoBytes, TryFromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SerializedCommitKey {
    vid: VolumeId,
    lsn: CBE64,
}

impl AsRef<[u8]> for SerializedCommitKey {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl FjallKeyPrefix for CommitKey {
    type Prefix = VolumeId;
}

proxy_to_fjall_repr!(
    encode (CommitKey) using proxy (SerializedCommitKey)
    into_proxy(me) {
        SerializedCommitKey {
            vid: me.vid,
            lsn: me.lsn.into(),
        }
    }
    from_proxy(proxy) {
        Ok(CommitKey {
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
    use crate::local::fjall_storage::fjall_repr::testutil::{
        test_invalid, test_roundtrip, test_serialized_order,
    };

    use super::*;

    #[graft_test::test]
    fn test_handle_id() {
        test_roundtrip(HandleId::new("test-handle").unwrap());
        test_invalid::<HandleId>(b"bad id");
        test_invalid::<HandleId>(b"");
        test_invalid::<HandleId>(&b"a".repeat(graft_core::handle_id::MAX_HANDLE_ID_LEN + 1));
    }

    #[graft_test::test]
    fn test_volume_id() {
        test_roundtrip(VolumeId::random());
        test_roundtrip(VolumeId::EMPTY);
        test_invalid::<VolumeId>(b"");
        test_invalid::<VolumeId>(b"asdf");
        test_invalid::<VolumeId>(SegmentId::random().as_bytes());
    }

    #[graft_test::test]
    fn test_commit_key() {
        test_roundtrip(CommitKey::new(VolumeId::random(), LSN::new(123)));

        // zero LSN is invalid
        test_invalid::<CommitKey>(
            SerializedCommitKey {
                vid: VolumeId::random(),
                lsn: CBE64::new(0),
            }
            .as_bytes(),
        );

        test_invalid::<CommitKey>(b"short");
        test_invalid::<CommitKey>(b"");

        // CommitKeys must naturally sort in descending order by LSN
        let vid1: VolumeId = "GonvRDHqjHwNsCpPBET3Ly".parse().unwrap();
        let vid2: VolumeId = "GonvRDHruDyBB6s6RmuiSZ".parse().unwrap();
        test_serialized_order(&[
            CommitKey::new(vid1.clone(), LSN::new(4)),
            CommitKey::new(vid1.clone(), LSN::new(3)),
            CommitKey::new(vid1.clone(), LSN::new(2)),
            CommitKey::new(vid1.clone(), LSN::new(1)),
            CommitKey::new(vid2.clone(), LSN::new(2)),
            CommitKey::new(vid2.clone(), LSN::new(1)),
        ]);
    }

    #[graft_test::test]
    fn test_page_key() {
        test_roundtrip(PageKey::new(SegmentId::random(), PageIdx::new(42)));

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
        let sid1: SegmentId = "LkykngWAEj8KaTkYeg5ZBY".parse().unwrap();
        let sid2: SegmentId = "LkykngWBbT1v8zGaRpdbpK".parse().unwrap();
        test_serialized_order(&[
            PageKey::new(sid1.clone(), PageIdx::new(1)),
            PageKey::new(sid1.clone(), PageIdx::new(2)),
            PageKey::new(sid1.clone(), PageIdx::new(3)),
            PageKey::new(sid1.clone(), PageIdx::new(4)),
            PageKey::new(sid2.clone(), PageIdx::new(1)),
            PageKey::new(sid2.clone(), PageIdx::new(2)),
        ]);
    }
}
