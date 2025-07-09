use bytes::Bytes;
use culprit::{Result, ResultExt};
use fjall::Slice;
use graft_core::{
    PageIdx, SegmentId, VolumeId,
    cbe::{CBE32, CBE64},
    handle_id::{HandleId, HandleIdErr},
    lsn::{InvalidLSN, LSN},
    page_idx::ConvertToPageIdxErr,
    zerocopy_ext::{TryFromBytesExt, ZerocopyErr},
};
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[derive(Debug, thiserror::Error)]
pub enum DecodeErr {
    #[error("Invalid LSN: {0}")]
    InvalidLSN(#[from] InvalidLSN),

    #[error("Zerocopy error: {0}")]
    Zerocopy(#[from] ZerocopyErr),

    #[error("Invalid page index: {0}")]
    InvalidPageIdx(#[from] ConvertToPageIdxErr),

    #[error("Invalid handle ID: {0}")]
    InvalidHandleId(#[from] HandleIdErr),

    #[error("Invalid VolumeID: {0}")]
    GidParseErr(#[from] graft_core::gid::GidParseErr),
}

pub trait FjallKey: Sized {
    /// Converts the key into a type that can be cheaply converted into a byte
    /// slice. For ZeroCopy types, this may simply be the raw bytes of the key.
    fn as_slice(&self) -> impl AsRef<[u8]>;

    /// Converts a Fjall Slice into a Key.
    fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr>;

    /// Converts the Key into a Fjall Slice
    #[inline]
    fn into_slice(self) -> Slice {
        Slice::new(self.as_slice().as_ref())
    }
}

pub trait FjallKeyPrefix {
    type Prefix: AsRef<[u8]>;
}

impl FjallKey for HandleId {
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

impl FjallKey for VolumeId {
    #[inline]
    fn as_slice(&self) -> impl AsRef<[u8]> {
        self.as_bytes()
    }

    #[inline]
    fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr> {
        VolumeId::try_from(Bytes::from(slice)).or_into_ctx()
    }
}

macro_rules! proxy_key_codec {
    (
        encode key ($key:ty) using proxy ($proxy:ty)
        into_proxy(&$self:ident) $into_proxy:block
        from_proxy($iproxy:ident) $from_proxy:block
    ) => {
        impl FjallKey for $key {
            #[inline]
            fn as_slice(&$self) -> impl AsRef<[u8]> {
                $into_proxy
            }

            #[inline]
            fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr> {
                let $iproxy: &$proxy =
                    <$proxy>::try_ref_from_unaligned_bytes(&slice).or_into_ctx()?;
                $from_proxy
            }
        }
    };
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

proxy_key_codec!(
    encode key (CommitKey) using proxy (SerializedCommitKey)
    into_proxy(&self) {
        SerializedCommitKey {
            vid: self.vid.clone(),
            lsn: self.lsn.into(),
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
    pageidx: CBE32,
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

proxy_key_codec!(
    encode key (PageKey) using proxy (SerializedPageKey)
    into_proxy(&self) {
        SerializedPageKey {
            sid: self.sid.clone(),
            pageidx: self.pageidx.into(),
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
    use std::fmt::Debug;

    use super::*;

    /// Tests that a FjallKey value can be encoded to a slice and then decoded back to the original value.
    #[track_caller]
    fn test_roundtrip<T>(value: T)
    where
        T: FjallKey + PartialEq + Clone + Debug,
    {
        let slice = value.clone().into_slice();
        let decoded = T::try_from_slice(slice).expect("Failed to decode");
        assert_eq!(value, decoded, "Roundtrip failed");
    }

    /// Tests that a FjallKey value correctly fails to decode invalid data.
    #[track_caller]
    fn test_invalid<T: FjallKey>(slice: &[u8]) {
        assert!(
            T::try_from_slice(Slice::from(slice)).is_err(),
            "Expected error for invalid slice"
        );
    }

    /// Tests that a FjallKey type serializes to the expected ordering.
    #[track_caller]
    fn test_serialized_order<T>(values: &[T])
    where
        T: FjallKey + PartialEq + Clone + Debug,
    {
        // serialize every element of T into a list of Slice
        let mut slices: Vec<Slice> = values.iter().cloned().map(|v| v.into_slice()).collect();

        // then sort the list of slices by their natural bytewise order
        slices.sort();

        // then verify that the resulting list of slices is in the same order as
        // the values array
        for (i, slice) in slices.into_iter().enumerate() {
            let decoded = T::try_from_slice(slice).expect("Failed to decode");
            assert_eq!(decoded, values[i], "Order mismatch at index {}", i);
        }
    }

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
                pageidx: CBE32::new(0),
            }
            .as_bytes(),
        );

        test_invalid::<PageKey>(b"short");
        test_invalid::<PageKey>(b"");

        // PageKeys must naturally sort in descending order by page index
        let sid1: SegmentId = "LkykngWAEj8KaTkYeg5ZBY".parse().unwrap();
        let sid2: SegmentId = "LkykngWBbT1v8zGaRpdbpK".parse().unwrap();
        test_serialized_order(&[
            PageKey::new(sid1.clone(), PageIdx::new(4)),
            PageKey::new(sid1.clone(), PageIdx::new(3)),
            PageKey::new(sid1.clone(), PageIdx::new(2)),
            PageKey::new(sid1.clone(), PageIdx::new(1)),
            PageKey::new(sid2.clone(), PageIdx::new(2)),
            PageKey::new(sid2.clone(), PageIdx::new(1)),
        ]);
    }
}
