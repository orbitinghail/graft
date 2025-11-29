use bytes::Bytes;
use bytestring::ByteString;
use culprit::{Result, ResultExt};
use fjall::Slice;
use graft_core::{
    LogId, PageIdx, SegmentId, cbe::CBE64, logref::LogRef, lsn::LSN, zerocopy_ext::TryFromBytesExt,
};
use zerocopy::{BigEndian, Immutable, IntoBytes, KnownLayout, TryFromBytes, U32, Unaligned};

use crate::{
    local::fjall_storage::fjall_repr::{DecodeErr, FjallRepr, FjallReprRef},
    proxy_to_fjall_repr,
};

pub trait FjallKeyPrefix {
    type Prefix: AsRef<[u8]>;
}

impl FjallReprRef for LogId {
    #[inline]
    fn as_slice(&self) -> impl AsRef<[u8]> {
        self.as_bytes()
    }
}

impl FjallRepr for LogId {
    #[inline]
    fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr> {
        LogId::try_from(Bytes::from(slice)).or_into_ctx()
    }
}

impl FjallReprRef for ByteString {
    #[inline]
    fn as_slice(&self) -> impl AsRef<[u8]> {
        self
    }
}

impl FjallRepr for ByteString {
    #[inline]
    fn try_from_slice(slice: Slice) -> Result<Self, DecodeErr> {
        let bytes: Bytes = slice.into();
        ByteString::try_from(bytes).or_into_ctx()
    }
}

#[derive(IntoBytes, TryFromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SerializedLogRef {
    log: LogId,
    lsn: CBE64,
}

impl AsRef<[u8]> for SerializedLogRef {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl FjallKeyPrefix for LogRef {
    type Prefix = LogId;
}

proxy_to_fjall_repr!(
    encode (LogRef) using proxy (SerializedLogRef)
    into_proxy(me) {
        SerializedLogRef {
            log: me.log,
            lsn: me.lsn.into(),
        }
    }
    from_proxy(proxy) {
        Ok(LogRef {
            log: proxy.log.clone(),
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
    fn test_log_id() {
        test_roundtrip(LogId::random());
        test_roundtrip(LogId::EMPTY);
        test_invalid::<LogId>(b"");
        test_invalid::<LogId>(b"asdf");
        test_invalid::<LogId>(SegmentId::random().as_bytes());
    }

    #[graft_test::test]
    fn test_commit_key() {
        test_roundtrip(LogRef::new(LogId::random(), lsn!(123)));

        // zero LSN is invalid
        test_invalid::<LogRef>(
            SerializedLogRef { log: LogId::random(), lsn: CBE64::new(0) }.as_bytes(),
        );

        test_invalid::<LogRef>(b"short");
        test_invalid::<LogRef>(b"");

        // CommitKeys must naturally sort in descending order by LSN
        let log1: LogId = "74ggc11XPe-3tpZminfUtzHG".parse().unwrap();
        let log2: LogId = "74ggc11YqY-3eHQq23tMuPUq".parse().unwrap();
        test_serialized_order(&[
            LogRef::new(log1.clone(), lsn!(4)),
            LogRef::new(log1.clone(), lsn!(3)),
            LogRef::new(log1.clone(), lsn!(2)),
            LogRef::new(log1, lsn!(1)),
            LogRef::new(log2.clone(), lsn!(2)),
            LogRef::new(log2, lsn!(1)),
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
        let sid1: SegmentId = "8H24TMbwaL-3sWCcWZqGu8DG".parse().unwrap();
        let sid2: SegmentId = "8H24TMby3c-2rtTmSK9xAWo4".parse().unwrap();
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
