use crate::core::{
    LogId, PageIdx, SegmentId, cbe::CBE64, logref::LogRef, lsn::LSN, zerocopy_ext::TryFromBytesExt,
};
use fjall::Slice;
use zerocopy::{BigEndian, Immutable, IntoBytes, KnownLayout, TryFromBytes, U32, Unaligned};

use crate::{
    local::fjall_storage::fjall_repr::{FjallRepr, FjallReprRef},
    proxy_to_fjall_repr,
};

pub trait FjallKeyPrefix {
    type Prefix: FjallReprRef;
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

/// Key for the `pages` keyspace
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

/// A reference to a page in a log
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PageRef {
    log: LogId,
    pageidx: PageIdx,
}

impl PageRef {
    #[inline]
    pub fn new(log: LogId, pageidx: PageIdx) -> Self {
        Self { log, pageidx }
    }
}

#[derive(IntoBytes, TryFromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SerializedPageRef {
    log: LogId,
    pageidx: U32<BigEndian>,
}

impl AsRef<[u8]> for SerializedPageRef {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

proxy_to_fjall_repr!(
    encode (PageRef) using proxy (SerializedPageRef)
    into_proxy(me) {
        SerializedPageRef {
            log: me.log,
            pageidx: me.pageidx.into(),
        }
    }
    from_proxy(proxy) {
        Ok(PageRef {
            log: proxy.log.clone(),
            pageidx: PageIdx::try_from(proxy.pageidx)?,
        })
    }
);

/// A reference to a specific version of a page in a log
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PageVersion {
    log: LogId,
    pageidx: PageIdx,
    lsn: LSN,
}

impl PageVersion {
    #[inline]
    pub fn new(log: LogId, pageidx: PageIdx, lsn: LSN) -> Self {
        Self { log, pageidx, lsn }
    }
}

impl FjallKeyPrefix for PageVersion {
    type Prefix = PageRef;
}

#[derive(IntoBytes, TryFromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SerializedPageVersion {
    log: LogId,
    pageidx: U32<BigEndian>,
    lsn: CBE64,
}

impl AsRef<[u8]> for SerializedPageVersion {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

proxy_to_fjall_repr!(
    encode (PageVersion) using proxy (SerializedPageVersion)
    into_proxy(me) {
        SerializedPageVersion {
            log: me.log,
            pageidx: me.pageidx.into(),
            lsn: me.lsn.into(),
        }
    }
    from_proxy(proxy) {
        Ok(PageVersion {
            log: proxy.log.clone(),
            pageidx: PageIdx::try_from(proxy.pageidx)?,
            lsn: LSN::try_from(proxy.lsn)?,
        })
    }
);

#[cfg(test)]
mod tests {
    use crate::{
        core::VolumeId,
        local::fjall_storage::fjall_repr::testutil::{
            test_invalid, test_roundtrip, test_serialized_order,
        },
        lsn, pageidx,
    };

    use super::*;

    #[test]
    fn test_volume_id() {
        test_roundtrip(VolumeId::random());
        test_roundtrip(VolumeId::EMPTY);
        test_invalid::<VolumeId>(b"");
        test_invalid::<VolumeId>(b"asdf");
        test_invalid::<VolumeId>(SegmentId::random().as_bytes());
    }

    #[test]
    fn test_log_id() {
        test_roundtrip(LogId::random());
        test_roundtrip(LogId::EMPTY);
        test_invalid::<LogId>(b"");
        test_invalid::<LogId>(b"asdf");
        test_invalid::<LogId>(SegmentId::random().as_bytes());
    }

    #[test]
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

    #[test]
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
