use std::ops::{RangeBounds, RangeInclusive};

use bytes::Bytes;
use splinter_rs::{CowSplinter, Cut, PartitionRead, PartitionWrite, Splinter};

use crate::{PageIdx, derive_newtype_proxy};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Graft {
    splinter: CowSplinter<Bytes>,
}

impl Graft {
    pub const EMPTY: Self = Self {
        splinter: CowSplinter::Owned(Splinter::EMPTY),
    };

    #[inline]
    pub fn new(splinter: CowSplinter<Bytes>) -> Self {
        assert!(
            !splinter.contains(0),
            "Invalid Graft: Splinter contains PageIdx 0"
        );
        Self { splinter }
    }

    #[inline]
    pub fn from_range(range: RangeInclusive<PageIdx>) -> Self {
        Self {
            splinter: Splinter::from(range.start().to_u32()..=range.end().to_u32()).into(),
        }
    }

    #[inline]
    pub fn cardinality(&self) -> usize {
        self.splinter.cardinality()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.splinter.is_empty()
    }

    #[inline]
    pub fn insert(&mut self, pageidx: PageIdx) -> bool {
        self.splinter.insert(pageidx.to_u32())
    }

    #[inline]
    pub fn contains(&self, pageidx: PageIdx) -> bool {
        self.splinter.contains(pageidx.to_u32())
    }

    pub fn remove_page_range<R: RangeBounds<PageIdx>>(&mut self, pages: R) {
        let r = (
            pages.start_bound().map(|start| start.to_u32()),
            pages.end_bound().map(|end| end.to_u32()),
        );

        self.splinter.remove_range(r);
    }

    /// Returns the intersection between self and rhs while removing the
    /// intersection from self
    pub fn cut(&mut self, rhs: &Graft) -> Graft {
        self.splinter.to_mut().cut(&rhs.splinter).into()
    }

    pub fn iter(&self) -> impl Iterator<Item = PageIdx> {
        self.splinter.iter().map(|v| {
            // SAFETY: The Graft type verifies that `0` is not contained by the
            // Splinter at creation time.
            unsafe { PageIdx::new_unchecked(v) }
        })
    }

    pub fn splinter(&self) -> &CowSplinter<Bytes> {
        &self.splinter
    }

    pub fn splinter_mut(&mut self) -> &CowSplinter<Bytes> {
        &mut self.splinter
    }
}

impl From<Splinter> for Graft {
    #[inline]
    fn from(value: Splinter) -> Self {
        Self::new(CowSplinter::Owned(value))
    }
}

impl From<Graft> for Splinter {
    fn from(value: Graft) -> Self {
        value.splinter.into_owned()
    }
}

derive_newtype_proxy!(
    newtype (Graft)
    with empty value (Graft::EMPTY)
    with proxy type (Bytes) and encoding (bilrost::encoding::General)
    with sample value (Graft::new(CowSplinter::from_iter(1u32..10)))
    into_proxy(&self) {
        self.splinter.encode_to_bytes()
    }
    from_proxy(&mut self, proxy) {
        *self = Graft::new(
            CowSplinter::from_bytes(proxy)
                .map_err(|_| bilrost::DecodeErrorKind::InvalidValue)?
        );
        Ok(())
    }
);
