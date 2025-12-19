use std::ops::{BitOrAssign, RangeBounds, RangeInclusive};

use bytes::Bytes;
use splinter_rs::{CowSplinter, Cut, PartitionRead, PartitionWrite, Splinter};

use crate::core::{PageCount, PageIdx};
use crate::derive_newtype_proxy;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PageSet {
    splinter: CowSplinter<Bytes>,
}

impl PageSet {
    pub const EMPTY: Self = Self {
        splinter: CowSplinter::Owned(Splinter::EMPTY),
    };

    #[inline]
    pub fn new(splinter: CowSplinter<Bytes>) -> Self {
        assert!(
            !splinter.contains(0),
            "Invalid PageSet: Splinter contains PageIdx 0"
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
    pub fn cardinality(&self) -> PageCount {
        PageCount::from(self.splinter.cardinality() as u32)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.splinter.is_empty()
    }

    #[inline]
    pub fn first(&self) -> Option<PageIdx> {
        self.splinter
            .select(0)
            // SAFETY: The PageSet type verifies that `0` is not contained by the
            // Splinter at creation time.
            .map(|n| unsafe { PageIdx::new_unchecked(n) })
    }

    #[inline]
    pub fn last(&self) -> Option<PageIdx> {
        self.splinter
            .last()
            // SAFETY: The PageSet type verifies that `0` is not contained by the
            // Splinter at creation time.
            .map(|n| unsafe { PageIdx::new_unchecked(n) })
    }

    #[inline]
    pub fn insert(&mut self, pageidx: PageIdx) -> bool {
        self.splinter.insert(pageidx.to_u32())
    }

    #[inline]
    pub fn contains(&self, pageidx: PageIdx) -> bool {
        self.splinter.contains(pageidx.to_u32())
    }

    #[inline]
    pub fn contains_all<R: RangeBounds<PageIdx>>(&self, pages: &R) -> bool {
        let r = (
            pages.start_bound().map(|start| start.to_u32()),
            pages.end_bound().map(|end| end.to_u32()),
        );
        self.splinter.contains_all(r)
    }

    #[inline]
    pub fn contains_any<R: RangeBounds<PageIdx>>(&self, pages: &R) -> bool {
        let r = (
            pages.start_bound().map(|start| start.to_u32()),
            pages.end_bound().map(|end| end.to_u32()),
        );
        self.splinter.contains_any(r)
    }

    /// Truncates the `PageSet` to the specified number of pages.
    pub fn truncate(&mut self, page_count: PageCount) {
        if page_count == PageCount::ZERO {
            self.splinter = CowSplinter::Owned(Splinter::EMPTY);
        } else if let Some(last_pageidx) = page_count.last_pageidx()
            && last_pageidx != PageIdx::LAST
        {
            self.remove_page_range(last_pageidx.saturating_next()..);
        }
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
    pub fn cut(&mut self, rhs: &PageSet) -> PageSet {
        self.splinter.to_mut().cut(&rhs.splinter).into()
    }

    pub fn iter(&self) -> impl Iterator<Item = PageIdx> {
        self.splinter.iter().map(|v| {
            // SAFETY: The PageSet type verifies that `0` is not contained by the
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

    pub fn inner(self) -> CowSplinter<Bytes> {
        self.splinter
    }
}

impl From<Splinter> for PageSet {
    #[inline]
    fn from(value: Splinter) -> Self {
        Self::new(CowSplinter::Owned(value))
    }
}

impl From<PageSet> for Splinter {
    fn from(value: PageSet) -> Self {
        value.splinter.into_owned()
    }
}

derive_newtype_proxy!(
    newtype (PageSet)
    with empty value (PageSet::EMPTY)
    with proxy type (Bytes) and encoding (bilrost::encoding::General)
    with sample value (PageSet::new(CowSplinter::from_iter(1u32..10)))
    into_proxy(&self) {
        self.splinter.encode_to_bytes()
    }
    from_proxy(&mut self, proxy) {
        *self = PageSet::new(
            CowSplinter::from_bytes(proxy)
                .map_err(|_| bilrost::DecodeErrorKind::InvalidValue)?
        );
        Ok(())
    }
);

impl BitOrAssign<Self> for PageSet {
    fn bitor_assign(&mut self, rhs: Self) {
        self.splinter.to_mut().bitor_assign(rhs.splinter);
    }
}
