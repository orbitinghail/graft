use std::ops::RangeBounds;

use crate::{page_count::PageCount, page_offset::PageOffset};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRange {
    /// Inclusive start of the page range
    start: PageOffset,
    /// Exclusive end of the page range
    end: PageOffset,
}

impl PageRange {
    #[inline]
    pub const fn new(start: PageOffset, end: PageOffset) -> Self {
        Self { start, end }
    }

    #[inline]
    /// Returns the inclusive start of the page range
    pub const fn start(&self) -> PageOffset {
        self.start
    }

    #[inline]
    /// Returns the exclusive end of the page range
    pub const fn end(&self) -> PageOffset {
        self.end
    }

    #[inline]
    /// Returns the number of pages in the range
    pub const fn len(&self) -> PageCount {
        PageCount::new(self.end.to_u32() - self.start.to_u32())
    }
}

impl Iterator for PageRange {
    type Item = PageOffset;

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len().to_usize();
        (len, Some(len))
    }

    fn next(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            let next = self.start;
            self.start = self.start.saturating_next();
            Some(next)
        } else {
            None
        }
    }
}

impl DoubleEndedIterator for PageRange {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.end > self.start {
            self.end = self.end.saturating_prev();
            Some(self.end)
        } else {
            None
        }
    }
}

impl RangeBounds<PageOffset> for PageRange {
    fn start_bound(&self) -> std::ops::Bound<&PageOffset> {
        std::ops::Bound::Included(&self.start)
    }

    fn end_bound(&self) -> std::ops::Bound<&PageOffset> {
        std::ops::Bound::Excluded(&self.end)
    }
}
