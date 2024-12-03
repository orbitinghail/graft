use std::ops::RangeBounds;

use crate::{page_count::PageCount, page_offset::PageOffset};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PageRange {
    start: PageOffset,
    end: PageOffset,
}

impl PageRange {
    pub fn new<T: Into<PageOffset>>(start: T, end: T) -> Self {
        Self { start: start.into(), end: end.into() }
    }

    #[inline]
    pub fn start(&self) -> PageOffset {
        self.start
    }

    #[inline]
    pub fn end(&self) -> PageOffset {
        self.end
    }

    #[inline]
    pub fn len(&self) -> PageCount {
        PageCount::new(u32::from(self.end) - u32::from(self.start))
    }
}

impl Iterator for PageRange {
    type Item = PageOffset;

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len: u32 = self.len().into();
        (len as usize, Some(len as usize))
    }

    fn next(&mut self) -> Option<Self::Item> {
        if self.start < self.end {
            let next = self.start;
            self.start = self.start + PageCount::ONE;
            Some(next)
        } else {
            None
        }
    }
}

impl DoubleEndedIterator for PageRange {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.end > self.start {
            self.end = self.end - PageCount::ONE;
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
