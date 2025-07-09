use std::{
    fmt::Debug,
    ops::{Index, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo},
};

use bytes::{Buf, Bytes, BytesMut};
use culprit::Culprit;
use thiserror::Error;

use crate::byte_unit::ByteUnit;

pub const PAGESIZE: ByteUnit = ByteUnit::from_kb(4);
static_assertions::const_assert!(PAGESIZE.is_power_of_two());

static STATIC_EMPTY_PAGE: [u8; PAGESIZE.as_usize()] = [0; PAGESIZE.as_usize()];
pub const EMPTY_PAGE: Page = Page(Bytes::from_static(&STATIC_EMPTY_PAGE));

#[derive(Debug, Error)]
#[error("Pages must have size {PAGESIZE}")]
pub struct PageSizeErr;

impl PageSizeErr {
    #[track_caller]
    fn check(size: usize) -> Result<(), Culprit<Self>> {
        if size != PAGESIZE.as_usize() {
            let size = ByteUnit::new(size as u64);
            Err(Culprit::new_with_note(
                Self,
                format!("invalid page size {size}"),
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
#[repr(transparent)]
pub struct Page(Bytes);

impl Page {
    /// Returns true if all of the page's bytes are 0.
    pub fn is_empty(&self) -> bool {
        self.0.as_ref() == STATIC_EMPTY_PAGE
    }

    pub fn from_buf<T: Buf>(mut buf: T) -> Result<Self, Culprit<PageSizeErr>> {
        PageSizeErr::check(buf.remaining())?;
        Ok(Page(buf.copy_to_bytes(buf.remaining())))
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

macro_rules! derive_index_ops {
    ($($idx:ty),+) => {
        $(
            impl Index<$idx> for Page {
                type Output = [u8];

                #[inline]
                fn index(&self, index: $idx) -> &Self::Output {
                    &self.0.as_ref()[index]
                }
            }
        )+
    };
}
derive_index_ops!(
    Range<usize>,
    RangeTo<usize>,
    RangeFrom<usize>,
    RangeFull,
    RangeInclusive<usize>
);

impl AsRef<[u8]> for Page {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<&[u8; PAGESIZE.as_usize()]> for Page {
    fn from(value: &[u8; PAGESIZE.as_usize()]) -> Self {
        Page(Bytes::copy_from_slice(value))
    }
}

impl From<Page> for Bytes {
    fn from(value: Page) -> Self {
        value.0
    }
}

impl From<Page> for BytesMut {
    fn from(value: Page) -> Self {
        value.0.into()
    }
}

impl TryFrom<BytesMut> for Page {
    type Error = Culprit<PageSizeErr>;

    fn try_from(value: BytesMut) -> Result<Self, Self::Error> {
        value.freeze().try_into()
    }
}

impl TryFrom<Bytes> for Page {
    type Error = Culprit<PageSizeErr>;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        PageSizeErr::check(value.len())?;
        Ok(Page(value))
    }
}

impl TryFrom<&[u8]> for Page {
    type Error = Culprit<PageSizeErr>;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        PageSizeErr::check(value.len())?;
        Ok(Page(Bytes::copy_from_slice(value)))
    }
}

impl Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Page({PAGESIZE})")
    }
}
