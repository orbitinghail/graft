use std::{fmt::Debug, ops::Deref};

use bytes::Bytes;
use culprit::Culprit;
use thiserror::Error;

use crate::byte_unit::ByteUnit;

pub const PAGESIZE: ByteUnit = ByteUnit::from_kb(4);
static_assertions::const_assert!(PAGESIZE.is_power_of_two());

pub const EMPTY_PAGE: Page = Page(Bytes::from_static(&[0; PAGESIZE.as_usize()]));

#[derive(Clone, PartialEq, Eq)]
pub struct Page(Bytes);

impl Deref for Page {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8]> for Page {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Error)]
#[error("Pages must have size {PAGESIZE}")]
pub struct PageSizeErr;

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

impl TryFrom<Bytes> for Page {
    type Error = Culprit<PageSizeErr>;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        if value.len() != PAGESIZE.as_usize() {
            let size = ByteUnit::new(value.len() as u64);
            return Err(Culprit::new_with_note(
                PageSizeErr,
                format!("invalid page size {size}"),
            ));
        }

        Ok(Page(value))
    }
}

impl TryFrom<&[u8]> for Page {
    type Error = Culprit<PageSizeErr>;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != PAGESIZE.as_usize() {
            let size = ByteUnit::new(value.len() as u64);
            return Err(Culprit::new_with_note(
                PageSizeErr,
                format!("invalid page size {size}"),
            ));
        }

        Ok(Page(Bytes::copy_from_slice(value)))
    }
}

impl Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Page({PAGESIZE})")
    }
}
