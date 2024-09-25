use std::{array::TryFromSliceError, fmt::Debug, ops::Deref};

use crate::byte_unit::ByteUnit;

pub const PAGESIZE: ByteUnit = ByteUnit::from_kb(4);
static_assertions::const_assert!(PAGESIZE.is_power_of_two());

#[derive(Clone, PartialEq, Eq)]
pub struct Page([u8; PAGESIZE.as_usize()]);

impl Deref for Page {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Page {
    pub fn as_ref(&self) -> PageRef<'_> {
        PageRef(&self.0)
    }
}

impl From<&[u8; PAGESIZE.as_usize()]> for Page {
    fn from(value: &[u8; PAGESIZE.as_usize()]) -> Self {
        Page(*value)
    }
}

impl Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Page").field(&self.0.len()).finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct PageRef<'a>(&'a [u8; PAGESIZE.as_usize()]);

impl<'a> TryFrom<&'a [u8]> for PageRef<'a> {
    type Error = TryFromSliceError;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        Ok(PageRef(value.try_into()?))
    }
}

impl<'a> From<&'a [u8; PAGESIZE.as_usize()]> for PageRef<'a> {
    fn from(value: &'a [u8; PAGESIZE.as_usize()]) -> Self {
        PageRef(value)
    }
}

impl Deref for PageRef<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl From<PageRef<'_>> for Page {
    fn from(value: PageRef<'_>) -> Self {
        Page(*value.0)
    }
}

impl Debug for PageRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PageRef").field(&self.0.len()).finish()
    }
}
