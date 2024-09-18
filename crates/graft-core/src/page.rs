use std::{fmt::Debug, ops::Deref};

use bytes::Bytes;

pub const PAGESIZE: usize = 4096;
static_assertions::const_assert!(PAGESIZE.is_power_of_two());

#[derive(Clone)]
pub struct Page(Bytes);

impl Deref for Page {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&[u8]> for Page {
    fn from(value: &[u8]) -> Self {
        Page(Bytes::copy_from_slice(value))
    }
}

impl Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Page").field(&self.0.len()).finish()
    }
}
