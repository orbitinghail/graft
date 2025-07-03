use std::{ops::Deref, str::FromStr};

use bytes::Bytes;
use bytestring::ByteString;
use thiserror::Error;

use crate::derive_newtype_proxy;

pub const MAX_HANDLE_ID_LEN: usize = 128;

#[derive(Debug, Error)]
#[error("Invalid handle ID")]
pub struct HandleIdErr;

/// Represents a `VolumeHandle`'s id. `HandleIds` are human readable, but must
/// conform to the regex: `^[-_a-zA-Z0-9]{0,128}$`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct HandleId(ByteString);

impl HandleId {
    pub const DEFAULT: Self = HandleId(ByteString::from_static("default"));

    pub fn new(raw: &str) -> Result<Self, HandleIdErr> {
        raw.parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    fn validate<T: Deref<Target = str>>(raw: T) -> Result<T, HandleIdErr> {
        if raw.is_empty()
            || raw.len() > MAX_HANDLE_ID_LEN
            || !raw
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            Err(HandleIdErr)
        } else {
            Ok(raw)
        }
    }
}

impl Default for HandleId {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl TryFrom<&[u8]> for HandleId {
    type Error = HandleIdErr;

    fn try_from(raw: &[u8]) -> Result<Self, Self::Error> {
        let raw_str = std::str::from_utf8(raw).map_err(|_| HandleIdErr)?;
        raw_str.parse()
    }
}

impl TryFrom<ByteString> for HandleId {
    type Error = HandleIdErr;

    fn try_from(raw: ByteString) -> Result<Self, Self::Error> {
        Self::validate(raw).map(|s| HandleId(s))
    }
}

impl FromStr for HandleId {
    type Err = HandleIdErr;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::validate(raw).map(|s| HandleId(s.into()))
    }
}

impl From<HandleId> for ByteString {
    fn from(handle_id: HandleId) -> Self {
        handle_id.0
    }
}

impl From<HandleId> for Bytes {
    fn from(handle_id: HandleId) -> Self {
        handle_id.0.into_bytes()
    }
}

derive_newtype_proxy!(
    newtype (HandleId)
    with empty value (HandleId::DEFAULT)
    with proxy type (ByteString) and encoding (bilrost::encoding::General)
    with sample value (HandleId::new("sample_handle").unwrap())
    into_proxy(&self) { self.0.clone() }
    from_proxy(&mut self, proxy) {
        *self = HandleId::try_from(proxy).map_err(|_| DecodeErrorKind::InvalidValue)?;
        Ok(())
    }
);
