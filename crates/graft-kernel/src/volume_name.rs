use std::{fmt::Display, ops::Deref, str::FromStr};

use bytes::Bytes;
use bytestring::ByteString;
use graft_core::derive_newtype_proxy;
use thiserror::Error;

pub const MAX_VOLUME_NAME_LEN: usize = 128;

#[derive(Debug, Error)]
pub enum VolumeNameErr {
    #[error("Volume names must conform to the regex: ^[-_a-zA-Z0-9]{{0,128}}$")]
    InvalidFormat,

    #[error("Volume name must be a valid UTF-8 string")]
    InvalidUtf8(#[from] std::str::Utf8Error),
}

/// The name of a `Graft`. `VolumeName`s are human readable, but must
/// conform to the regex: `^[-_a-zA-Z0-9]{0,128}$`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct VolumeName(ByteString);

impl VolumeName {
    pub const DEFAULT: Self = VolumeName(ByteString::from_static("default"));

    pub fn new<T: Deref<Target = str>>(raw: T) -> Result<Self, VolumeNameErr> {
        raw.parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    fn validate<T: Deref<Target = str>>(raw: T) -> Result<T, VolumeNameErr> {
        if raw.is_empty()
            || raw.len() > MAX_VOLUME_NAME_LEN
            || !raw
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            Err(VolumeNameErr::InvalidFormat)
        } else {
            Ok(raw)
        }
    }
}

impl Display for VolumeName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Default for VolumeName {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl TryFrom<&[u8]> for VolumeName {
    type Error = VolumeNameErr;

    fn try_from(raw: &[u8]) -> Result<Self, Self::Error> {
        let raw_str = std::str::from_utf8(raw)?;
        raw_str.parse()
    }
}

impl TryFrom<ByteString> for VolumeName {
    type Error = VolumeNameErr;

    fn try_from(raw: ByteString) -> Result<Self, Self::Error> {
        Self::validate(raw).map(VolumeName)
    }
}

impl TryFrom<Bytes> for VolumeName {
    type Error = VolumeNameErr;

    fn try_from(raw: Bytes) -> Result<Self, Self::Error> {
        Self::try_from(ByteString::try_from(raw)?)
    }
}

impl FromStr for VolumeName {
    type Err = VolumeNameErr;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        Self::validate(raw).map(|s| VolumeName(s.into()))
    }
}

impl From<VolumeName> for ByteString {
    fn from(value: VolumeName) -> Self {
        value.0
    }
}

impl From<VolumeName> for Bytes {
    fn from(value: VolumeName) -> Self {
        value.0.into_bytes()
    }
}

derive_newtype_proxy!(
    newtype (VolumeName)
    with empty value (VolumeName::DEFAULT)
    with proxy type (ByteString) and encoding (bilrost::encoding::General)
    with sample value (VolumeName::new("sample_name").unwrap())
    into_proxy(&self) { self.0.clone() }
    from_proxy(&mut self, proxy) {
        *self = VolumeName::try_from(proxy).map_err(|_| DecodeErrorKind::InvalidValue)?;
        Ok(())
    }
);
