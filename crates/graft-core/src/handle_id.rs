use std::str::FromStr;

use thiserror::Error;

pub const MAX_HANDLE_ID_LEN: usize = 128;

#[derive(Debug, Error)]
#[error("Invalid handle ID")]
pub struct HandleIdErr;

/// Represents a VolumeHandle's id. HandleIds are human readable, but must
/// conform to the regex: `^[-_a-zA-Z0-9]{0,128}$`
pub struct HandleId(String);

impl HandleId {
    pub fn new(raw: &str) -> Result<Self, HandleIdErr> {
        raw.parse()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl TryFrom<&[u8]> for HandleId {
    type Error = HandleIdErr;

    fn try_from(raw: &[u8]) -> Result<Self, Self::Error> {
        let raw_str = std::str::from_utf8(raw).map_err(|_| HandleIdErr)?;
        raw_str.parse()
    }
}

impl FromStr for HandleId {
    type Err = HandleIdErr;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        if raw.is_empty()
            || raw.len() > MAX_HANDLE_ID_LEN
            || !raw
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            Err(HandleIdErr)
        } else {
            Ok(Self(raw.to_string()))
        }
    }
}
