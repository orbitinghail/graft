use std::fmt::{Debug, Display};

use borsh::{BorshDeserialize, BorshSchema, BorshSerialize};
use bytes::Bytes;
use thiserror::Error;

use crate::byte_unit::ByteUnit;

const GUID_SIZE: ByteUnit = ByteUnit::new(16);

#[derive(
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    BorshSerialize,
    BorshDeserialize,
    BorshSchema,
    zerocopy::AsBytes,
    zerocopy::FromZeroes,
    zerocopy::FromBytes,
)]
#[repr(transparent)]
pub struct Guid<const PREFIX: char>([u8; GUID_SIZE.as_usize()]);

pub type VolumeId = Guid<'V'>;
pub type SegmentId = Guid<'S'>;

static_assertions::assert_eq_size!(Guid<'G'>, [u8; GUID_SIZE.as_usize()]);

impl<const P: char> Guid<P> {
    pub fn random() -> Self {
        Self(rand::random())
    }

    pub fn derive(name: &str) -> Self {
        let mut hasher = blake3::Hasher::default();
        hasher.update(name.as_bytes());
        let data = hasher.finalize().as_bytes()[..GUID_SIZE.as_usize()]
            .try_into()
            .unwrap();
        Self(data)
    }

    pub fn pretty(&self) -> String {
        let data = bs58::encode(&self.0).into_string();
        format!("{}{}", P, &data[0..])
    }

    pub fn short(&self) -> String {
        self.pretty()[..8].to_string()
    }
}

impl<const P: char> Display for Guid<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.pretty())
    }
}

impl<const P: char> Debug for Guid<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.short())
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum GuidParseError {
    #[error("invalid base58 encoding")]
    DecodeError(#[from] bs58::decode::Error),

    #[error("invalid length")]
    InvalidLength,

    #[error("invalid prefix: {0}")]
    InvalidPrefix(String),
}

impl<const P: char> TryFrom<&str> for Guid<P> {
    type Error = GuidParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // To calculate this compute ceil(16 * (log2(256) / log2(58))) + 1
        // The format is the prefix byte followed by up to 22 base58 characters
        static MAX_ENCODED_LEN: usize = 23;
        // the minimum length is 17 bytes
        static MIN_ENCODED_LEN: usize = 17;

        // verify the length
        if value.len() < MIN_ENCODED_LEN || value.len() > MAX_ENCODED_LEN {
            return Err(GuidParseError::InvalidLength);
        }

        let (prefix, rest) = value.split_at(1);

        // verify the prefix
        if prefix.chars().next().unwrap() != P {
            return Err(GuidParseError::InvalidPrefix(prefix.to_string()));
        }

        Ok(Guid(bs58::decode(rest.as_bytes()).into_array_const()?))
    }
}

impl<const P: char> TryFrom<Bytes> for Guid<P> {
    type Error = GuidParseError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        if value.len() != GUID_SIZE.as_usize() {
            return Err(GuidParseError::InvalidLength);
        }

        let raw: [u8; GUID_SIZE.as_usize()] = value.as_ref().try_into().unwrap();
        Ok(Guid(raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_trip() {
        let guid = Guid::<'X'>::random();
        let pretty = guid.pretty();
        println!("random: {pretty}");
        let parsed: Guid<'X'> = pretty.as_str().try_into().unwrap();
        assert_eq!(guid, parsed);

        let guid = Guid::<'X'>::derive("hello world");
        assert_eq!(guid, Guid::<'X'>::derive("hello world"));
        let pretty = guid.pretty();
        println!("derived: {pretty}");
        let parsed: Guid<'X'> = pretty.as_str().try_into().unwrap();
        assert_eq!(guid, parsed);
    }

    #[test]
    fn test_invalid_parse() {
        // wrong lengths
        let cases = [
            "invalid",
            "",
            "asdfjasdkfjkajfe",
            "superlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstringsuperlongstring",
            "X111111111111111",
        ];

        for &case in cases.iter() {
            let result: Result<Guid<'X'>, _> = case.try_into();
            assert_eq!(result, Err(GuidParseError::InvalidLength));
        }

        // bad prefix
        let cases = ["Y2v7DnXv9qw2fN7BjPRqJVh", "Y1111111111111111"];
        for &case in cases.iter() {
            let result: Result<Guid<'X'>, _> = case.try_into();
            assert_eq!(result, Err(GuidParseError::InvalidPrefix("Y".into())));
        }

        // bad encoding
        let cases = ["Xasdfasdfas!dfasdf"];
        for &case in cases.iter() {
            let result: Result<Guid<'X'>, _> = case.try_into();
            assert!(matches!(result, Err(GuidParseError::DecodeError(_))));
        }
    }
}
