use std::{
    fmt::{Debug, Display},
    hash::Hash,
    time::SystemTime,
};

use bytes::Bytes;
use prefix::Prefix;
use thiserror::Error;
use time::GidTimestamp;
use zerocopy::{try_transmute, Immutable, IntoBytes, KnownLayout, TryFromBytes};

use crate::byte_unit::ByteUnit;

const GID_SIZE: ByteUnit = ByteUnit::new(16);

mod prefix;
mod time;

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Hash, IntoBytes, TryFromBytes, Immutable, KnownLayout,
)]
#[repr(C)]
pub struct Gid<const P: u8> {
    prefix: Prefix,
    ts: GidTimestamp,
    random: [u8; 9],
}

pub type VolumeId = Gid<{ Prefix::Volume.as_u8() }>;
pub type SegmentId = Gid<{ Prefix::Segment.as_u8() }>;

static_assertions::assert_eq_size!(VolumeId, [u8; GID_SIZE.as_usize()]);

impl<const P: u8> Gid<P> {
    pub fn random() -> Self {
        Self {
            prefix: Prefix::from_u8(P),
            ts: GidTimestamp::now(),
            random: rand::random(),
        }
    }

    pub fn pretty(&self) -> String {
        bs58::encode(self.as_bytes()).into_string()
    }

    pub fn short(&self) -> String {
        self.pretty()[..8].to_string()
    }

    pub fn as_time(&self) -> SystemTime {
        self.ts.as_time()
    }
}

impl<const P: u8> Display for Gid<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.pretty())
    }
}

impl<const P: u8> Debug for Gid<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.short())
    }
}

impl<const P: u8> AsRef<[u8]> for Gid<P> {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum GidParseErr {
    #[error("invalid base58 encoding")]
    DecodeErr(#[from] bs58::decode::Error),

    #[error("invalid length")]
    InvalidLength,

    #[error("invalid binary layout for id")]
    InvalidLayout,
}

impl<const P: u8> TryFrom<[u8; GID_SIZE.as_usize()]> for Gid<P> {
    type Error = GidParseErr;

    fn try_from(value: [u8; GID_SIZE.as_usize()]) -> Result<Self, Self::Error> {
        try_transmute!(value).map_err(|_| GidParseErr::InvalidLayout)
    }
}

impl<const P: u8> TryFrom<&str> for Gid<P> {
    type Error = GidParseErr;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        // To calculate this compute ceil(16 * (log2(256) / log2(58))) + 1
        // The format is the prefix byte followed by up to 22 base58 characters
        static MAX_ENCODED_LEN: usize = 22;

        // Note: we require that Gid's always are their maximum length
        // This is currently guaranteed for well-constructed Gid's due to the
        // prefix byte always occupying the high bits.

        // verify the length
        if value.len() != MAX_ENCODED_LEN {
            return Err(GidParseErr::InvalidLength);
        }

        // parse from base58
        let bytes: [u8; GID_SIZE.as_usize()] = bs58::decode(value.as_bytes()).into_array_const()?;
        bytes.try_into()
    }
}

impl<const P: u8> TryFrom<Bytes> for Gid<P> {
    type Error = GidParseErr;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        if value.len() != GID_SIZE.as_usize() {
            return Err(GidParseErr::InvalidLength);
        }

        let bytes: [u8; GID_SIZE.as_usize()] = value.as_ref().try_into().unwrap();
        bytes.try_into()
    }
}

impl<'a, const P: u8> TryFrom<&'a [u8]> for &'a Gid<P> {
    type Error = GidParseErr;

    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        if value.len() != GID_SIZE.as_usize() {
            return Err(GidParseErr::InvalidLength);
        }

        Gid::<P>::try_ref_from_bytes(value).map_err(|_| GidParseErr::InvalidLayout)
    }
}

impl<const P: u8> From<&Gid<P>> for Bytes {
    fn from(val: &Gid<P>) -> Self {
        Bytes::copy_from_slice(val.as_bytes())
    }
}

impl<const P: u8> From<Gid<P>> for Bytes {
    fn from(val: Gid<P>) -> Self {
        (&val).into()
    }
}

#[cfg(test)]
mod tests {
    use rand::random;

    use super::*;

    fn mkgid(prefix: u8, ts: SystemTime, random: u8) -> [u8; 16] {
        let mut bytes = [random; 16];
        bytes[0] = prefix;
        bytes[1..7].copy_from_slice(GidTimestamp::from(ts).as_bytes());
        bytes
    }

    #[test]
    fn test_size() {
        let g = SegmentId {
            prefix: Prefix::Segment,
            ts: GidTimestamp::now(),
            random: [0x00; 9],
        };
        println!("gid: {}", g.pretty());
        assert_eq!(g.pretty().len(), 22);

        let g = VolumeId {
            prefix: Prefix::Volume,
            ts: GidTimestamp::now(),
            random: [0xff; 9],
        };
        println!("gid: {}", g.pretty());
        assert_eq!(g.pretty().len(), 22);
    }

    #[test]
    fn test_round_trip() {
        let id = SegmentId::random();

        // round trip through pretty format
        let pretty = id.pretty();
        println!("random: {pretty}");
        let parsed: SegmentId = pretty.as_str().try_into().unwrap();
        assert_eq!(id, parsed);

        // round trip through bytes
        let bytes: Bytes = (&id).into();
        let parsed: SegmentId = bytes.try_into().unwrap();
        assert_eq!(id, parsed);
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
            let result: Result<VolumeId, _> = case.try_into();
            assert_eq!(result, Err(GidParseErr::InvalidLength));
        }

        // bad encoding
        let cases = ["GontbnaXtUE3!BbafyDiJt", "zzzzzzzzzzzzzzzzzzzzzz"];
        for &case in cases.iter() {
            let result: Result<VolumeId, _> = case.try_into();
            assert!(matches!(result, Err(GidParseErr::DecodeErr(_))));
        }

        // bad layout
        let cases = ["GGGGGGGGGGGGGGGGGGGGGG"];
        for &case in cases.iter() {
            let result: Result<VolumeId, _> = case.try_into();
            assert_eq!(result, Err(GidParseErr::InvalidLayout));
        }

        // bad layout, direct from binary repr
        let cases = [
            mkgid(5, SystemTime::now(), 0),
            mkgid(5, SystemTime::now(), random()),
        ];
        for &case in cases.iter() {
            let result: Result<VolumeId, _> = case.try_into();
            assert_eq!(result, Err(GidParseErr::InvalidLayout));
        }
    }
}
