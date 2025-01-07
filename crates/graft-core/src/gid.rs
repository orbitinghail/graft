use std::{
    fmt::{Debug, Display},
    hash::Hash,
    str::FromStr,
    time::SystemTime,
};

use bytes::Bytes;
use prefix::Prefix;
use thiserror::Error;
use time::GidTimestamp;
use zerocopy::{ConvertError, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use crate::{byte_unit::ByteUnit, zerocopy_err::ZerocopyErr};

const GID_SIZE: ByteUnit = ByteUnit::new(16);

mod prefix;
mod time;

#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    IntoBytes,
    TryFromBytes,
    Immutable,
    KnownLayout,
    Unaligned,
)]
#[repr(C)]
pub struct Gid<P: Prefix> {
    prefix: P,
    ts: GidTimestamp,
    random: [u8; 9],
}

pub type VolumeId = Gid<prefix::Volume>;
pub type SegmentId = Gid<prefix::Segment>;
pub type ClientId = Gid<prefix::Client>;

static_assertions::assert_eq_size!(VolumeId, [u8; GID_SIZE.as_usize()]);

impl<P: Prefix> Gid<P> {
    pub const SIZE: ByteUnit = GID_SIZE;

    pub fn random() -> Self {
        Self {
            prefix: P::default(),
            ts: GidTimestamp::now(),
            random: rand::random(),
        }
    }

    pub fn pretty(&self) -> String {
        bs58::encode(self.as_bytes()).into_string()
    }

    /// returns only the random portion of the Gid encoded to bs58
    pub fn short(&self) -> String {
        bs58::encode(&self.random).into_string()
    }

    pub fn as_time(&self) -> SystemTime {
        self.ts.as_time()
    }

    pub fn copy_to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

impl<P: Prefix> Display for Gid<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.pretty())
    }
}

impl<P: Prefix> Debug for Gid<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.short())
    }
}

impl<P: Prefix> AsRef<[u8]> for Gid<P> {
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

    #[error("invalid binary layout for gid")]
    Corrupt(#[from] ZerocopyErr),
}

impl<A, S, V> From<ConvertError<A, S, V>> for GidParseErr {
    #[inline]
    fn from(value: ConvertError<A, S, V>) -> Self {
        Self::Corrupt(value.into())
    }
}

impl<P: Prefix> FromStr for Gid<P> {
    type Err = GidParseErr;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // To calculate this compute ceil(16 * (log2(256) / log2(58)))
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

impl<P: Prefix> TryFrom<Bytes> for Gid<P> {
    type Error = GidParseErr;

    #[inline]
    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        if value.len() != GID_SIZE.as_usize() {
            return Err(GidParseErr::InvalidLength);
        }

        Ok(Gid::<P>::try_read_from_bytes(&value)?)
    }
}

impl<'a, P: Prefix> TryFrom<&'a [u8]> for &'a Gid<P> {
    type Error = GidParseErr;

    #[inline]
    fn try_from(value: &'a [u8]) -> Result<Self, Self::Error> {
        if value.len() != GID_SIZE.as_usize() {
            return Err(GidParseErr::InvalidLength);
        }

        Ok(Gid::<P>::try_ref_from_bytes(value)?)
    }
}

impl<P: Prefix> TryFrom<[u8; GID_SIZE.as_usize()]> for Gid<P> {
    type Error = GidParseErr;

    #[inline]
    fn try_from(value: [u8; GID_SIZE.as_usize()]) -> Result<Self, Self::Error> {
        Ok(Gid::<P>::try_read_from_bytes(&value)?)
    }
}

#[cfg(test)]
mod tests {

    use assert_matches::assert_matches;
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
            prefix: Default::default(),
            ts: GidTimestamp::now(),
            random: [0x00; 9],
        };
        println!("gid: {}", g.pretty());
        assert_eq!(g.pretty().len(), 22);

        let g = VolumeId {
            prefix: Default::default(),
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
        let parsed: SegmentId = pretty.parse().unwrap();
        assert_eq!(id, parsed);

        // round trip through bytes
        let bytes = id.copy_to_bytes();
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
            let result: Result<VolumeId, _> = case.parse();
            assert_matches!(result.unwrap_err(), GidParseErr::InvalidLength);
        }

        // bad encoding
        let cases = ["GontbnaXtUE3!BbafyDiJt", "zzzzzzzzzzzzzzzzzzzzzz"];
        for &case in cases.iter() {
            let result: Result<VolumeId, _> = case.parse();
            assert_matches!(result.unwrap_err(), GidParseErr::DecodeErr(_));
        }

        // bad layout
        let cases = ["GGGGGGGGGGGGGGGGGGGGGG"];
        for &case in cases.iter() {
            let result: Result<VolumeId, _> = case.parse();
            assert_matches!(
                result.unwrap_err(),
                GidParseErr::Corrupt(ZerocopyErr::InvalidData)
            );
        }

        // bad layout, direct from binary repr
        let cases = [
            mkgid(5, SystemTime::now(), 0),
            mkgid(5, SystemTime::now(), random()),
        ];
        for &case in cases.iter() {
            let result: Result<VolumeId, _> = case.try_into();
            assert_matches!(
                result.unwrap_err(),
                GidParseErr::Corrupt(ZerocopyErr::InvalidData)
            );
        }

        // wrong prefix
        let raw = mkgid(prefix::Segment::Value as u8, SystemTime::now(), random());
        assert_matches!(
            VolumeId::try_from(raw).unwrap_err(),
            GidParseErr::Corrupt(ZerocopyErr::InvalidData)
        );
    }
}
