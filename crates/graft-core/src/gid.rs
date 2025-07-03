use std::{
    fmt::{Debug, Display},
    str::FromStr,
    time::SystemTime,
};

use bytes::Bytes;
use prefix::Prefix;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::GidTimestamp;
use zerocopy::{
    ByteHash, ConvertError, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned,
    ValidityError,
};

use crate::{
    byte_unit::ByteUnit,
    derive_zerocopy_encoding,
    zerocopy_ext::{TryFromBytesExt, ZerocopyErr},
};

const GID_SIZE: ByteUnit = ByteUnit::new(16);
const SHORT_LEN: usize = 12;

mod prefix;
mod time;

#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    ByteHash,
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
    pub const EMPTY: Self = Self {
        prefix: P::DEFAULT,
        ts: GidTimestamp::ZERO,
        random: [0; 9],
    };

    #[inline]
    pub fn is_empty(&self) -> bool {
        self == &Self::EMPTY
    }

    #[inline]
    pub fn random() -> Self {
        Self {
            prefix: P::DEFAULT,
            ts: GidTimestamp::now(),
            random: rand::random(),
        }
    }

    /// encodes the Gid to bs58 and returns it as a string
    pub fn pretty(&self) -> String {
        bs58::encode(self.as_bytes()).into_string()
    }

    /// returns the `SHORT_LEN` suffix of self.pretty
    pub fn short(&self) -> String {
        let pretty = self.pretty();
        pretty[pretty.len() - SHORT_LEN..].to_owned()
    }

    #[inline]
    pub fn as_time(&self) -> SystemTime {
        self.ts.as_time()
    }

    pub fn copy_to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

impl<P: Prefix> Default for Gid<P> {
    #[inline]
    fn default() -> Self {
        Self::EMPTY
    }
}

impl ClientId {
    /// derive a `ClientId` from source bytes deterministically
    pub fn derive(source: &[u8]) -> ClientId {
        let hash = blake3::hash(source);
        let mut random = [0; 9];
        random.copy_from_slice(&hash.as_bytes()[..9]);
        ClientId {
            prefix: Default::default(),
            ts: GidTimestamp::ZERO,
            random,
        }
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

impl<S, D: ?Sized + TryFromBytes> From<ValidityError<S, D>> for GidParseErr {
    #[inline]
    fn from(value: ValidityError<S, D>) -> Self {
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

        Ok(Gid::<P>::try_ref_from_unaligned_bytes(value)?)
    }
}

impl<P: Prefix> TryFrom<[u8; GID_SIZE.as_usize()]> for Gid<P> {
    type Error = GidParseErr;

    #[inline]
    fn try_from(value: [u8; GID_SIZE.as_usize()]) -> Result<Self, Self::Error> {
        Ok(Self::try_read_from_bytes(&value)?)
    }
}

impl<P: Prefix> Serialize for Gid<P> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.pretty())
        } else {
            serializer.serialize_bytes(self.as_bytes())
        }
    }
}

impl<'de, P: Prefix> Deserialize<'de> for Gid<P> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            s.parse().map_err(serde::de::Error::custom)
        } else {
            let bytes = <[u8; GID_SIZE.as_usize()]>::deserialize(deserializer)?;
            bytes.try_into().map_err(serde::de::Error::custom)
        }
    }
}

derive_zerocopy_encoding!(
    encode borrowed type (Gid<P>)
    with size (GID_SIZE.as_usize())
    with empty (Gid::<P>::EMPTY)
    with generics (P: Prefix)
);

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use assert_matches::assert_matches;
    use bilrost::{BorrowedMessage, Message, OwnedMessage};
    use rand::random;

    use crate::codec::zerocopy_encoding::CowEncoding;

    use super::*;

    fn mkgid(prefix: u8, ts: SystemTime, random: u8) -> [u8; 16] {
        let mut bytes = [random; 16];
        bytes[0] = prefix;
        bytes[1..7].copy_from_slice(GidTimestamp::from(ts).as_bytes());
        bytes
    }

    #[graft_test::test]
    fn test_pretty_short() {
        // short is always substr of pretty
        for _ in 0..100 {
            let id = SegmentId::random();
            let pretty = id.pretty();
            let short = id.short();
            println!("{pretty} {short}");
            assert!(pretty.contains(&short), "pretty: {pretty}, short: {short}");
        }
    }

    #[graft_test::test]
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

    #[graft_test::test]
    fn test_parse_round_trip() {
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

    #[graft_test::test]
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
        let cases = ["GontbnaXtaE3!BbafyDiJt", "zzzzzzzzzzzzzzzzzzzzzz"];
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

    #[graft_test::test]
    fn test_bilrost() {
        #[derive(Message, Debug, PartialEq, Eq)]
        struct TestMsg {
            vid: VolumeId,
            sid: SegmentId,
            cid: ClientId,

            vids: Vec<VolumeId>,
        }

        let msg = TestMsg {
            vid: VolumeId::random(),
            sid: SegmentId::random(),
            cid: ClientId::random(),

            vids: vec![VolumeId::random(), VolumeId::random()],
        };
        let b = msg.encode_to_bytes();
        let decoded: TestMsg = TestMsg::decode(b).unwrap();
        assert_eq!(decoded, msg, "Decoded message does not match original");
    }

    #[graft_test::test]
    fn test_bilrost_borrowed() {
        #[derive(Message, Debug, PartialEq, Eq)]
        struct TestMsg<'a> {
            #[bilrost(encoding(CowEncoding))]
            vid: Cow<'a, VolumeId>,
            #[bilrost(encoding(CowEncoding))]
            sid: Cow<'a, SegmentId>,
            #[bilrost(encoding(CowEncoding))]
            cid: Cow<'a, ClientId>,
        }
        let msg = TestMsg {
            vid: Cow::Owned(VolumeId::random()),
            sid: Cow::Owned(SegmentId::random()),
            cid: Cow::Owned(ClientId::random()),
        };
        let b = msg.encode_to_vec();
        let decoded = TestMsg::decode_borrowed(b.as_slice()).unwrap();
        assert_eq!(decoded, msg, "Decoded message does not match original");
        assert_matches!(decoded.vid, Cow::Borrowed(_));
        assert_matches!(decoded.sid, Cow::Borrowed(_));
        assert_matches!(decoded.cid, Cow::Borrowed(_));
    }
}
