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
    ByteEq, ByteHash, ConvertError, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned,
    ValidityError,
};

use crate::core::{
    byte_unit::ByteUnit,
    gid::{prefix::ConstDefault, random::GidRandom},
    zerocopy_ext::{TryFromBytesExt, ZerocopyErr},
};
use crate::derive_zerocopy_encoding;

/// Size of a GID in bytes
const GID_SIZE: ByteUnit = ByteUnit::new(16);
const GID_TIME_SIZE: ByteUnit = ByteUnit::new(7);
const GID_RANDOM_SIZE: ByteUnit = ByteUnit::new(9);

static_assertions::const_assert_eq!(
    GID_SIZE.as_usize(),
    GID_TIME_SIZE.as_usize() + GID_RANDOM_SIZE.as_usize()
);

/// We serialize GIDs to the format `TIME-RANDOM` where
/// TIME: is the Base58 encoding of `time`
/// RANDOM: is the Base58 encoding of `random`
///
/// Base58 encoding is fixed length assuming the input bytes do not have leading
/// zeroes. GIDs guarantee that TIME and RANDOM do not start with a zero byte by
/// injecting a prefix into both.
///
/// To calculate the size of a Base58 string (assuming no leading zeroes) use:
///     ceil(N * 8 / log2(58))
/// where N is the number of bytes
const ENCODED_TIME_LEN: usize = 10;
const ENCODED_RANDOM_LEN: usize = 13;
const ENCODED_LEN: usize = ENCODED_TIME_LEN + 1 + ENCODED_RANDOM_LEN;

mod prefix;
mod random;
mod time;

#[derive(Clone, ByteEq, ByteHash, IntoBytes, TryFromBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
pub struct Gid<P: Prefix> {
    time: GidTimestamp<P>,
    random: GidRandom,
}

pub type VolumeId = Gid<prefix::Volume>;
pub type LogId = Gid<prefix::Log>;
pub type SegmentId = Gid<prefix::Segment>;

static_assertions::assert_eq_size!(VolumeId, [u8; GID_SIZE.as_usize()]);
static_assertions::assert_eq_size!(LogId, [u8; GID_SIZE.as_usize()]);
static_assertions::assert_eq_size!(SegmentId, [u8; GID_SIZE.as_usize()]);

impl<P: Prefix> Gid<P> {
    pub const SIZE: ByteUnit = GID_SIZE;
    pub const EMPTY: Self = Self {
        time: GidTimestamp::DEFAULT,
        random: GidRandom::DEFAULT,
    };

    pub fn random() -> Self {
        Self {
            time: GidTimestamp::now(),
            random: GidRandom::random(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self == &Self::EMPTY
    }

    /// serializes the Gid to a string
    /// This representation round-trips via `FromStr`.
    /// This representation sorts alphanumerically in ascending time order (ms
    /// granularity).
    pub fn serialize(&self) -> String {
        let mut out = [0u8; ENCODED_LEN];

        // encode time
        let mut n = bs58::encode(self.time.as_bytes())
            .onto(&mut out[..ENCODED_TIME_LEN])
            .expect("BUG: Gid encode buf size");
        assert_eq!(n, ENCODED_TIME_LEN);

        // encode separator
        out[n] = b'-';
        n += 1;

        // encode random
        let m = bs58::encode(self.random.as_bytes())
            .onto(&mut out[n..])
            .expect("BUG: Gid encode buf size");
        assert_eq!(m, ENCODED_RANDOM_LEN);

        // convert to string
        str::from_utf8(&out)
            .expect("BUG: bs58 non-utf8 bytes")
            .to_owned()
    }

    /// serializes the random portion of the Gid to a string
    /// This representation does *not* round trip
    pub fn short(&self) -> String {
        bs58::encode(self.random.as_bytes()).into_string()
    }

    #[inline]
    pub fn as_time(&self) -> SystemTime {
        self.time.as_time()
    }

    pub fn copy_to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(self.as_bytes())
    }
}

impl<P: Prefix> Ord for Gid<P> {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl<P: Prefix> PartialOrd for Gid<P> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<P: Prefix> Default for Gid<P> {
    #[inline]
    fn default() -> Self {
        Self::EMPTY
    }
}

impl<P: Prefix> Display for Gid<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.serialize())
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

    #[error("invalid layout")]
    InvalidGidLayout,

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
        // verify the length
        if value.len() != ENCODED_LEN {
            return Err(GidParseErr::InvalidLength);
        }

        // split at '-'
        let Some((time, random)) = value.split_once("-") else {
            return Err(GidParseErr::InvalidGidLayout);
        };

        // check component lengths
        if time.len() != ENCODED_TIME_LEN || random.len() != ENCODED_RANDOM_LEN {
            return Err(GidParseErr::InvalidGidLayout);
        }

        // parse from base58
        let time: [u8; GID_TIME_SIZE.as_usize()] =
            bs58::decode(time.as_bytes()).into_array_const()?;
        let random: [u8; GID_RANDOM_SIZE.as_usize()] =
            bs58::decode(random.as_bytes()).into_array_const()?;

        Ok(Self {
            time: GidTimestamp::try_read_from_bytes(&time)?,
            random: GidRandom::try_read_from_bytes(&random)?,
        })
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

impl<P: Prefix> TryFrom<&[u8]> for Gid<P> {
    type Error = GidParseErr;

    #[inline]
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != GID_SIZE.as_usize() {
            return Err(GidParseErr::InvalidLength);
        }

        Ok(Self::try_read_from_bytes(value)?)
    }
}

impl<P: Prefix> TryFrom<Bytes> for Gid<P> {
    type Error = GidParseErr;

    #[inline]
    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        Self::try_from(value.as_ref())
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
            serializer.serialize_str(&self.serialize())
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
            let s = <&str>::deserialize(deserializer)?;
            s.parse().map_err(serde::de::Error::custom)
        } else {
            let bytes = <&[u8]>::deserialize(deserializer)?;
            bytes.try_into().map_err(serde::de::Error::custom)
        }
    }
}

derive_zerocopy_encoding!(
    encode type (Gid<P>)
    with size (GID_SIZE.as_usize())
    with empty (Gid::<P>::EMPTY)
    with generics (P: Prefix)
);

#[cfg(test)]
mod tests {

    use std::time::UNIX_EPOCH;

    use assert_matches::assert_matches;
    use bilrost::{Message, OwnedMessage};
    use rand::random;

    use super::*;

    fn mkgid(prefix: u8, ts: SystemTime, random: u8) -> [u8; 16] {
        let mut bytes = [random; 16];
        bytes[..7].copy_from_slice(GidTimestamp::<prefix::Log>::from(ts).as_bytes());
        bytes[0] = prefix;
        bytes[7] |= 0x80; // set the first bit in random to 1
        bytes
    }

    #[graft_test::test]
    fn test_mkgid() {
        let gid = mkgid(prefix::Volume::Value as u8, SystemTime::UNIX_EPOCH, 0);
        let gid = VolumeId::try_read_from_bytes(&gid).unwrap();
        assert_eq!(gid.as_time(), SystemTime::UNIX_EPOCH);
        assert_eq!(gid.random, GidRandom::DEFAULT);

        let gid = mkgid(prefix::Log::Value as u8, SystemTime::UNIX_EPOCH, 0);
        let gid = LogId::try_read_from_bytes(&gid).unwrap();
        assert_eq!(gid.as_time(), SystemTime::UNIX_EPOCH);
        assert_eq!(gid.random, GidRandom::DEFAULT);

        let gid = mkgid(prefix::Segment::Value as u8, SystemTime::UNIX_EPOCH, 0);
        let gid = SegmentId::try_read_from_bytes(&gid).unwrap();
        assert_eq!(gid.as_time(), SystemTime::UNIX_EPOCH);
        assert_eq!(gid.random, GidRandom::DEFAULT);
    }

    #[graft_test::test]
    fn test_serialize_short() {
        // short is always substr of serialize
        for _ in 0..100 {
            let id = SegmentId::random();
            let serialized = id.serialize();
            let short = id.short();
            println!("{serialized} {short}");
            assert!(
                serialized.ends_with(&format!("-{short}")),
                "pretty: {serialized}, short: {short}"
            );
        }
    }

    #[graft_test::test]
    fn test_ts() {
        let vid = LogId::random();
        let ts = vid.as_time();
        assert!(ts.duration_since(UNIX_EPOCH).unwrap().as_millis() > 0)
    }

    #[graft_test::test]
    fn test_size() {
        let g = SegmentId::default();
        println!("gid: {}", g.serialize());
        assert!(g.serialize().len() == ENCODED_LEN);

        let g = LogId::default();
        println!("gid: {}", g.serialize());
        assert!(g.serialize().len() == ENCODED_LEN);

        let g = VolumeId::default();
        println!("gid: {}", g.serialize());
        assert!(g.serialize().len() == ENCODED_LEN);
    }

    #[graft_test::test]
    fn test_parse_round_trip() {
        let id = SegmentId::random();

        // round trip through string
        let pretty = id.serialize();
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
            let result: Result<LogId, _> = case.parse();
            assert_matches!(result.unwrap_err(), GidParseErr::InvalidLength);
        }

        // no dash or invalid component lengths
        let cases = [
            "5rMJhdWYUu2eBDLsydsjKPAT",
            "5rMJhdWYe92deCanWXyGaHdm",
            "5rMJhdWvcU2-dZ9EGhu1vV8X",
            "5rMJhdWzu92doCGF-QwpamqX",
        ];
        for &case in cases.iter() {
            let result: Result<LogId, _> = case.parse();
            assert_matches!(result.unwrap_err(), GidParseErr::InvalidGidLayout);
        }

        // invalid prefix
        let cases = ["2rMJhdWzu9-2doCGFQwpamqX", "5rMJhdWzu9-3doCGFQwpamqX"];
        for &case in cases.iter() {
            let result: Result<LogId, _> = case.parse();
            assert_matches!(result.unwrap_err(), GidParseErr::Corrupt(_));
        }

        // invalid prefix from binary repr
        let cases = [
            mkgid(5, SystemTime::now(), 0),
            mkgid(5, SystemTime::now(), random()),
        ];
        for &case in cases.iter() {
            let result: Result<LogId, _> = case.try_into();
            assert_matches!(
                result.unwrap_err(),
                GidParseErr::Corrupt(ZerocopyErr::InvalidData)
            );
        }

        // wrong prefix
        let raw = mkgid(prefix::Segment::Value as u8, SystemTime::now(), random());
        assert_matches!(
            LogId::try_from(raw).unwrap_err(),
            GidParseErr::Corrupt(ZerocopyErr::InvalidData)
        );
    }

    #[graft_test::test]
    fn test_bilrost() {
        #[derive(Message, Debug, PartialEq, Eq)]
        struct TestMsg {
            vid: LogId,
            sid: SegmentId,

            vids: Vec<LogId>,
        }

        let msg = TestMsg {
            vid: LogId::random(),
            sid: SegmentId::random(),

            vids: vec![LogId::random(), LogId::random()],
        };
        let b = msg.encode_to_bytes();
        let decoded: TestMsg = TestMsg::decode(b).unwrap();
        assert_eq!(decoded, msg, "Decoded message does not match original");
    }

    #[graft_test::test]
    fn test_gid_alphanumeric_sort() {
        // generate 10 gids separated by random times from 1ms to 1000ms
        // then verify that they sort in order
        let mut gids: Vec<LogId> = Vec::new();
        let mut current_time = SystemTime::UNIX_EPOCH;
        for _ in 0..10 {
            let delta = std::time::Duration::from_millis(rand::random::<u64>() % 1000 + 1);
            current_time += delta;
            let gid = LogId {
                time: GidTimestamp::from(current_time),
                random: GidRandom::random(),
            };
            gids.push(gid);
        }
        // clone gids and sort by alphanumeric order of their string representation
        let mut sorted_gids = gids.clone();
        sorted_gids.sort_by_key(|gid| gid.serialize());
        // verify that the sorted gids match the original gids
        assert_eq!(gids, sorted_gids);

        // clone gids and sort them natively
        let mut sorted_gids = gids.clone();
        sorted_gids.sort();
        // verify that the sorted gids match the original gids
        assert_eq!(gids, sorted_gids);
    }
}
