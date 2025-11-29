use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

use thiserror::Error;
use zerocopy::{Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

use crate::{
    LogId,
    cbe::CBE64,
    derive_zerocopy_encoding,
    lsn::LSN,
    page::Page,
    page_count::PageCount,
    pageidx::PageIdx,
    zerocopy_ext::{self, ZerocopyErr},
};

/// The size of a `CommitHash` in bytes.
const COMMIT_HASH_SIZE: usize = 32;

/// The size of the hash portion of the `CommitHash` in bytes.
const HASH_SIZE: usize = 31;

/// Magic number to initialize commit hash computation
const COMMIT_HASH_MAGIC: [u8; 4] = [0x68, 0xA4, 0x19, 0x30];

// The length of an encoded CommitHash in base58.
// To calculate this compute ceil(32 * (log2(256) / log2(58)))
//
// Note: we require that CommitHash's always are their maximum length
// This is currently guaranteed for well-constructed CommitHash's due to the
// CommitHashPrefix occupying the most significant byte.
const ENCODED_LEN: usize = 44;

#[derive(Debug, Error, PartialEq)]
pub enum CommitHashParseErr {
    #[error("invalid base58 encoding")]
    DecodeErr(#[from] bs58::decode::Error),

    #[error("invalid zerocopy encoding")]
    ZerocopyErr(#[from] zerocopy_ext::ZerocopyErr),

    #[error("invalid length")]
    InvalidLength,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    TryFromBytes,
    IntoBytes,
    Immutable,
    KnownLayout,
    Unaligned,
)]
#[repr(u8)]
pub enum CommitHashPrefix {
    #[default]
    Value = b'C',
}

#[derive(
    Clone, PartialEq, Eq, Default, TryFromBytes, IntoBytes, Immutable, KnownLayout, Unaligned,
)]
#[repr(C)]
pub struct CommitHash {
    prefix: CommitHashPrefix,
    hash: [u8; HASH_SIZE],
}

static_assertions::assert_eq_size!(CommitHash, [u8; COMMIT_HASH_SIZE]);

impl CommitHash {
    pub const ZERO: Self = Self {
        prefix: CommitHashPrefix::Value,
        hash: [0; HASH_SIZE],
    };

    #[cfg(any(test, feature = "testutil"))]
    pub fn testonly_random() -> Self {
        Self {
            prefix: CommitHashPrefix::Value,
            hash: rand::random(),
        }
    }

    /// Encodes the `CommitHash` to base58 and returns it as a string
    #[inline]
    pub fn pretty(&self) -> String {
        bs58::encode(self.as_bytes()).into_string()
    }
}

impl TryFrom<[u8; COMMIT_HASH_SIZE]> for CommitHash {
    type Error = CommitHashParseErr;

    #[inline]
    fn try_from(value: [u8; COMMIT_HASH_SIZE]) -> Result<Self, Self::Error> {
        Ok(zerocopy::try_transmute!(value).map_err(ZerocopyErr::from)?)
    }
}

impl From<CommitHash> for [u8; COMMIT_HASH_SIZE] {
    #[inline]
    fn from(value: CommitHash) -> Self {
        zerocopy::transmute!(value)
    }
}

impl FromStr for CommitHash {
    type Err = CommitHashParseErr;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        // verify the length
        if value.len() != ENCODED_LEN {
            return Err(CommitHashParseErr::InvalidLength);
        }

        // parse from base58
        let bytes: [u8; COMMIT_HASH_SIZE] = bs58::decode(value.as_bytes()).into_array_const()?;
        bytes.try_into()
    }
}

impl Debug for CommitHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CommitHash({})", self.pretty())
    }
}

impl Display for CommitHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.pretty())
    }
}

derive_zerocopy_encoding!(
    encode type (CommitHash)
    with size (COMMIT_HASH_SIZE)
    with empty (CommitHash::ZERO)
);

/// Builder for computing commit hashes using BLAKE3.
///
/// Implements the commit hash algorithm as specified in RFC 0001.
/// The hash incorporates the Log ID, LSN, page count, and page data
/// to ensure uniqueness and integrity verification.
pub struct CommitHashBuilder {
    hasher: blake3::Hasher,
    last_pageidx: Option<PageIdx>,
}

impl CommitHashBuilder {
    /// Creates a new `CommitHashBuilder` initialized with the given metadata.
    pub fn new(logid: LogId, lsn: LSN, pages: PageCount) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&COMMIT_HASH_MAGIC);
        hasher.update(logid.as_bytes());
        hasher.update(CBE64::from(lsn).as_bytes());
        hasher.update(&pages.to_u32().to_be_bytes());
        Self { hasher, last_pageidx: None }
    }

    /// Writes a page to the hash computation.
    ///
    /// # Panics
    /// This method will panic if pages are written out of order by pageidx
    pub fn write_page(&mut self, pageidx: PageIdx, page: &Page) {
        // Ensure pages are written in order
        if let Some(last_pageidx) = self.last_pageidx.replace(pageidx) {
            assert!(
                pageidx > last_pageidx,
                "Pages must be written in order by pageidx. Last: {last_pageidx}, Current: {pageidx}"
            );
        }

        self.hasher.update(&pageidx.to_u32().to_be_bytes());
        self.hasher.update(page.as_ref());
    }

    /// Finalizes the hash computation and returns the `CommitHash`.
    pub fn build(self) -> CommitHash {
        let hash = self.hasher.finalize();
        let mut bytes = *hash.as_bytes();
        bytes[0] = CommitHashPrefix::Value as u8;
        zerocopy::try_transmute!(bytes).expect("prefix byte manually set")
    }
}

#[cfg(test)]
mod tests {
    use std::panic;

    use super::*;
    use crate::{lsn, pageidx};
    use bilrost::{Message, OwnedMessage};

    #[graft_test::test]
    fn test_commit_hash_bilrost() {
        #[derive(Message, Debug, PartialEq, Eq)]
        struct TestMsg {
            hash: Option<CommitHash>,
        }

        let msg = TestMsg {
            hash: Some(CommitHash::testonly_random()),
        };
        let b = msg.encode_to_bytes();
        let decoded: TestMsg = TestMsg::decode(b).unwrap();
        assert_eq!(decoded, msg, "Decoded message does not match original");
    }

    #[graft_test::test]
    fn test_commit_hash_builder_table() {
        let log: LogId = "74ggbzxuMf-2uAmM7FwXntwW".parse().unwrap();

        struct TestCase {
            name: &'static str,
            log: LogId,
            lsn: LSN,
            page_count: PageCount,
            pages: Vec<(PageIdx, Page)>,
            expected_hash: &'static str,
        }

        let test_cases = vec![
            TestCase {
                name: "empty_log",
                log: log.clone(),
                lsn: lsn!(1),
                page_count: PageCount::ZERO,
                pages: vec![],
                expected_hash: "5ZCKZ9nz14E6kttXgRzGzWPe4iGad8fqE6bADSLxzfXV",
            },
            TestCase {
                name: "single_page",
                log: log.clone(),
                lsn: lsn!(42),
                page_count: PageCount::new(1),
                pages: vec![(pageidx!(1), Page::test_filled(0xAA))],
                expected_hash: "5Zx7fz5utSpLyJvurgLiQGHzdNHH4Wwk1BoxoyfR3C5j",
            },
            TestCase {
                name: "multiple_pages",
                log,
                lsn: lsn!(123),
                page_count: PageCount::new(2),
                pages: vec![
                    (pageidx!(1), Page::test_filled(0x11)),
                    (pageidx!(2), Page::test_filled(0x22)),
                ],
                expected_hash: "5Xsk16UBYSSQ75xbikQfTHykWpbVv3az1ncaFGajqjhe",
            },
        ];

        for test_case in test_cases {
            let mut builder =
                CommitHashBuilder::new(test_case.log, test_case.lsn, test_case.page_count);

            for (pageidx, page) in test_case.pages {
                builder.write_page(pageidx, &page);
            }

            let hash = builder.build();
            println!("hash for case {}: {}", test_case.name, hash.pretty());
            let expected_hash: CommitHash = test_case.expected_hash.parse().unwrap();

            assert_eq!(
                hash,
                expected_hash,
                "Hash mismatch for test case: {}. Expected: {}, Got: {}",
                test_case.name,
                test_case.expected_hash,
                hash.pretty()
            );
            assert_eq!(
                &hash.pretty(),
                test_case.expected_hash,
                "Pretty format mismatch for test case: {}. Expected: {}, Got: {}",
                test_case.name,
                test_case.expected_hash,
                hash.pretty()
            );
        }
    }

    #[graft_test::test]
    #[should_panic(expected = "Pages must be written in order by pageidx")]
    fn test_commit_hash_builder_page_order_panic() {
        let mut builder = CommitHashBuilder::new(LogId::random(), LSN::FIRST, PageCount::ZERO);
        builder.write_page(pageidx!(2), &Page::test_filled(0x22));
        builder.write_page(pageidx!(1), &Page::test_filled(0x11)); // This should panic
    }

    #[graft_test::test]
    #[test]
    fn test_commit_hash_from_str() {
        let hash: CommitHash = "5aNs8RN7tSRqfi66ubcPqSVqrWBGbaPU6C4mBVp6NYgo"
            .parse()
            .unwrap();
        let encoded = hash.pretty();
        let decoded: CommitHash = encoded.parse().unwrap();
        assert_eq!(hash, decoded);
    }

    #[graft_test::test]
    fn test_commit_hash_from_str_invalid() {
        // Test various invalid inputs
        let invalid_cases = vec![
            "",      // empty string
            "short", // too short
            "verylongstringthatiswaytoologtobeahashverylongstringthatiswaytoologtobeahashverylongstringthatiswaytoologtobeahash", // too long
            "invalid!@#$%^&*()characters", // invalid characters
            "5aNs8RN7tSRqfi66ubcPqSVqrWBGbaPU6C4mBVp6NYg", // wrong length (43 chars)
            "5aNs8RN7tSRqfi66ubcPqSVqrWBGbaPU6C4mBVp6NYgoY", // wrong length (45 chars)
            "4aNs8RN7tSRqfi66ubcPqSVqrWBGbaPU6C4mBVp6NYgo", // wrong prefix
        ];

        for case in invalid_cases {
            if let Ok(hash) = case.parse::<CommitHash>() {
                panic!(
                    "Expected error for case: `{}`, but parsed successfully: {}",
                    case,
                    hash.pretty()
                )
            }
        }
    }
}
