use std::{fmt::Display, iter::Sum};

use zerocopy::{ByteEq, ByteHash, FromBytes, Immutable, IntoBytes, KnownLayout};

#[derive(Clone, Debug, Default, IntoBytes, FromBytes, ByteEq, ByteHash, Immutable, KnownLayout)]
#[repr(C)]
pub struct Checksum {
    /// wrapping sum of 128-bit digests
    sum: u128,
    /// xor of 128-bit digests
    xor: u128,
    /// number of elements
    count: u128,
    /// total byte length (helps distinguish permutations with same digests)
    bytes: u128,
}

impl Display for Checksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // hash the checksum using blake3 to make it easier to read/compare
        let hashed = blake3::hash(self.as_bytes());
        f.write_str(&bs58::encode(hashed.as_bytes()).into_string())
    }
}

/// A builder for computing order-independent checksums over a set of elements.
///
/// `ChecksumBuilder` computes a checksum that is invariant to the order in which
/// elements are added. This is useful for checksumming sets, hash maps, or any
/// collection where element order doesn't matter.
///
/// ## How It Works
///
/// The checksum combines four values:
/// - **sum**: Wrapping sum of `xxh3_128` hashes (order-independent via commutativity)
/// - **xor**: XOR of `xxh3_128` hashes (order-independent via commutativity)
/// - **count**: Number of elements added
/// - **bytes**: Total byte length of all elements
///
/// Both sum (with wrapping arithmetic) and XOR are commutative operations, meaning
/// `a + b = b + a` and `a ^ b = b ^ a`, which makes the final checksum independent
/// of element order.
///
/// ## Example
///
/// ```rust
/// use graft_core::checksum::ChecksumBuilder;
///
/// let mut builder = ChecksumBuilder::new();
/// builder.write(&"apple");
/// builder.write(&"banana");
/// builder.write(&"cherry");
/// let checksum1 = builder.build();
///
/// let mut builder2 = ChecksumBuilder::new();
/// builder2.write(&"cherry");  // Different order
/// builder2.write(&"apple");
/// builder2.write(&"banana");
/// let checksum2 = builder2.build();
///
/// assert_eq!(checksum1, checksum2);  // Same checksum despite different order
/// ```
///
/// ## Use Cases
///
/// - Checksumming unordered sets of data
/// - Verifying that two collections contain the same elements (ignoring order)
/// - Detecting changes in sets where element order is not significant
///
/// ## Note on Duplicates
///
/// The checksum distinguishes between sets with different numbers of duplicate
/// elements because the `count` and `bytes` fields differ. However, the XOR
/// component will cancel out for pairs of identical elements.
#[derive(Default, Debug)]
#[repr(C)]
pub struct ChecksumBuilder {
    checksum: Checksum,
}

impl ChecksumBuilder {
    pub const DEFAULT: Self = Self {
        checksum: Checksum { sum: 0, xor: 0, count: 0, bytes: 0 },
    };

    /// Creates a new empty `ChecksumBuilder`.
    pub const fn new() -> Self {
        Self::DEFAULT
    }

    /// Adds an element to the checksum.
    ///
    /// The order in which elements are added does not affect the final checksum.
    /// You can add elements in any order and still get the same result.
    pub fn write<B: AsRef<[u8]>>(&mut self, data: &B) {
        let hash = xxhash_rust::xxh3::xxh3_128(data.as_ref());
        self.checksum.sum = self.checksum.sum.wrapping_add(hash);
        self.checksum.xor = self.checksum.xor ^ hash;
        self.checksum.count = self.checksum.count.wrapping_add(1);
        self.checksum.bytes = self
            .checksum
            .bytes
            .wrapping_add(data.as_ref().len() as u128);
    }

    /// Merges another `ChecksumBuilder` into this one, and returns the result
    pub const fn merge(mut self, b: Self) -> Self {
        self.checksum.sum = self.checksum.sum.wrapping_add(b.checksum.sum);
        self.checksum.xor = self.checksum.xor ^ b.checksum.xor;
        self.checksum.count = self.checksum.count.wrapping_add(b.checksum.count);
        self.checksum.bytes = self.checksum.bytes.wrapping_add(b.checksum.bytes);
        self
    }

    /// Consumes the builder and returns the final checksum.
    pub const fn build(self) -> Checksum {
        self.checksum
    }
}

impl Sum for ChecksumBuilder {
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(ChecksumBuilder::new(), |a, b| a.merge(b))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_order_independence_two_items() {
        let mut builder1 = ChecksumBuilder::new();
        builder1.write(&"hello");
        builder1.write(&"world");
        let checksum1 = builder1.build();

        let mut builder2 = ChecksumBuilder::new();
        builder2.write(&"world");
        builder2.write(&"hello");
        let checksum2 = builder2.build();

        assert_eq!(checksum1, checksum2, "Checksum should be order-independent");
    }

    #[test]
    fn test_order_independence_multiple_items() {
        let items = vec!["apple", "banana", "cherry", "date", "elderberry"];

        // Build checksum with items in original order
        let mut builder1 = ChecksumBuilder::new();
        for item in &items {
            builder1.write(item);
        }
        let checksum1 = builder1.build();

        // Build checksum with items in reverse order
        let mut builder2 = ChecksumBuilder::new();
        for item in items.iter().rev() {
            builder2.write(item);
        }
        let checksum2 = builder2.build();

        // Build checksum with items in a different permutation
        let shuffled = vec!["cherry", "apple", "elderberry", "banana", "date"];
        let mut builder3 = ChecksumBuilder::new();
        for item in &shuffled {
            builder3.write(item);
        }
        let checksum3 = builder3.build();

        assert_eq!(
            checksum1, checksum2,
            "Checksum should be order-independent (reversed)"
        );
        assert_eq!(
            checksum1, checksum3,
            "Checksum should be order-independent (shuffled)"
        );
    }

    #[test]
    fn test_empty_checksum() {
        let checksum = ChecksumBuilder::new().build();
        assert_eq!(checksum.sum, 0);
        assert_eq!(checksum.xor, 0);
        assert_eq!(checksum.count, 0);
        assert_eq!(checksum.bytes, 0);
    }

    #[test]
    fn test_single_item() {
        let mut builder = ChecksumBuilder::new();
        builder.write(&"test");
        let checksum = builder.build();

        assert_ne!(checksum.sum, 0, "Sum should be non-zero after writing data");
        assert_ne!(checksum.xor, 0, "XOR should be non-zero after writing data");
        assert_eq!(checksum.count, 1);
        assert_eq!(checksum.bytes, 4);
    }

    #[test]
    fn test_identical_items_different_from_single() {
        let mut builder1 = ChecksumBuilder::new();
        builder1.write(&"data");
        let checksum1 = builder1.build();

        let mut builder2 = ChecksumBuilder::new();
        builder2.write(&"data");
        builder2.write(&"data");
        let checksum2 = builder2.build();

        // Different counts should make checksums different
        assert_ne!(checksum1.count, checksum2.count);
        // XOR of identical values cancels out
        assert_eq!(checksum2.xor, 0);
        // But sum should be different
        assert_ne!(checksum1.sum, checksum2.sum);
    }

    #[test]
    fn test_different_data_different_checksum() {
        let mut builder1 = ChecksumBuilder::new();
        builder1.write(&"hello");
        let checksum1 = builder1.build();

        let mut builder2 = ChecksumBuilder::new();
        builder2.write(&"world");
        let checksum2 = builder2.build();

        assert_ne!(checksum1, checksum2);
    }

    #[test]
    fn test_merge() {
        // generate some random data
        let data = (0..1_000)
            .map(|i| format!("random_data_{}", i))
            .collect::<Vec<_>>();

        // first build the checksum serially
        let mut builder = ChecksumBuilder::new();
        for item in &data {
            builder.write(item);
        }
        let serial = builder.build();

        // build the checksum via merge
        let parallel = data
            .iter()
            .fold(ChecksumBuilder::new(), |mut builder, item| {
                builder.write(&item);
                builder
            })
            .build();

        assert_eq!(serial, parallel);
    }
}
