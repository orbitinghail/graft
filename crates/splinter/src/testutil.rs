use std::fmt::Debug;

use bytes::Bytes;

use crate::{util::CopyToOwned, Splinter, SplinterRef};
use itertools::Itertools;

pub fn mksplinter(values: impl IntoIterator<Item = u32>) -> Splinter {
    let mut splinter = Splinter::default();
    for i in values {
        splinter.insert(i);
    }
    splinter
}

pub fn mksplinter_ref(values: impl IntoIterator<Item = u32>) -> SplinterRef<Bytes> {
    SplinterRef::from_bytes(mksplinter(values).serialize_to_bytes()).unwrap()
}

/// Create a pair of `Splinter` and `SplinterRef` from the same values.
pub fn mksplinters(values: impl IntoIterator<Item = u32> + Clone) -> [TestSplinter; 2] {
    let splinter = mksplinter(values.clone());
    let splinter_ref = mksplinter_ref(values);
    [
        TestSplinter::Splinter(splinter),
        TestSplinter::SplinterRef(splinter_ref),
    ]
}

pub fn check_combinations<L, R, E, F>(left: L, right: R, expected: E, test: F)
where
    L: IntoIterator<Item = u32> + Clone,
    R: IntoIterator<Item = u32> + Clone,
    E: IntoIterator<Item = u32> + Clone,
    F: Fn(TestSplinter, TestSplinter) -> Splinter,
{
    let left = mksplinters(left);
    let right = mksplinters(right);
    let expected = mksplinter(expected);
    for (lhs, rhs) in left.into_iter().cartesian_product(right) {
        let label = format!("lhs: {lhs:?}, rhs: {rhs:?}");
        let out = test(lhs, rhs);
        assert_eq!(out, expected, "{label}");
    }
}

#[derive(Clone)]
pub enum TestSplinter {
    Splinter(Splinter),
    SplinterRef(SplinterRef<Bytes>),
}

impl Debug for TestSplinter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Splinter(splinter) => {
                let prefix: Vec<_> = splinter.iter().take(10).collect();
                f.debug_struct("Splinter")
                    .field("meta", splinter)
                    .field("prefix", &prefix)
                    .finish()
            }
            Self::SplinterRef(splinter) => {
                let prefix: Vec<_> = splinter.copy_to_owned().iter().take(10).collect();
                f.debug_struct("SplinterRef")
                    .field("meta", splinter)
                    .field("prefix", &prefix)
                    .finish()
            }
        }
    }
}
