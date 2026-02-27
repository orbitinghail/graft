use crate::ops::Merge;

use super::{Splinter, SplinterRef};

// Splinter <> Splinter
impl Merge for Splinter {
    fn merge(&mut self, rhs: &Self) {
        self.partitions.merge(&rhs.partitions);
    }
}

// Splinter <> SplinterRef
impl<T: AsRef<[u8]>> Merge<SplinterRef<T>> for Splinter {
    fn merge(&mut self, rhs: &SplinterRef<T>) {
        self.partitions.merge(&rhs.load_partitions());
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ops::Merge,
        testutil::{mksplinter, mksplinters, TestSplinter},
        Splinter,
    };

    impl Merge<TestSplinter> for Splinter {
        fn merge(&mut self, rhs: &TestSplinter) {
            use TestSplinter::*;
            match rhs {
                Splinter(rhs) => self.merge(rhs),
                SplinterRef(rhs) => self.merge(rhs),
            }
        }
    }

    fn check_merge<L, R, E>(left: L, right: R, expected: E)
    where
        L: IntoIterator<Item = u32> + Clone,
        R: IntoIterator<Item = u32> + Clone,
        E: IntoIterator<Item = u32> + Clone,
    {
        let left = mksplinter(left);
        let right = mksplinters(right);
        let expected = mksplinter(expected);
        for rhs in right.into_iter() {
            let mut left = left.clone();
            let label = format!("lhs: {:?}, rhs: {:?}", left, rhs);
            left.merge(&rhs);
            assert_eq!(left, expected, "merge: {label}");
        }
    }

    #[test]
    fn test_sanity() {
        check_merge(0..0, 0..0, 0..0);
        check_merge(0..5, 3..10, 0..10);
        check_merge(0..5, 0..0, 0..5);
        check_merge(0..0, 0..5, 0..5);
        check_merge(0..1, 65535..65536, vec![0, 65535]);
    }
}
