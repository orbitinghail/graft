use crate::{
    ops::{Merge, Union},
    util::CopyToOwned,
};

use super::{Splinter, SplinterRef};

// Splinter <> Splinter
impl Union for Splinter {
    type Output = Splinter;

    fn union(&self, rhs: &Self) -> Self::Output {
        let mut out = self.clone();
        out.merge(rhs);
        out
    }
}

// Splinter <> SplinterRef
impl<T: AsRef<[u8]>> Union<SplinterRef<T>> for Splinter {
    type Output = Splinter;

    fn union(&self, rhs: &SplinterRef<T>) -> Self::Output {
        let mut out = self.clone();
        out.merge(rhs);
        out
    }
}

// SplinterRef <> Splinter
impl<T: AsRef<[u8]>> Union<Splinter> for SplinterRef<T> {
    type Output = Splinter;

    fn union(&self, rhs: &Splinter) -> Self::Output {
        rhs.union(self)
    }
}

// SplinterRef <> SplinterRef
impl<T1, T2> Union<SplinterRef<T2>> for SplinterRef<T1>
where
    T1: AsRef<[u8]>,
    T2: AsRef<[u8]>,
{
    type Output = Splinter;

    fn union(&self, rhs: &SplinterRef<T2>) -> Self::Output {
        let mut out = self.copy_to_owned();
        out.merge(rhs);
        out
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ops::Union,
        testutil::{check_combinations, TestSplinter},
        Splinter,
    };

    impl Union for TestSplinter {
        type Output = Splinter;

        fn union(&self, rhs: &Self) -> Self::Output {
            use TestSplinter::*;
            match (self, rhs) {
                (Splinter(lhs), Splinter(rhs)) => lhs.union(rhs),
                (Splinter(lhs), SplinterRef(rhs)) => lhs.union(rhs),
                (SplinterRef(lhs), Splinter(rhs)) => lhs.union(rhs),
                (SplinterRef(lhs), SplinterRef(rhs)) => lhs.union(rhs),
            }
        }
    }

    #[test]
    fn test_sanity() {
        check_combinations(0..0, 0..0, 0..0, |lhs, rhs| lhs.union(&rhs));
        check_combinations(0..100, 30..150, 0..150, |lhs, rhs| lhs.union(&rhs));
    }
}
