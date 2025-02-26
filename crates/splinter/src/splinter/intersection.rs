use crate::{ops::Intersection, relational::Relation};

use super::{Splinter, SplinterRef};

// Splinter <> Splinter
impl Intersection for Splinter {
    type Output = Splinter;

    fn intersection(&self, rhs: &Self) -> Self::Output {
        let mut out = Splinter::default();
        for (a, left, right) in self.partitions.inner_join(&rhs.partitions) {
            for (b, left, right) in left.inner_join(&right) {
                for (c, left, right) in left.inner_join(&right) {
                    out.insert_block(a, b, c, left.intersection(right));
                }
            }
        }
        out
    }
}

// Splinter <> SplinterRef
impl<T: AsRef<[u8]>> Intersection<SplinterRef<T>> for Splinter {
    type Output = Splinter;

    fn intersection(&self, rhs: &SplinterRef<T>) -> Self::Output {
        let mut out = Splinter::default();
        let rhs = rhs.load_partitions();
        for (a, left, right) in self.partitions.inner_join(&rhs) {
            for (b, left, right) in left.inner_join(&right) {
                for (c, left, right) in left.inner_join(&right) {
                    out.insert_block(a, b, c, left.intersection(&right));
                }
            }
        }
        out
    }
}

// SplinterRef <> Splinter
impl<T: AsRef<[u8]>> Intersection<Splinter> for SplinterRef<T> {
    type Output = Splinter;

    fn intersection(&self, rhs: &Splinter) -> Self::Output {
        rhs.intersection(self)
    }
}

// SplinterRef <> SplinterRef
impl<T1: AsRef<[u8]>, T2: AsRef<[u8]>> Intersection<SplinterRef<T2>> for SplinterRef<T1> {
    type Output = Splinter;

    fn intersection(&self, rhs: &SplinterRef<T2>) -> Self::Output {
        let mut out = Splinter::default();
        let rhs = rhs.load_partitions();
        for (a, left, right) in self.load_partitions().inner_join(&rhs) {
            for (b, left, right) in left.inner_join(&right) {
                for (c, left, right) in left.inner_join(&right) {
                    out.insert_block(a, b, c, left.intersection(&right));
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Splinter,
        ops::Intersection,
        testutil::{TestSplinter, check_combinations},
    };

    impl Intersection for TestSplinter {
        type Output = Splinter;

        fn intersection(&self, rhs: &Self) -> Self::Output {
            use TestSplinter::*;
            match (self, rhs) {
                (Splinter(lhs), Splinter(rhs)) => lhs.intersection(rhs),
                (Splinter(lhs), SplinterRef(rhs)) => lhs.intersection(rhs),
                (SplinterRef(lhs), Splinter(rhs)) => lhs.intersection(rhs),
                (SplinterRef(lhs), SplinterRef(rhs)) => lhs.intersection(rhs),
            }
        }
    }

    #[test]
    fn test_sanity() {
        check_combinations(0..0, 0..0, 0..0, |lhs, rhs| lhs.intersection(&rhs));
        check_combinations(0..100, 30..150, 30..100, |lhs, rhs| lhs.intersection(&rhs));

        // 8 sparse blocks
        let set = (0..=1024).step_by(128).collect::<Vec<_>>();
        check_combinations(set.clone(), vec![0, 128], vec![0, 128], |lhs, rhs| {
            lhs.intersection(&rhs)
        });
    }
}
