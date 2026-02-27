use crate::{bitmap::BitmapExt, ops::Cut, relational::Relation};

use super::{Splinter, SplinterRef};

impl Cut for Splinter {
    type Output = Splinter;

    fn cut(&mut self, rhs: &Self) -> Self::Output {
        let mut out = Splinter::default();

        let rhs = &rhs.partitions;
        self.partitions.retain(|&high, left| {
            if let Some(right) = rhs.get(high) {
                // we need to cut right out of left
                left.retain(|&mid, left| {
                    if let Some(right) = right.get(mid) {
                        out.insert_block(high, mid, left.cut(right));
                        left.has_bits_set()
                    } else {
                        true
                    }
                });
                !left.is_empty()
            } else {
                true
            }
        });

        out
    }
}

impl<T: AsRef<[u8]>> Cut<SplinterRef<T>> for Splinter {
    type Output = Splinter;

    fn cut(&mut self, rhs: &SplinterRef<T>) -> Self::Output {
        let mut out = Splinter::default();

        let rhs = rhs.load_partitions();
        self.partitions.retain(|&high, left| {
            if let Some(right) = rhs.get(high) {
                // we need to cut right out of left
                left.retain(|&mid, left| {
                    if let Some(right) = right.get(mid) {
                        out.insert_block(high, mid, left.cut(&right));
                        left.has_bits_set()
                    } else {
                        true
                    }
                });
                !left.is_empty()
            } else {
                true
            }
        });

        out
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ops::Cut,
        testutil::{mksplinter, mksplinters, TestSplinter},
        Splinter,
    };

    impl Cut<TestSplinter> for Splinter {
        type Output = Splinter;

        fn cut(&mut self, rhs: &TestSplinter) -> Self::Output {
            use TestSplinter::*;
            match rhs {
                Splinter(rhs) => self.cut(rhs),
                SplinterRef(rhs) => self.cut(rhs),
            }
        }
    }

    fn check_cut<L, R, E, O>(left: L, right: R, expected_cut: E, expected_out: O)
    where
        L: IntoIterator<Item = u32> + Clone,
        R: IntoIterator<Item = u32> + Clone,
        E: IntoIterator<Item = u32> + Clone,
        O: IntoIterator<Item = u32> + Clone,
    {
        let left = mksplinter(left);
        let right = mksplinters(right);
        let expected_cut = mksplinter(expected_cut);
        let expected_out = mksplinter(expected_out);
        for rhs in right.into_iter() {
            let mut left = left.clone();
            let label = format!("lhs: {:?}, rhs: {:?}", left, rhs);
            let out = left.cut(&rhs);
            assert_eq!(left, expected_cut, "cut: {label}");
            assert_eq!(out, expected_out, "intersection: {label}");
        }
    }

    #[test]
    fn test_sanity() {
        check_cut(0..0, 0..0, 0..0, 0..0);
        check_cut(0..10, 0..5, 5..10, 0..5);
        check_cut(0..10, 0..10, 0..0, 0..10);
    }
}
