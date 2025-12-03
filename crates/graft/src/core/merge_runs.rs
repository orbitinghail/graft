use std::{iter::FusedIterator, ops::RangeInclusive};

use range_set_blaze::{Integer, SortedDisjoint, SortedStarts};

/// `MergeRuns` produces a sequence of `RangeInclusive` runs from a *sorted &
/// deduped* iterator of values, by merging sequential items into
/// `RangeInclusive` values.
///
/// It's optimized to be used as a `RangeSetBlaze::SortedDisjoint` iterator,
/// allowing set operations to be run in linear time over the runs. Thus the
/// value type must implement `Integer`.
///
/// # Panics
/// This iterator will panic upon encountering unsorted values.
#[must_use]
pub struct MergeRuns<I, T> {
    inner: I,
    run: Option<(T, T)>,
}

impl<I, T> MergeRuns<I, T>
where
    T: Integer,
    I: Iterator<Item = T>,
{
    pub fn new(mut inner: I) -> Self {
        let run = inner.next().map(|x| (x, x));
        Self { inner, run }
    }
}

impl<I, T> FusedIterator for MergeRuns<I, T>
where
    T: Integer,
    I: Iterator<Item = T>,
{
}

impl<I, T> Iterator for MergeRuns<I, T>
where
    T: Integer,
    I: Iterator<Item = T>,
{
    type Item = RangeInclusive<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(cursor) = self.run.as_mut() {
            for next in self.inner.by_ref() {
                assert!(cursor.1 < next, "values must be sorted and deduplicated");
                if cursor.1.add_one() == next {
                    cursor.1 = next;
                } else {
                    let run = cursor.0..=cursor.1;
                    *cursor = (next, next);
                    return Some(run);
                }
            }
        }
        self.run.take().map(|(a, b)| a..=b)
    }
}

impl<I, T> SortedStarts<T> for MergeRuns<I, T>
where
    T: range_set_blaze::Integer,
    I: Iterator<Item = T>,
{
}

impl<I, T> SortedDisjoint<T> for MergeRuns<I, T>
where
    T: range_set_blaze::Integer,
    I: Iterator<Item = T>,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_runs() {
        struct TestCase {
            name: &'static str,
            input: Vec<u64>,
            expected: Vec<RangeInclusive<u64>>,
        }

        let test_cases = vec![
            TestCase {
                name: "empty iterator",
                input: vec![],
                expected: vec![],
            },
            TestCase {
                name: "single value",
                input: vec![5],
                expected: vec![5..=5],
            },
            TestCase {
                name: "two sequential values",
                input: vec![1, 2],
                expected: vec![1..=2],
            },
            TestCase {
                name: "two non-sequential values",
                input: vec![1, 3],
                expected: vec![1..=1, 3..=3],
            },
            TestCase {
                name: "single long run",
                input: vec![1, 2, 3, 4, 5],
                expected: vec![1..=5],
            },
            TestCase {
                name: "multiple runs",
                input: vec![1, 2, 3, 10, 11, 20],
                expected: vec![1..=3, 10..=11, 20..=20],
            },
            TestCase {
                name: "alternating singles and runs",
                input: vec![1, 5, 6, 7, 10, 15, 16],
                expected: vec![1..=1, 5..=7, 10..=10, 15..=16],
            },
            TestCase {
                name: "gaps of various sizes",
                input: vec![1, 3, 4, 8, 9, 10, 20, 21, 22, 23],
                expected: vec![1..=1, 3..=4, 8..=10, 20..=23],
            },
        ];

        for tc in test_cases {
            let result: Vec<_> = MergeRuns::new(tc.input.into_iter()).collect();
            assert_eq!(result, tc.expected, "test case '{}' failed", tc.name);
        }
    }

    #[test]
    #[should_panic(expected = "values must be sorted and deduplicated")]
    fn test_unsorted_values_panic() {
        let input = vec![1, 3, 2];
        let _: Vec<_> = MergeRuns::new(input.into_iter()).collect();
    }

    #[test]
    #[should_panic(expected = "values must be sorted and deduplicated")]
    fn test_duplicate_values_panic() {
        let input = vec![1, 2, 2, 3];
        let _: Vec<_> = MergeRuns::new(input.into_iter()).collect();
    }

    #[test]
    #[should_panic(expected = "values must be sorted and deduplicated")]
    fn test_reverse_sorted_panic() {
        let input = vec![5, 4, 3, 2, 1];
        let _: Vec<_> = MergeRuns::new(input.into_iter()).collect();
    }

    #[test]
    fn test_fused_iterator() {
        let input = vec![1, 2, 3];
        let mut iter = MergeRuns::new(input.into_iter());

        fn is_fused<I: FusedIterator>(_: &I) {}
        is_fused(&iter);

        assert_eq!(iter.next(), Some(1..=3));
        assert_eq!(iter.next(), None);
        // FusedIterator should continue returning None
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }
}
