use circular_buffer::CircularBuffer;
use graft_core::PageIdx;

pub trait Oracle {
    /// `observe_cache_hit` is called whenever Graft satisfies a page read from
    /// it's local cache. This function is not called on a cache miss.
    fn observe_cache_hit(&mut self, pageidx: PageIdx);

    /// `predict_next` is called when Graft has a cache miss, and can be used to
    /// hint that Graft should fetch additional pages along with the requested
    /// page. The returned iterator should be empty if no additional pages
    /// should be fetched, and it does not need to include the requested page.
    fn predict_next(&mut self, pageidx: PageIdx) -> impl Iterator<Item = PageIdx>;
}

pub struct NoopOracle;

impl Oracle for NoopOracle {
    fn observe_cache_hit(&mut self, _pageidx: PageIdx) {
        // do nothing
    }

    fn predict_next(&mut self, _pageidx: PageIdx) -> impl Iterator<Item = PageIdx> {
        // predict nothing
        std::iter::empty()
    }
}

/// `LeapOracle` is an implementation of the algorithm described by the paper
/// "Effectively Prefetching Remote Memory with Leap". It provides an Oracle
/// that attempts to predict future page requests based on trends found in
/// recent history.
///
/// Hasan Al Maruf and Mosharaf Chowdhury. (2020). [_Effectively Prefetching
/// Remote Memory with Leap_][1]. In _Proceedings of the 2020 USENIX Conference on
/// Usenix Annual Technical Conference (USENIX ATC'20)_, Article 58, 843–857.
/// USENIX Association, USA.
///
/// [1]: https://www.usenix.org/system/files/atc20-maruf.pdf
#[derive(Debug, Default)]
pub struct LeapOracle {
    /// the last observed read
    last_read: PageIdx,
    /// history of page index deltas ordered from most recent to least recent
    history: CircularBuffer<32, isize>,
    /// the last prediction
    prediction: Vec<PageIdx>,
    /// cache hits since the last prediction
    prediction_hits: usize,
}

impl LeapOracle {
    /// Tries to find a trend in the data by searching for strict majorities in
    /// the access history. Returns None if no trend can be found.
    fn find_trend(&self) -> Option<isize> {
        const N_SPLIT: usize = 4;
        let mut window_size = (self.history.len() / N_SPLIT).max(1);
        while window_size <= self.history.len() {
            let window = self.history.range(0..window_size);
            if let Some(trend) = boyer_moore_strict_majority(window.copied()) {
                return Some(trend);
            }
            window_size *= 2;
        }
        None
    }

    fn record_read(&mut self, pageidx: PageIdx) {
        // update history buffer
        let delta = pageidx.to_u32() as isize - self.last_read.to_u32() as isize;
        self.history.push_front(delta);

        // update last read and whether or not we are following the current trend
        self.last_read = pageidx;
    }
}

impl Oracle for LeapOracle {
    fn observe_cache_hit(&mut self, pageidx: PageIdx) {
        // ignore duplicate reads
        if pageidx == self.last_read {
            return;
        }

        // update hits counter
        if self.prediction.contains(&pageidx) {
            self.prediction_hits += 1;
        }

        self.record_read(pageidx);
    }

    fn predict_next(&mut self, pageidx: PageIdx) -> impl Iterator<Item = PageIdx> {
        const MAX_LOOKAHEAD: usize = 8;

        // calculate the trend
        let trend = self.find_trend();

        // calculate the number of predictions to make
        let lookahead = if self.prediction_hits == 0 {
            // the last prediction wasn't great
            // check to see if reads are starting to follow a trend
            if TrendIter::once(self.last_read, trend.unwrap_or(1)) == Some(pageidx) {
                1
            } else {
                0
            }
        } else {
            // the last prediction had hits
            (self.prediction_hits + 1)
                .checked_next_power_of_two()
                .unwrap_or(MAX_LOOKAHEAD)
        }
        // ensure lookhead doesn't grow too large
        .min(MAX_LOOKAHEAD)
        // shrink lookahead smoothly
        .max(self.prediction.len() / 2);

        // clear previous prediction state
        self.prediction_hits = 0;
        self.prediction.clear();

        // construct next prediction
        if lookahead != 0 {
            if let Some(trend) = trend {
                // trend found, prefetch along the trend
                self.prediction
                    .extend(TrendIter::new(pageidx, trend).take(lookahead));
            } else {
                // no trend found, prefetch around the current page index
                for i in 1..=(lookahead / 2) {
                    self.prediction.push(pageidx.saturating_add(i as u32));
                    self.prediction.push(pageidx.saturating_sub(i as u32));
                }
            }
        } else {
            // predictions are disabled until a new trend is established
        }

        self.record_read(pageidx);

        self.prediction.iter().copied()
    }
}

struct TrendIter {
    cursor: isize,
    trend: isize,
}

impl TrendIter {
    fn new(pageidx: PageIdx, trend: isize) -> Self {
        Self { cursor: pageidx.to_u32() as isize, trend }
    }

    fn once(pageidx: PageIdx, trend: isize) -> Option<PageIdx> {
        Self::new(pageidx, trend).next()
    }
}

impl Iterator for TrendIter {
    type Item = PageIdx;

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor += self.trend;
        PageIdx::try_new(self.cursor as u32)
    }
}

/// Computes the majority value contained by an iterator in two passes. If no
/// strict majority (occurs > count/2 times) is found returns None.
fn boyer_moore_strict_majority<I>(iter: I) -> Option<isize>
where
    I: Iterator<Item = isize> + Clone,
{
    let mut candidate = 0;
    let mut count = 0;
    let mut total_count = 0;

    // First pass: Find candidate and count total elements
    for num in iter.clone() {
        total_count += 1;
        if count == 0 {
            candidate = num;
            count = 1;
        } else if num == candidate {
            count += 1;
        } else {
            count -= 1;
        }
    }

    // Second pass: Verify candidate
    let mut occurrence = 0;
    for num in iter {
        if num == candidate {
            occurrence += 1;
        }
    }

    if occurrence > total_count / 2 {
        Some(candidate)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_boyer_moore_strict_majority() {
        let test_cases = [
            (vec![], None),
            (vec![1], Some(1)),
            (vec![1, 0], None),
            (vec![0, 0, 0, 0], Some(0)),
            (vec![0, 1, 0, 0], Some(0)),
            (vec![0, 1, 1, 0], None),
            (vec![0, 1, 1, 1], Some(1)),
            (vec![0, 1, 1, 1, 0], Some(1)),
            (vec![72, -3, -3, -3], Some(-3)),
            (vec![-3, -58, 2, 2], None),
            (vec![72, -3, -3, -3, -3, -58, 2, 2], None),
            (vec![2, -58, 2, 2], Some(2)),
            (vec![2, 2, 2, 4, -41, -39, 2, 2], Some(2)),
        ];

        for (input, expected) in test_cases {
            assert_eq!(boyer_moore_strict_majority(input.into_iter()), expected);
        }
    }

    #[test]
    fn test_leap_oracle() {
        #[derive(Default)]
        struct State {
            oracle: LeapOracle,
            cache: HashSet<PageIdx>,
        }
        struct Case {
            name: &'static str,
            reads: Vec<u32>,
            expected_misses: usize,
        }

        fn run_test(state: &mut State, case: Case) {
            let mut misses = 0;
            for pageidx in case.reads {
                let pageidx = PageIdx::new(pageidx);
                if state.cache.contains(&pageidx) {
                    state.oracle.observe_cache_hit(pageidx);
                } else {
                    state.cache.insert(pageidx);
                    state.cache.extend(state.oracle.predict_next(pageidx));
                    misses += 1;
                }
            }
            assert_eq!(
                misses, case.expected_misses,
                "{} failed: unexpected miss count",
                case.name
            );
        }

        let cases = [
            Case {
                name: "sequential",
                reads: (1..=100).collect(),
                expected_misses: 15,
            },
            Case {
                name: "random",
                reads: vec![
                    1, 56, 12, 100, 124, 15550, 51, 10, 7, 4101, 23, 1, 154, 1856, 15,
                ],
                // every read is a miss
                expected_misses: 14,
            },
            Case {
                name: "interrupted-scan",
                reads: (1..=100)
                    .enumerate()
                    // inject a huge random read every 15 pages to test algorithm resilience
                    .map(
                        |(i, p): (usize, u32)| {
                            if i % 15 == 0 { p + 116589 } else { p }
                        },
                    )
                    .collect(),
                expected_misses: 25,
            },
            Case {
                name: "stride-2",
                reads: (1..=200).step_by(2).collect(),
                expected_misses: 15,
            },
            Case {
                name: "reverse",
                reads: (1..=100).rev().collect(),
                expected_misses: 15,
            },
            Case {
                name: "reverse-stride-2",
                reads: (1..=200).rev().step_by(2).collect(),
                expected_misses: 15,
            },
            Case {
                name: "multi-pattern",
                reads: (1..=100)
                    .chain((101..=300).step_by(2))
                    .chain((301..=500).rev().step_by(2))
                    .chain((501..=600).rev())
                    .collect(),
                expected_misses: 59,
            },
            Case {
                name: "multi-pattern-random-middle",
                reads: (1..=100)
                    .chain((101..=300).step_by(2))
                    .chain([
                        1, 56, 12, 100, 124, 15550, 51, 10, 7, 4101, 23, 1, 154, 1856, 15,
                    ])
                    .chain((301..=700).rev().step_by(4))
                    .chain((701..=800).rev())
                    .collect(),
                expected_misses: 68,
            },
        ];
        for case in cases {
            run_test(&mut State::default(), case);
        }
    }
}
