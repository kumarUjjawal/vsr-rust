#[derive(Debug, Clone, PartialEq)]
pub struct Interval {
    pub lower_bound: i64,
    pub upper_bound: i64,
    pub sources_true: u8,
    pub sources_false: u8,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tuple {
    pub source: u8,
    pub offset: i64,
    pub bound: Bound,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Bound {
    Lower,
    Upper,
}

pub struct Marzullo;

impl Marzullo {
    pub fn smallest_interval(tuples: &mut [Tuple]) -> Interval {
        let sources = (tuples.len() / 2) as u8;

        if sources == 0 {
            return Interval {
                lower_bound: 0,
                upper_bound: 0,
                sources_true: 0,
                sources_false: 0,
            };
        }

        insertion_sort(tuples);

        let mut best: i64 = 0;
        let mut count: i64 = 0;
        let mut previous: Option<Tuple> = None;
        let mut interval = Interval {
            lower_bound: 0,
            upper_bound: 0,
            sources_true: 0,
            sources_false: 0,
        };

        for (i, &tuple) in tuples.iter().enumerate() {
            if let Some(p) = previous {
                debug_assert!(p.offset <= tuple.offset);
                if p.offset == tuple.offset {
                    if p.bound != tuple.bound {
                        debug_assert!(p.bound == Bound::Lower && tuple.bound == Bound::Upper);
                    } else {
                        debug_assert!(p.source < tuple.source);
                    }
                }
            }

            previous = Some(tuple);

            match tuple.bound {
                Bound::Lower => count += 1,
                Bound::Upper => count -= 1,
            }

            if count > best {
                best = count;
                interval.lower_bound = tuple.offset;
                interval.upper_bound = tuples[i + 1].offset;
            } else if count == best && tuples[i + 1].bound == Bound::Upper {
                let alternative = tuples[i + 1].offset - tuple.offset;
                if alternative < interval.upper_bound - interval.lower_bound {
                    interval.lower_bound = tuple.offset;
                    interval.upper_bound = tuples[i + 1].offset
                }
            }
        }

        debug_assert!(previous.unwrap().bound == Bound::Upper);

        debug_assert!(best <= sources as i64);
        interval.sources_true = best as u8;
        interval.sources_false = sources - best as u8;
        debug_assert!(interval.sources_true + interval.sources_false == sources);

        interval
    }
}

fn less_than(a: &Tuple, b: &Tuple) -> bool {
    if a.offset < b.offset {
        return true;
    };
    if b.offset < a.offset {
        return false;
    }
    if a.bound == Bound::Lower && b.bound == Bound::Upper {
        return true;
    }
    if b.bound == Bound::Lower && a.bound == Bound::Upper {
        return false;
    }

    if a.source < b.source {
        return true;
    }
    if b.source < a.source {
        return false;
    }

    false
}

fn insertion_sort(tuples: &mut [Tuple]) {
    for i in 1..tuples.len() {
        let key = tuples[i];
        let mut j = i;
        while j > 0 && less_than(&key, &tuples[j - 1]) {
            tuples[j] = tuples[j - 1];
            j -= 1;
        }

        tuples[j] = key;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_smallest_interval(bounds: &[i64], expected: Interval) {
        let mut tuples = Vec::new();
        for (i, &bound) in bounds.iter().enumerate() {
            tuples.push(Tuple {
                source: (i / 2) as u8,
                offset: bound,
                bound: if i % 2 == 0 {
                    Bound::Lower
                } else {
                    Bound::Upper
                },
            });
        }
    }

    #[test]
    fn test_three_overlapping_sources() {
        test_smallest_interval(
            &[11, 13, 10, 12, 8, 12],
            Interval {
                lower_bound: 11,
                upper_bound: 12,
                sources_true: 3,
                sources_false: 0,
            },
        );
    }

    #[test]
    fn test_partial_overlap() {
        test_smallest_interval(
            &[8, 12, 11, 13, 14, 15],
            Interval {
                lower_bound: 11,
                upper_bound: 12,
                sources_true: 2,
                sources_false: 1,
            },
        );
    }

    #[test]
    fn test_point_intersection() {
        test_smallest_interval(
            &[-10, 10, -1, 1, 0, 0],
            Interval {
                lower_bound: 0,
                upper_bound: 0,
                sources_true: 3,
                sources_false: 0,
            },
        );
    }

    #[test]
    fn test_inclusive_overlap() {
        // The upper bound of the first interval overlaps inclusively with the lower of the last.
        test_smallest_interval(
            &[8, 12, 10, 11, 8, 10],
            Interval {
                lower_bound: 10,
                upper_bound: 10,
                sources_true: 3,
                sources_false: 0,
            },
        );
    }

    #[test]
    fn test_tie_breaking_first_smallest() {
        // The first smallest interval is selected. The alternative with equal overlap is 10..12.
        // However, while this shares the same number of sources, it is not the smallest interval.
        test_smallest_interval(
            &[8, 12, 10, 12, 8, 9],
            Interval {
                lower_bound: 8,
                upper_bound: 9,
                sources_true: 2,
                sources_false: 1,
            },
        );
    }

    #[test]
    fn test_tie_breaking_last_smallest() {
        // The last smallest interval is selected. The alternative with equal overlap is 7..9.
        // However, while this shares the same number of sources, it is not the smallest interval.
        test_smallest_interval(
            &[7, 9, 7, 12, 10, 11],
            Interval {
                lower_bound: 10,
                upper_bound: 11,
                sources_true: 2,
                sources_false: 1,
            },
        );
    }

    #[test]
    fn test_negative_offsets() {
        // The same idea as the previous test, but with negative offsets.
        test_smallest_interval(
            &[-9, -7, -12, -7, -11, -10],
            Interval {
                lower_bound: -11,
                upper_bound: -10,
                sources_true: 2,
                sources_false: 1,
            },
        );
    }

    #[test]
    fn test_empty_cluster() {
        // A cluster of one with no remote sources.
        test_smallest_interval(
            &[],
            Interval {
                lower_bound: 0,
                upper_bound: 0,
                sources_true: 0,
                sources_false: 0,
            },
        );
    }

    #[test]
    fn test_single_remote_source() {
        // A cluster of two with one remote source.
        test_smallest_interval(
            &[1, 3],
            Interval {
                lower_bound: 1,
                upper_bound: 3,
                sources_true: 1,
                sources_false: 0,
            },
        );
    }

    #[test]
    fn test_two_sources_agreement() {
        // A cluster of three with agreement.
        test_smallest_interval(
            &[1, 3, 2, 2],
            Interval {
                lower_bound: 2,
                upper_bound: 2,
                sources_true: 2,
                sources_false: 0,
            },
        );
    }

    #[test]
    fn test_two_sources_no_agreement() {
        // A cluster of three with no agreement, still returns the smallest interval.
        test_smallest_interval(
            &[1, 3, 4, 5],
            Interval {
                lower_bound: 4,
                upper_bound: 5,
                sources_true: 1,
                sources_false: 1,
            },
        );
    }
}
