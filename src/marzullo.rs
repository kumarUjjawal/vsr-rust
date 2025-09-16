#[derive(Debug, Clone, PartialEq)]
pub struct Interval {
    lower_bound: i64,
    upper_bound: i64,
    sources_true: u8,
    sources_false: u8,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tuple {
    source: u8,
    offset: i64,
    bound: Bound,
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

    return false;
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

    #[test]
    fn test_empty_input() {
        let mut tuples: Vec<Tuple> = vec![];
        let result = Marzullo::smallest_interval(&mut tuples);
        assert_eq!(result.lower_bound, 0);
        assert_eq!(result.upper_bound, 0);
        assert_eq!(result.sources_true, 0);
        assert_eq!(result.sources_false, 0);
    }

    #[test]
    fn test_single_source() {
        let mut tuples = vec![
            Tuple {
                source: 0,
                offset: 10,
                bound: Bound::Lower,
            },
            Tuple {
                source: 0,
                offset: 20,
                bound: Bound::Upper,
            },
        ];
        let result = Marzullo::smallest_interval(&mut tuples);
        assert_eq!(result.lower_bound, 10);
        assert_eq!(result.upper_bound, 20);
        assert_eq!(result.sources_true, 1);
        assert_eq!(result.sources_false, 0);
    }
}
