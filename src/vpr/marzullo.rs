use std::cmp::Ordering;

/// Marzullo's algorithm, used to select sources for estimating accurate time.
pub struct Marzullo;

impl Marzullo {
    /// Returns the smallest interval consistent with the largest number of sources.
    pub fn smallest_interval(tuples: &mut [Tuple]) -> Interval {
        let sources_total = tuples.len() / 2;
        if sources_total == 0 {
            return Interval {
                lower_bound: 0,
                upper_bound: 0,
                sources_true: 0,
            };
        }

        // Sort tuples to process them in order of their offset.
        tuples.sort_unstable();

        let mut best_count = 0;
        let mut current_count = 0;
        let mut interval = Interval::default();

        for i in 0..tuples.len() {
            let tuple = &tuples[i];
            match tuple.bound {
                Bound::Lower => current_count += 1,
                Bound::Upper => current_count -= 1,
            }

            // We look for the interval that starts at the current tuple and ends
            // at the next one, which contains the most overlapping sources.
            if current_count > best_count {
                best_count = current_count;
                interval.lower_bound = tuple.offset;
                // This is safe because an upper bound must always follow.
                interval.upper_bound = tuples[i + 1].offset;
            }
        }

        interval.sources_true = best_count as u8;
        interval
    }
}

/// The smallest interval consistent with the largest number of sources.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Interval {
    pub lower_bound: i64,
    pub upper_bound: i64,
    pub sources_true: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bound {
    Lower,
    Upper,
}

/// Represents either the lower or upper end of a time source's uncertainty interval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tuple {
    pub offset: i64,
    pub bound: Bound,
}

// Custom Ord implementation to sort tuples correctly for the algorithm.
impl Ord for Tuple {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.offset != other.offset {
            return self.offset.cmp(&other.offset);
        }
        // If offsets are equal, lower bounds come before upper bounds
        // to correctly handle zero-duration overlaps.
        match (self.bound, other.bound) {
            (Bound::Lower, Bound::Upper) => Ordering::Less,
            (Bound::Upper, Bound::Lower) => Ordering::Greater,
            _ => Ordering::Equal,
        }
    }
}

impl PartialOrd for Tuple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
