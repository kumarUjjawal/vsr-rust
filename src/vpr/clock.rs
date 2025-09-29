use crate::config;
use crate::services::TimeSource;
use crate::vpr::marzullo::{self, Marzullo};

const NS_PER_MS: u64 = 1_000_000;

#[derive(Debug, Clone, Copy)]
struct Sample {
    /// The relative difference between our wall clock and a remote clock.
    clock_offset: i64,
    one_way_delay: u64,
}

struct Epoch {
    /// The best clock offset sample per remote clock source.
    sources: Vec<Option<Sample>>,
    /// Monotonic timestamp when this epoch began.
    monotonic_start: u64,
    /// Wall clock timestamp when this epoch began.
    realtime_start: i64,
    /// Once synchronized, contains the lower and upper bounds of the true cluster time.
    synchronized: Option<marzullo::Interval>,
}

impl Epoch {
    fn new(replica_count: u8, time_source: &mut impl TimeSource, self_replica_id: u8) -> Self {
        let mut sources = vec![None; replica_count as usize];
        // A replica always has zero offset and delay to itself.
        sources[self_replica_id as usize] = Some(Sample {
            clock_offset: 0,
            one_way_delay: 0,
        });

        Self {
            sources,
            monotonic_start: time_source.monotonic(),
            realtime_start: time_source.realtime(),
            synchronized: None,
        }
    }

    fn elapsed_ns(&self, time_source: &impl TimeSource) -> u64 {
        time_source.monotonic().saturating_sub(self.monotonic_start)
    }
}

/// A distributed, fault-tolerant clock that provides synchronized time.
pub struct Clock<T: TimeSource> {
    time_source: T,
    replica_id: u8,
    /// The currently active epoch, used for providing synchronized time.
    epoch: Epoch,
    /// The next epoch, which is currently collecting samples.
    window: Epoch,
    /// Pre-allocated buffer for Marzullo's algorithm tuples.
    marzullo_tuples: Vec<marzullo::Tuple>,
    synchronization_disabled: bool,
}

impl<T: TimeSource> Clock<T> {
    pub fn new(mut time_source: T, replica_count: u8, replica_id: u8) -> Self {
        let epoch = Epoch::new(replica_count, &mut time_source, replica_id);
        let window = Epoch::new(replica_count, &mut time_source, replica_id);

        let mut new_self = Self {
            time_source,
            replica_id,
            epoch,
            window,
            marzullo_tuples: vec![
                marzullo::Tuple {
                    offset: 0,
                    bound: marzullo::Bound::Lower
                };
                replica_count as usize * 2
            ],
            synchronization_disabled: replica_count <= 1,
        };

        new_self.epoch.synchronized = None;
        new_self
    }
    /// Learns from a remote clock sample (m0, t1, m2).
    pub fn learn(&mut self, remote_replica_id: u8, m0: u64, t1: i64, m2: u64) {
        if self.synchronization_disabled || remote_replica_id == self.replica_id {
            return;
        }

        if m0 > m2 {
            return;
        } // Invalid sample
        if m0 < self.window.monotonic_start {
            return;
        } // Stale sample from before window

        let round_trip_time = m2 - m0;
        let one_way_delay = round_trip_time / 2;
        let elapsed_in_window = m2 - self.window.monotonic_start;
        let t2 = self.window.realtime_start + elapsed_in_window as i64;
        let clock_offset = (t1 + one_way_delay as i64) - t2;

        let new_sample = Sample {
            clock_offset,
            one_way_delay,
        };

        // Keep the sample with the minimum one-way delay, as it's the most accurate.
        let existing = &mut self.window.sources[remote_replica_id as usize];
        if existing.is_none() || one_way_delay < existing.unwrap().one_way_delay {
            *existing = Some(new_sample);
        }
    }

    /// The main tick function, called periodically to advance time and attempt synchronization.
    pub fn tick(&mut self) {
        self.time_source.tick();
        if self.synchronization_disabled {
            return;
        }

        self.synchronize();

        // Expire the current epoch if it's too old.
        let epoch_max_ns = config::CLOCK_EPOCH_MAX_MS * NS_PER_MS;
        if self.epoch.elapsed_ns(&self.time_source) >= epoch_max_ns {
            self.epoch = Epoch::new(
                self.window.sources.len() as u8,
                &mut self.time_source,
                self.replica_id,
            );
        }
    }

    /// Attempts to create a new synchronized epoch from the current sample window.
    fn synchronize(&mut self) {
        let window_min_ns = config::CLOCK_SYNCHRONIZATION_WINDOW_MIN_MS * NS_PER_MS;
        let window_max_ns = config::CLOCK_SYNCHRONIZATION_WINDOW_MAX_MS * NS_PER_MS;
        let elapsed = self.window.elapsed_ns(&self.time_source);

        if elapsed < window_min_ns {
            return;
        }

        if elapsed >= window_max_ns {
            // Window expired, start a new one.
            self.window = Epoch::new(
                self.window.sources.len() as u8,
                &mut self.time_source,
                self.replica_id,
            );
            return;
        }

        // Prepare tuples for Marzullo's algorithm.
        let mut tuple_count = 0;
        let tolerance = config::CLOCK_OFFSET_TOLERANCE_MAX_MS * NS_PER_MS;
        for sample_opt in &self.window.sources {
            if let Some(sample) = sample_opt {
                let error_margin = (sample.one_way_delay + tolerance) as i64;
                self.marzullo_tuples[tuple_count] = marzullo::Tuple {
                    offset: sample.clock_offset - error_margin,
                    bound: marzullo::Bound::Lower,
                };
                self.marzullo_tuples[tuple_count + 1] = marzullo::Tuple {
                    offset: sample.clock_offset + error_margin,
                    bound: marzullo::Bound::Upper,
                };
                tuple_count += 2;
            }
        }

        let interval = Marzullo::smallest_interval(&mut self.marzullo_tuples[..tuple_count]);
        let replica_count = self.window.sources.len();
        let majority = (replica_count / 2) + 1;

        // If we have a majority agreement, promote the window to the new epoch.
        if interval.sources_true as usize >= majority {
            self.epoch = std::mem::replace(
                &mut self.window,
                Epoch::new(replica_count as u8, &mut self.time_source, self.replica_id),
            );
            self.epoch.synchronized = Some(interval);
        }
    }

    /// Returns the current synchronized time, or None if the clock is not synchronized.
    pub fn realtime_synchronized(&mut self) -> Option<i64> {
        if self.synchronization_disabled {
            return Some(self.time_source.realtime());
        }

        if let Some(interval) = self.epoch.synchronized {
            let elapsed = self.epoch.elapsed_ns(&self.time_source) as i64;
            let system_realtime = self.time_source.realtime();

            let lower_bound = self.epoch.realtime_start + elapsed + interval.lower_bound;
            let upper_bound = self.epoch.realtime_start + elapsed + interval.upper_bound;

            // Clamp the system's wall-clock time to be within the synchronized bounds.
            Some(system_realtime.clamp(lower_bound, upper_bound))
        } else {
            None
        }
    }

    pub fn time_source_mut(&mut self) -> &mut T {
        &mut self.time_source
    }
}
