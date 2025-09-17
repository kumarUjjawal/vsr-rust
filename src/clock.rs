use crate::config;
use log::{debug, error, info, warn};
use std::cmp;
use std::time::{Duration, Instant};

const CLOCK_OFFSET_TOLERANCE_MAX: Duration =
    Duration::from_millis(config::CLOCK_OFFSET_TOLERANCE_MAX_MS);
const EPOCH_MAX: Duration = Duration::from_millis(config::CLOCK_EPOCH_MAX_MS);
const WINDOW_MIN: Duration = Duration::from_millis(config::CLOCK_SYNCHRONIZATION_WINDOW_MIN_MS);
const WINDOW_MAX: Duration = Duration::from_millis(config::CLOCK_SYNCHRONIZATION_WINDOW_MAX_MS);

use crate::marzullo::{Bound, Interval, Marzullo, Tuple};

pub trait Time {
    fn monotonic(&self) -> Instant;
    fn realtime(&self) -> i64;
    fn tick(&mut self);
}

#[derive(Debug, Clone, Copy)]
struct Sample {
    /// The relative difference between our wall clock reading and that of the remote clock source.
    clock_offset: i64,
    one_way_delay: Duration,
}

#[derive(Debug)]
struct Epoch {
    /// The best clock offset sample per remote clock source (with minimum one way delay) collected
    /// over the course of a window period of several seconds.
    sources: Vec<Option<Sample>>,

    /// The total number of samples learned while synchronizing this epoch.
    samples: usize,

    /// The monotonic clock timestamp when this epoch began. We use this to measure elapsed time.
    monotonic: Instant,

    /// The wall clock timestamp when this epoch began. We add the elapsed monotonic time to this
    /// plus the synchronized clock offset to arrive at a synchronized realtime timestamp. We
    /// capture this realtime when starting the epoch, before we take any samples, to guard against
    /// any jumps in the system's realtime clock from impacting our measurements.
    realtime: i64,

    /// Once we have enough source clock offset samples in agreement, the epoch is synchronized.
    /// We then have lower and upper bounds on the true cluster time, and can install this epoch for
    /// subsequent clock readings. This epoch is then valid for several seconds, while clock drift
    /// has not had enough time to accumulate into any significant clock skew, and while we collect
    /// samples for the next epoch to refresh and replace this one.
    synchronized: Option<Interval>,

    /// A guard to prevent synchronizing too often without having learned any new samples.
    learned: bool,
}

impl Epoch {
    fn new(replica_count: u8) -> Self {
        Self {
            sources: vec![None; replica_count as usize],
            samples: 0,
            monotonic: Instant::now(),
            realtime: 0,
            synchronized: None,
            learned: false,
        }
    }

    fn elapsed<T: Time>(&self, clock: &Clock<T>) -> Duration {
        clock.time.monotonic().duration_since(self.monotonic)
    }

    fn reset(&mut self, replica: u8, monotonic: Instant, realtime: i64) {
        self.sources.fill(None);
        // A replica always has zero clock offset and network delay to its own system time reading:
        self.sources[replica as usize] = Some(Sample {
            clock_offset: 0,
            one_way_delay: Duration::ZERO,
        });
        self.samples = 1;
        self.monotonic = monotonic;
        self.realtime = realtime;
        self.synchronized = None;
        self.learned = false;
    }

    fn sources_sampled(&self) -> usize {
        self.sources.iter().filter(|s| s.is_some()).count()
    }
}

pub struct Clock<T: Time> {
    /// The index of the replica using this clock to provide synchronized time.
    replica: u8,

    /// The underlying time source for this clock (system time or deterministic time).
    time: T,

    /// An epoch from which the clock can read synchronized clock timestamps within safe bounds.
    /// At least `config.clock_synchronization_window_min_ms` is needed for this to be ready to use.
    epoch: Epoch,

    /// The next epoch (collecting samples and being synchronized) to replace the current epoch.
    window: Epoch,

    /// A static allocation to convert window samples into tuple bounds for Marzullo's algorithm.
    marzullo_tuples: Vec<Tuple>,

    /// A kill switch to revert to unsynchronized realtime.
    synchronization_disabled: bool,
}

impl<T: Time> Clock<T> {
    pub fn new(replica_count: u8, replica: u8, time: T) -> Self {
        assert!(replica_count > 0);
        assert!(replica < replica_count);

        // There are two Marzullo tuple bounds (lower and upper) per source clock offset sample:
        let marzullo_tuples = Vec::with_capacity(replica_count as usize * 2);

        let mut clock = Self {
            replica,
            time,
            epoch: Epoch::new(replica_count),
            window: Epoch::new(replica_count),
            marzullo_tuples,
            synchronization_disabled: replica_count == 1, // A cluster of one cannot synchronize.
        };

        // Reset the current epoch to be unsynchronized,
        clock.reset_epoch();
        // and open a new epoch window to start collecting samples...
        clock.reset_window();

        clock
    }

    /// Called by `Replica.on_pong()` with:
    /// * the index of the `replica` that has replied to our ping with a pong,
    /// * our monotonic timestamp `m0` embedded in the ping we sent, carried over into this pong,
    /// * the remote replica's `realtime()` timestamp `t1`, and
    /// * our monotonic timestamp `m2` as captured by our `Replica.on_pong()` handler.
    pub fn learn(&mut self, replica: u8, m0: Instant, t1: i64, m2: Instant) {
        if self.synchronization_disabled {
            return;
        }

        // A network routing fault must have replayed one of our outbound messages back against us:
        if replica == self.replica {
            warn!("{}: learn: replica == self.replica", self.replica);
            return;
        }

        // Our m0 and m2 readings should always be monotonically increasing if not equal.
        // Crucially, it is possible for a very fast network to have m0 == m2, especially where
        // `config.tick_ms` is at a more course granularity. We must therefore tolerate RTT=0 or
        // otherwise we would have a liveness bug simply because we would be throwing away
        // perfectly good clock samples.
        // This condition should never be true. Reject this as a bad sample:
        if m0 > m2 {
            warn!("{}: learn: m0={:?} > m2={:?}", self.replica, m0, m2);
            return;
        }

        // We may receive delayed packets after a reboot, in which case m0/m2 may be invalid:
        if m0 < self.window.monotonic {
            warn!(
                "{}: learn: m0={:?} < window.monotonic={:?}",
                self.replica, m0, self.window.monotonic
            );
            return;
        }

        if m2 < self.window.monotonic {
            warn!(
                "{}: learn: m2={:?} < window.monotonic={:?}",
                self.replica, m2, self.window.monotonic
            );
            return;
        }

        let elapsed = m2.duration_since(self.window.monotonic);
        if elapsed > WINDOW_MAX {
            warn!(
                "{}: learn: elapsed={:?} > window_max={:?}",
                self.replica, elapsed, WINDOW_MAX
            );
            return;
        }

        let round_trip_time = m2.duration_since(m0);
        let one_way_delay = round_trip_time / 2;
        let t2 = self.window.realtime + elapsed.as_nanos() as i64;
        let clock_offset = t1 + one_way_delay.as_nanos() as i64 - t2;
        let asymmetric_delay = self.estimate_asymmetric_delay(replica, one_way_delay, clock_offset);
        let clock_offset_corrected = clock_offset + asymmetric_delay;

        debug!(
            "{}: learn: replica={} m0={:?} t1={} m2={:?} t2={} one_way_delay={:?} \
             asymmetric_delay={} clock_offset={}",
            self.replica,
            replica,
            m0,
            t1,
            m2,
            t2,
            one_way_delay,
            asymmetric_delay,
            clock_offset_corrected
        );

        // The less network delay, the more likely we have an accurate clock offset measurement:
        self.window.sources[replica as usize] = minimum_one_way_delay(
            self.window.sources[replica as usize],
            Some(Sample {
                clock_offset: clock_offset_corrected,
                one_way_delay,
            }),
        );

        self.window.samples += 1;

        // We decouple calls to `synchronize()` so that it's not triggered by these network events.
        // Otherwise, excessive duplicate network packets would burn the CPU.
        self.window.learned = true;
    }

    /// Called by `Replica.on_ping_timeout()` to provide `m0` when we decide to send a ping.
    /// Called by `Replica.on_pong()` to provide `m2` when we receive a pong.
    pub fn monotonic(&self) -> Instant {
        self.time.monotonic()
    }

    /// Called by `Replica.on_ping()` when responding to a ping with a pong.
    /// This should never be used by the state machine, only for measuring clock offsets.
    pub fn realtime(&self) -> i64 {
        self.time.realtime()
    }

    /// Called by `StateMachine.prepare_timestamp()` when the leader wants to timestamp a batch.
    /// If the leader's clock is not synchronized with the cluster, it must wait until it is.
    /// Returns the system time clamped to be within our synchronized lower and upper bounds.
    /// This is complementary to NTP and allows clusters with very accurate time to make use of it,
    /// while providing guard rails for when NTP is partitioned or unable to correct quickly enough.
    pub fn realtime_synchronized(&self) -> Option<i64> {
        if self.synchronization_disabled {
            Some(self.realtime())
        } else if let Some(interval) = &self.epoch.synchronized {
            let elapsed = self.epoch.elapsed(self).as_nanos() as i64;
            Some(cmp::max(
                cmp::min(
                    self.realtime(),
                    self.epoch.realtime + elapsed + interval.upper_bound,
                ),
                self.epoch.realtime + elapsed + interval.lower_bound,
            ))
        } else {
            None
        }
    }

    pub fn tick(&mut self) {
        self.time.tick();

        if self.synchronization_disabled {
            return;
        }

        self.synchronize();

        // Expire the current epoch if successive windows failed to synchronize:
        // Gradual clock drift prevents us from using an epoch for more than a few seconds.
        if self.epoch.elapsed(self) >= EPOCH_MAX {
            error!(
                "{}: no agreement on cluster time (partitioned or too many clock faults)",
                self.replica
            );
            self.reset_epoch();
        }
    }

    /// Estimates the asymmetric delay for a sample compared to the previous window, according to
    /// Algorithm 1 from Section 4.2, "A System for Clock Synchronization in an Internet of Things".
    fn estimate_asymmetric_delay(
        &self,
        replica: u8,
        one_way_delay: Duration,
        clock_offset: i64,
    ) -> i64 {
        // Note that `one_way_delay` may be 0 for very fast networks.

        let error_margin = Duration::from_millis(10).as_nanos() as i64;

        if let Some(epoch) = self.epoch.sources[replica as usize] {
            if one_way_delay <= epoch.one_way_delay {
                0
            } else if clock_offset > epoch.clock_offset + error_margin {
                // The asymmetric error is on the forward network path.
                -((one_way_delay - epoch.one_way_delay).as_nanos() as i64)
            } else if clock_offset < epoch.clock_offset - error_margin {
                // The asymmetric error is on the reverse network path.
                (one_way_delay - epoch.one_way_delay).as_nanos() as i64
            } else {
                0
            }
        } else {
            0
        }
    }

    fn synchronize(&mut self) {
        assert!(self.window.synchronized.is_none());

        // Wait until the window has enough accurate samples:
        let elapsed = self.window.elapsed(self);
        if elapsed < WINDOW_MIN {
            return;
        }
        if elapsed >= WINDOW_MAX {
            // We took too long to synchronize the window, expire stale samples...
            let sources_sampled = self.window.sources_sampled();
            if sources_sampled <= self.window.sources.len() / 2 {
                error!(
                    "{}: synchronization failed, partitioned (sources={} samples={})",
                    self.replica, sources_sampled, self.window.samples
                );
            } else {
                error!(
                    "{}: synchronization failed, no agreement (sources={} samples={})",
                    self.replica, sources_sampled, self.window.samples
                );
            }
            self.reset_window();
            return;
        }

        if !self.window.learned {
            return;
        }
        // Do not reset `learned` any earlier than this (before we have attempted to synchronize).
        self.window.learned = false;

        // Starting with the most clock offset tolerance, while we have a majority, find the best
        // smallest interval with the least clock offset tolerance, reducing tolerance at each step:
        let mut tolerance = CLOCK_OFFSET_TOLERANCE_MAX;
        let mut terminate = false;
        let mut rounds = 0;

        // Do at least one round if tolerance=0 and cap the number of rounds to avoid runaway loops.
        while !terminate && rounds < 64 {
            if tolerance == Duration::ZERO {
                terminate = true;
            }
            rounds += 1;

            let tuples = self.window_tuples(tolerance);
            let interval = Marzullo::smallest_interval(&mut tuples.clone());
            let majority = interval.sources_true > (self.window.sources.len() / 2) as u8;

            if !majority {
                break;
            }

            // The new interval may reduce the number of `sources_true` while also decreasing error.
            // In other words, provided we maintain a majority, we prefer tighter tolerance bounds.
            self.window.synchronized = Some(interval);

            tolerance = tolerance / 2;
        }

        // Wait for more accurate samples or until we timeout the window for lack of majority:
        if self.window.synchronized.is_none() {
            return;
        }

        std::mem::swap(&mut self.epoch, &mut self.window);
        self.reset_window();

        self.after_synchronization();
    }

    fn after_synchronization(&self) {
        let new_interval = self.epoch.synchronized.as_ref().unwrap();

        debug!(
            "{}: synchronized: truechimers={}/{} clock_offset={}..{} accuracy={}",
            self.replica,
            new_interval.sources_true,
            self.epoch.sources.len(),
            format_duration_signed(new_interval.lower_bound),
            format_duration_signed(new_interval.upper_bound),
            format_duration_signed(new_interval.upper_bound - new_interval.lower_bound),
        );

        let elapsed = self.epoch.elapsed(self).as_nanos() as i64;
        let system = self.realtime();
        let lower = self.epoch.realtime + elapsed + new_interval.lower_bound;
        let upper = self.epoch.realtime + elapsed + new_interval.upper_bound;
        let cluster = cmp::max(cmp::min(system, upper), lower);

        if system == cluster {
            // System time is within bounds
        } else if system < lower {
            let delta = lower - system;
            if delta < Duration::from_millis(1).as_nanos() as i64 {
                info!(
                    "{}: system time is {} behind",
                    self.replica,
                    format_duration_signed(delta)
                );
            } else {
                error!(
                    "{}: system time is {} behind, clamping system time to cluster time",
                    self.replica,
                    format_duration_signed(delta)
                );
            }
        } else {
            let delta = system - upper;
            if delta < Duration::from_millis(1).as_nanos() as i64 {
                info!(
                    "{}: system time is {} ahead",
                    self.replica,
                    format_duration_signed(delta)
                );
            } else {
                error!(
                    "{}: system time is {} ahead, clamping system time to cluster time",
                    self.replica,
                    format_duration_signed(delta)
                );
            }
        }
    }

    fn window_tuples(&mut self, tolerance: Duration) -> Vec<Tuple> {
        assert_eq!(
            self.window.sources[self.replica as usize]
                .unwrap()
                .clock_offset,
            0
        );
        assert_eq!(
            self.window.sources[self.replica as usize]
                .unwrap()
                .one_way_delay,
            Duration::ZERO
        );

        self.marzullo_tuples.clear();

        for (source, sample) in self.window.sources.iter().enumerate() {
            if let Some(sample) = sample {
                let tolerance_ns = tolerance.as_nanos() as i64;
                let one_way_delay_ns = sample.one_way_delay.as_nanos() as i64;

                self.marzullo_tuples.push(Tuple {
                    source: source as u8,
                    offset: sample.clock_offset - (one_way_delay_ns + tolerance_ns),
                    bound: Bound::Lower,
                });
                self.marzullo_tuples.push(Tuple {
                    source: source as u8,
                    offset: sample.clock_offset + (one_way_delay_ns + tolerance_ns),
                    bound: Bound::Upper,
                });
            }
        }

        self.marzullo_tuples.clone()
    }

    fn reset_epoch(&mut self) {
        let monotonic = self.time.monotonic();
        let realtime = self.time.realtime();
        self.epoch.reset(self.replica, monotonic, realtime);
    }

    fn reset_window(&mut self) {
        let monotonic = self.time.monotonic();
        let realtime = self.time.realtime();
        self.window.reset(self.replica, monotonic, realtime);
    }
}

fn minimum_one_way_delay(a: Option<Sample>, b: Option<Sample>) -> Option<Sample> {
    match (a, b) {
        (None, b) => b,
        (a, None) => a,
        (Some(a), Some(b)) => {
            if a.one_way_delay < b.one_way_delay {
                Some(a)
            } else {
                // Choose B if B's one way delay is less or the same (we assume B is the newer sample):
                Some(b)
            }
        }
    }
}

// Helper function to format durations with sign (simplified version)
fn format_duration_signed(nanos: i64) -> String {
    if nanos >= 0 {
        format!("{}ns", nanos)
    } else {
        format!("-{}ns", -nanos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    struct MockTime {
        monotonic_time: Instant,
        realtime: i64,
    }

    impl MockTime {
        fn new() -> Self {
            Self {
                monotonic_time: Instant::now(),
                realtime: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as i64,
            }
        }
    }

    impl Time for MockTime {
        fn monotonic(&self) -> Instant {
            self.monotonic_time
        }

        fn realtime(&self) -> i64 {
            self.realtime
        }

        fn tick(&mut self) {
            // Mock implementation
        }
    }

    #[test]
    fn test_clock_creation() {
        let time = MockTime::new();
        let clock = Clock::new(3, 0, time);

        assert_eq!(clock.replica, 0);
        assert!(!clock.synchronization_disabled);
        assert!(clock.realtime_synchronized().is_none());
    }

    #[test]
    fn test_single_replica_cluster() {
        let time = MockTime::new();
        let clock = Clock::new(1, 0, time);

        assert!(clock.synchronization_disabled);
        assert!(clock.realtime_synchronized().is_some());
    }
}
