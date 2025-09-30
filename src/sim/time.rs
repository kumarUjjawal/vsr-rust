use crate::services::TimeSource;
use rand::prelude::*;
use rand_distr::StandardNormal;

/// Defines the different models of clock drift that can be simulated.
#[derive(Debug, Clone, Copy)]
pub enum OffsetType {
    Linear,
    Periodic,
    Step,
    NonIdeal,
}

/// A deterministic time source for simulations, implementing the `TimeSource` trait.
///
/// It allows for simulating various kinds of clock drift by modeling an offset
/// from a perfect monotonic clock.
#[derive(Debug, Clone)]
pub struct DeterministicTime {
    /// The duration of a single tick in nanoseconds.
    resolution: u64,
    offset_type: OffsetType,
    /// Coefficient A for the offset function (drift, amplitude, etc.).
    offset_coefficient_a: i64,
    /// Coefficient B for the offset function (initial offset, period, etc.).
    offset_coefficient_b: i64,
    /// Coefficient C for the offset function (randomness amplitude).
    offset_coefficient_c: u32,
    prng: StdRng,
    /// The number of ticks elapsed since initialization.
    ticks: u64,
    /// The instant in time chosen as the origin of this time source.
    epoch: i64,
}

impl DeterministicTime {
    pub fn new(seed: u64) -> Self {
        Self {
            resolution: 0,
            offset_type: OffsetType::Linear,
            offset_coefficient_a: 0,
            offset_coefficient_b: 0,
            offset_coefficient_c: 0,
            prng: StdRng::seed_from_u64(seed),
            ticks: 0,
            epoch: 0,
        }
    }

    /// Calculates the clock offset for a given number of ticks based on the configured model.
    fn offset(&mut self, ticks: u64) -> i64 {
        match self.offset_type {
            OffsetType::Linear => {
                let drift_per_tick = self.offset_coefficient_a;
                (ticks as i64 * drift_per_tick) + self.offset_coefficient_b
            }
            OffsetType::Periodic => {
                let ticks_f = ticks as f64;
                let period_f = self.offset_coefficient_b as f64;
                let amplitude_f = self.offset_coefficient_a as f64;
                let unscaled = (ticks_f * 2.0 * std::f64::consts::PI / period_f).sin();
                (amplitude_f * unscaled).floor() as i64
            }
            OffsetType::Step => {
                if ticks as i64 > self.offset_coefficient_b {
                    self.offset_coefficient_a
                } else {
                    0
                }
            }
            OffsetType::NonIdeal => {
                let ticks_f = ticks as f64;
                let period_f = self.offset_coefficient_b as f64
                    + self.prng.sample::<f64, _>(StandardNormal) * 10.0;
                let amplitude_f = self.offset_coefficient_a as f64;

                let phase = ticks_f * 2.0 * std::f64::consts::PI / period_f;
                let unscaled = phase.sin();
                let scaled = (amplitude_f * unscaled).floor() as i64;

                let random_offset = self.prng.random_range(
                    -(self.offset_coefficient_c as i64)..=(self.offset_coefficient_c as i64),
                );

                scaled + random_offset
            }
        }
    }
}

impl DeterministicTime {
    /// Creates a deterministic clock with a linear offset model and the
    /// provided tick resolution. Matches the defaults used by the reference
    /// simulator so that Rust replicas experience the same clock drift.
    pub fn with_linear_model(resolution: u64, seed: u64) -> Self {
        Self {
            resolution,
            offset_type: OffsetType::Linear,
            offset_coefficient_a: 0,
            offset_coefficient_b: 0,
            offset_coefficient_c: 0,
            prng: StdRng::seed_from_u64(seed),
            ticks: 0,
            epoch: 0,
        }
    }
}

impl TimeSource for DeterministicTime {
    fn monotonic(&self) -> u64 {
        self.ticks * self.resolution
    }

    fn realtime(&mut self) -> i64 {
        let monotonic_time = self.monotonic() as i64;
        let offset = self.offset(self.ticks);
        self.epoch + monotonic_time - offset
    }

    fn tick(&mut self) {
        self.ticks += 1;
    }
}
