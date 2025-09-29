use crate::config;
use rand::prelude::*;

/// Calculate exponential backoff with jitter to prevent cascading failure.
fn exponential_backoff_with_jitter(rng: &mut impl Rng, min: u64, max: u64, attempt: u8) -> u64 {
    let range = max.saturating_sub(min);
    if range == 0 {
        return min;
    }

    let exponent = attempt as u64;
    let power = 2u64.saturating_sub(exponent);
    let min_nonzero = std::cmp::max(1, min);

    let backoff = std::cmp::min(range, min_nonzero.saturating_mul(power));
    let jitter = if backoff > 0 {
        rng.random_range(0..=backoff)
    } else {
        0
    };

    min + jitter
}

#[derive(Debug)]
pub struct Timeout {
    pub name: &'static str,
    pub id: u128,
    pub after: u64,
    pub attempts: u8,
    pub rtt: u64,
    pub rtt_multiple: u8,
    pub ticks: u64,
    pub ticking: bool,
}

impl Timeout {
    pub fn new(name: &'static str, id: u128, after: u64) -> Self {
        Self {
            name,
            id,
            after,
            attempts: 0,
            rtt: config::RTT_TICKS as u64,
            rtt_multiple: config::RTT_MULTIPLE as u8,
            ticks: 0,
            ticking: false,
        }
    }

    pub fn start(&mut self) {
        self.attempts = 0;
        self.ticks = 0;
        self.ticking = true;
    }

    pub fn stop(&mut self) {
        self.attempts = 0;
        self.ticks = 0;
        self.ticking = false;
    }

    pub fn reset(&mut self) {
        self.attempts = 0;
        self.ticks = 0;
        assert!(self.ticking, "Cannot reset a stopped timeout");
    }

    pub fn tick(&mut self) {
        if self.ticking {
            self.ticks += 1;
        }
    }

    pub fn fired(&self) -> bool {
        if self.ticking && self.ticks >= self.after {
            if self.ticks > self.after {
                panic!(
                    "Timeout {} fired on a previous tick and was not reset!",
                    self.name
                );
            }
            true
        } else {
            false
        }
    }

    pub fn backoff(&mut self, rng: &mut impl Rng) {
        assert!(self.ticking, "Cannot backoff a stopped timout");
        self.ticks = 0;
        self.attempts = self.attempts.wrapping_add(1);

        let base = self.rtt * self.rtt_multiple as u64;
        let jitter = exponential_backoff_with_jitter(
            rng,
            config::BACKOFF_MIN_TICKS as u64,
            config::BACKOFF_MAX_TICKS as u64,
            self.attempts,
        );

        self.after = base.saturating_add(jitter);
    }
}
