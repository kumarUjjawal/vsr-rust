use crate::config;
use crate::services::{Storage, StorageError};
use async_trait::async_trait;
use rand::prelude::*;
use rand_distr::Exp;
use std::sync::Mutex;
use tokio::time::{Duration, sleep};

#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub seed: u64,
    pub read_latency_min: u64,
    pub read_latency_mean: u64,
    pub write_latency_min: u64,
    pub write_latency_mean: u64,
    pub read_fault_probability: u8,
    pub write_fault_probability: u8,
}

#[derive(Debug, Clone, Copy)]
pub struct FaultyAreas {
    pub first_offset: u64,
    pub period: u64,
}

/// A simulated storage implementation that implements the `Storage` trait.
/// It uses an in-memory vector as a disk and simulates latency and faults.
pub struct SimulatedStorage {
    memory: Mutex<Vec<u8>>,
    options: Options,
    prng: Mutex<StdRng>,
    faulty_areas: FaultyAreas,
}

impl SimulatedStorage {
    pub fn new(size: u64, options: Options, faulty_areas: FaultyAreas) -> Self {
        assert!(options.read_latency_mean >= options.read_latency_min);
        assert!(options.write_latency_mean >= options.write_latency_min);

        Self {
            memory: Mutex::new(vec![0; size as usize]),
            options,
            prng: Mutex::new(StdRng::seed_from_u64(options.seed)),
            faulty_areas,
        }
    }

    /// Resets any outstanding I/O state while keeping persisted bytes intact.
    ///
    /// The current implementation performs synchronous reads/writes and does
    /// not maintain queued operations, but we still provide the hook so higher
    /// level failure-injection logic can mimic the behaviour of the reference
    /// simulator.
    pub fn reset(&self) {}

    async fn simulate_latency(&self, min: u64, mean: u64) {
        if mean == 0 || mean <= min {
            return;
        }
        let exp = Exp::new(1.0 / (mean - min) as f64).unwrap();
        let delay = min as f64 + self.prng.lock().unwrap().sample(exp);
        sleep(Duration::from_millis(delay as u64)).await;
    }

    fn x_in_100(&self, x: u8) -> bool {
        assert!(x <= 100);
        if x == 0 {
            return false;
        }
        self.prng.lock().unwrap().random_range(0..100) < x
    }

    fn faulty_sectors_range(&self, offset: u64, len: u64) -> Option<(u64, u64)> {
        let message_size_max = config::MESSAGE_SIZE_MAX as u64;
        let period = self.faulty_areas.period;
        if period == 0 {
            return None;
        }

        let faulty_offset = self.faulty_areas.first_offset + (offset / period) * period;
        let start = std::cmp::max(offset, faulty_offset);
        let end = std::cmp::min(offset + len, faulty_offset + message_size_max);

        if start >= end {
            None
        } else {
            Some((start, end))
        }
    }

    fn assert_bounds_and_alignment(&self, buffer: &[u8], offset: u64) {
        assert!(buffer.len() > 0);
        assert_eq!(buffer.len() % config::SECTOR_SIZE, 0);
        assert_eq!(offset % config::SECTOR_SIZE as u64, 0);
        assert_eq!(buffer.as_ptr() as usize % config::SECTOR_SIZE, 0);

        let mem = self.memory.lock().unwrap();
        assert!(offset as usize + buffer.len() <= mem.len());
    }
}

pub fn generate_faulty_areas(rng: &mut StdRng, size: u64, replica_count: u8) -> Vec<FaultyAreas> {
    let message_size_max = config::MESSAGE_SIZE_MAX as u64;
    let mut areas: Vec<FaultyAreas> = match replica_count {
        0 => Vec::new(),
        1 => vec![FaultyAreas {
            first_offset: size,
            period: 1,
        }],
        2 => vec![
            FaultyAreas {
                first_offset: 0 * message_size_max,
                period: 4 * message_size_max,
            },
            FaultyAreas {
                first_offset: 2 * message_size_max,
                period: 4 * message_size_max,
            },
        ],
        3 => vec![
            FaultyAreas {
                first_offset: 0 * message_size_max,
                period: 6 * message_size_max,
            },
            FaultyAreas {
                first_offset: 2 * message_size_max,
                period: 6 * message_size_max,
            },
            FaultyAreas {
                first_offset: 4 * message_size_max,
                period: 6 * message_size_max,
            },
        ],
        4 => vec![
            FaultyAreas {
                first_offset: 0 * message_size_max,
                period: 4 * message_size_max,
            },
            FaultyAreas {
                first_offset: 0 * message_size_max,
                period: 4 * message_size_max,
            },
            FaultyAreas {
                first_offset: 2 * message_size_max,
                period: 4 * message_size_max,
            },
            FaultyAreas {
                first_offset: 2 * message_size_max,
                period: 4 * message_size_max,
            },
        ],
        5 => vec![
            FaultyAreas {
                first_offset: 0 * message_size_max,
                period: 6 * message_size_max,
            },
            FaultyAreas {
                first_offset: 0 * message_size_max,
                period: 6 * message_size_max,
            },
            FaultyAreas {
                first_offset: 2 * message_size_max,
                period: 6 * message_size_max,
            },
            FaultyAreas {
                first_offset: 2 * message_size_max,
                period: 6 * message_size_max,
            },
            FaultyAreas {
                first_offset: 4 * message_size_max,
                period: 6 * message_size_max,
            },
        ],
        6 => vec![
            FaultyAreas {
                first_offset: 0 * message_size_max,
                period: 6 * message_size_max,
            },
            FaultyAreas {
                first_offset: 0 * message_size_max,
                period: 6 * message_size_max,
            },
            FaultyAreas {
                first_offset: 2 * message_size_max,
                period: 6 * message_size_max,
            },
            FaultyAreas {
                first_offset: 2 * message_size_max,
                period: 6 * message_size_max,
            },
            FaultyAreas {
                first_offset: 4 * message_size_max,
                period: 6 * message_size_max,
            },
            FaultyAreas {
                first_offset: 4 * message_size_max,
                period: 6 * message_size_max,
            },
        ],
        _ => Vec::new(),
    };

    areas.truncate(replica_count as usize);
    areas.shuffle(rng);
    areas
}

#[async_trait]
impl Storage for SimulatedStorage {
    async fn read_sectors(&self, buffer: &mut [u8], offset: u64) -> Result<(), StorageError> {
        self.assert_bounds_and_alignment(buffer, offset);

        self.simulate_latency(
            self.options.read_latency_min,
            self.options.read_latency_mean,
        )
        .await;

        let mut mem = self.memory.lock().unwrap();
        let len = buffer.len();

        if let Some((fault_start, fault_end)) = self.faulty_sectors_range(offset, len as u64)
            && self.x_in_100(self.options.read_fault_probability)
        {
            let relative_start = (fault_start - offset) as usize;
            let relative_end = (fault_end - offset) as usize;
            let faulty_slice = &mut mem[offset as usize..][relative_start..relative_end];
            self.prng.lock().unwrap().fill_bytes(faulty_slice);
        }

        let disk_slice = &mem[offset as usize..offset as usize + len];
        buffer.copy_from_slice(disk_slice);

        Ok(())
    }

    async fn write_sectors(&self, buffer: &[u8], offset: u64) -> Result<(), StorageError> {
        self.assert_bounds_and_alignment(buffer, offset);

        self.simulate_latency(
            self.options.write_latency_min,
            self.options.write_latency_mean,
        )
        .await;

        let mut mem = self.memory.lock().unwrap();
        let len = buffer.len();

        mem[offset as usize..offset as usize + len].copy_from_slice(buffer);

        if let Some((fault_start, fault_end)) = self.faulty_sectors_range(offset, len as u64)
            && self.x_in_100(self.options.write_fault_probability)
        {
            let faulty_slice = &mut mem[fault_start as usize..fault_end as usize];
            self.prng.lock().unwrap().fill_bytes(faulty_slice);
        }

        Ok(())
    }
}
