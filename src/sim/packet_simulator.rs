use rand::prelude::*;
use rand_distr::Exp;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fmt::Debug;

/// Configuration options for the PacketSimulator.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub one_way_delay_mean: u64,
    pub one_way_delay_min: u64,
    pub packet_loss_probability: u8,
    pub packet_replay_probability: u8,
    pub seed: u64,
    pub replica_count: u8,
    pub client_count: u8,
    pub node_count: u8,
    pub partition_mode: PartitionMode,
    pub partition_probability: u8,
    pub unpartition_probability: u8,
    pub partition_stability: u32,
    pub unpartition_stability: u32,
    pub path_maximum_capacity: u8,
    pub path_clog_duration_mean: u64,
    pub path_clog_probability: u8,
}

/// Defines a network path between two nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Path {
    pub source: u8,
    pub target: u8,
}

/// Defines how network partitions are created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionMode {
    UniformSize,
    UniformPartition,
    IsolateSingle,
}

/// A wrapper for packets stored in the priority queue (min-heap).
/// Implements `Ord` in reverse to make `BinaryHeap` behave as a min-heap.
struct Data<P> {
    expiry: u64,
    packet: P,
    path: Path,
}

impl<P> PartialEq for Data<P> {
    fn eq(&self, other: &Self) -> bool {
        self.expiry == other.expiry
    }
}
impl<P> Eq for Data<P> {}

impl<P> PartialOrd for Data<P> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<P> Ord for Data<P> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering to make BinaryHeap a min-heap
        other.expiry.cmp(&self.expiry)
    }
}

/// A generic, fault-injecting network simulator.
///
/// It is generic over any packet type `P` and delivers them to a callback.
pub struct PacketSimulator<P: Clone + Send + 'static> {
    paths: Vec<BinaryHeap<Data<P>>>,
    path_clogged_until: Vec<u64>,
    ticks: u64,
    options: Options,
    prng: StdRng,
    is_partitioned: bool,
    /// `partition[i] = true` means replica `i` is in the majority partition.
    partition: Vec<bool>,
    stability_counter: u32,
    replica_indices: Vec<usize>,
    callback: Box<dyn Fn(P, Path) + Send>,
}

impl<P: Clone + Send + 'static> PacketSimulator<P> {
    pub fn new(options: Options, callback: Box<dyn Fn(P, Path) + Send>) -> Self {
        let node_count = options.node_count as usize;
        let replica_count = options.replica_count as usize;
        let mut paths = Vec::with_capacity(node_count * node_count);
        let path_capacity = options.path_maximum_capacity as usize;
        for _ in 0..(node_count * node_count) {
            paths.push(BinaryHeap::with_capacity(path_capacity.max(1)));
        }

        let mut path_clogged_until = vec![0u64; node_count * node_count];
        path_clogged_until.fill(0);

        Self {
            paths,
            path_clogged_until,
            ticks: 0,
            options,
            prng: StdRng::seed_from_u64(options.seed),
            is_partitioned: false,
            partition: vec![false; replica_count],
            stability_counter: options.unpartition_stability,
            replica_indices: (0..replica_count).collect(),
            callback,
        }
    }

    /// Submits a packet to the network, scheduling it for future delivery.
    pub fn submit_packet(&mut self, packet: P, path: Path) {
        let data = Data {
            expiry: self.ticks + self.sample_one_way_delay(),
            packet,
            path,
        };

        let path_index = self.path_index(path);
        self.push_with_capacity(path_index, data);
    }

    /// Advances the simulation by one time step.
    pub fn tick(&mut self) {
        self.ticks += 1;
        self.tick_partitions();

        for source in 0..self.options.node_count {
            for target in 0..self.options.node_count {
                let path = Path { source, target };
                if self.is_path_clogged(path) {
                    continue;
                }

                let path_index = self.path_index(path);

                while let Some(peeked) = self.paths[path_index].peek() {
                    if peeked.expiry > self.ticks {
                        break;
                    }

                    let data = self.paths[path_index].pop().unwrap();

                    if self.is_partitioned
                        && source < self.options.replica_count
                        && target < self.options.replica_count
                        && self.partition[source as usize] != self.partition[target as usize]
                    {
                        // Packet dropped due to partition.
                        continue;
                    }

                    if self
                        .prng
                        .random_ratio(self.options.packet_loss_probability as u32, 100)
                    {
                        // Packet dropped due to loss.
                        continue;
                    }

                    if self
                        .prng
                        .random_ratio(self.options.packet_replay_probability as u32, 100)
                    {
                        let replay_expiry = self.ticks + self.sample_one_way_delay();
                        let replay_data = Data {
                            expiry: replay_expiry,
                            path: data.path,
                            packet: data.packet.clone(),
                        };
                        self.push_with_capacity(path_index, replay_data);
                        (self.callback)(data.packet, data.path);
                    } else {
                        (self.callback)(data.packet, data.path);
                    }
                }

                let reverse_path = Path {
                    source: target,
                    target: source,
                };

                if self.should_clog(reverse_path) {
                    let clog_duration = self.sample_clog_duration();
                    self.clog_path(reverse_path, clog_duration);
                }
            }
        }
    }

    /// Updates the network partition state for the current tick.
    fn tick_partitions(&mut self) {
        if self.stability_counter > 0 {
            self.stability_counter -= 1;
            return;
        }

        if self.is_partitioned {
            if self
                .prng
                .random_ratio(self.options.unpartition_probability as u32, 100)
            {
                self.unpartition_network();
            }
        } else if self.options.replica_count > 1
            && self
                .prng
                .random_ratio(self.options.partition_probability as u32, 100)
        {
            self.partition_network();
        }
    }

    fn partition_network(&mut self) {
        self.is_partitioned = true;
        self.stability_counter = self.options.partition_stability;
        let replica_count = self.options.replica_count as usize;

        match self.options.partition_mode {
            PartitionMode::IsolateSingle => {
                self.partition.fill(false);
                if replica_count > 0 {
                    let isolated_node = self.prng.random_range(0..replica_count);
                    self.partition[isolated_node] = true;
                }
            }
            PartitionMode::UniformSize => {
                if replica_count > 1 {
                    let partition_size = self.prng.random_range(1..replica_count);
                    self.replica_indices.clear();
                    self.replica_indices.extend(0..replica_count);
                    self.replica_indices.shuffle(&mut self.prng);
                    for (i, &replica_idx) in self.replica_indices.iter().enumerate() {
                        self.partition[replica_idx] = i < partition_size;
                    }
                }
            }
            PartitionMode::UniformPartition => {
                if replica_count > 0 {
                    loop {
                        for i in 0..replica_count {
                            self.partition[i] = self.prng.random_bool(0.5);
                        }
                        let first = self.partition[0];
                        if !self.partition.iter().all(|&p| p == first) {
                            break;
                        }
                    }
                }
            }
        }
    }

    fn unpartition_network(&mut self) {
        self.is_partitioned = false;
        self.stability_counter = self.options.unpartition_stability;
    }

    fn path_index(&self, path: Path) -> usize {
        (path.source as usize * self.options.node_count as usize) + path.target as usize
    }

    fn is_path_clogged(&self, path: Path) -> bool {
        let idx = self.path_index(path);
        self.path_clogged_until[idx] > self.ticks
    }

    fn should_clog(&mut self, path: Path) -> bool {
        if self.options.path_clog_probability == 0 {
            return false;
        }
        if path.source as usize >= self.options.node_count as usize
            || path.target as usize >= self.options.node_count as usize
        {
            return false;
        }
        self.prng
            .random_ratio(self.options.path_clog_probability as u32, 100)
    }

    fn clog_path(&mut self, path: Path, duration: u64) {
        let idx = self.path_index(path);
        self.path_clogged_until[idx] = self.ticks + duration;
    }

    fn sample_one_way_delay(&mut self) -> u64 {
        let min = self.options.one_way_delay_min;
        let mean = self.options.one_way_delay_mean;
        if mean == 0 || mean <= min {
            return min;
        }

        let rate = 1.0 / (mean - min) as f64;
        let exp = Exp::new(rate).unwrap();
        min + self.prng.sample(exp) as u64
    }

    fn sample_clog_duration(&mut self) -> u64 {
        let mean = self.options.path_clog_duration_mean;
        if mean == 0 {
            return 0;
        }

        let exp = Exp::new(1.0 / mean as f64).unwrap();
        self.prng.sample(exp) as u64
    }

    fn push_with_capacity(&mut self, index: usize, data: Data<P>) {
        let cap = self.options.path_maximum_capacity as usize;
        let mut heap = std::mem::take(&mut self.paths[index]);

        if cap > 0 && heap.len() >= cap {
            let mut items = heap.into_vec();
            if !items.is_empty() {
                let drop_idx = self.prng.random_range(0..items.len());
                items.swap_remove(drop_idx);
            }
            heap = BinaryHeap::from(items);
        }

        heap.push(data);
        self.paths[index] = heap;
    }
}
