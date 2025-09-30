use crate::config;
use crate::message_pool::{MessagePool, PooledMessage, ProcessType};
use crate::replica::Replica;
use crate::sim::network::{Network, NetworkOptions, SimulatedMessageBus};
use crate::sim::state_machine::HashingStateMachine;
use crate::sim::storage::{self, FaultyAreas, Options as StorageOptions, SimulatedStorage};
use crate::sim::time::DeterministicTime;
use crate::vpr::client::Client;
use crate::vsr;
use rand::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinHandle;

#[derive(Clone)]
pub struct ClusterOptions {
    pub cluster: u32,
    pub replica_count: u8,
    pub client_count: u8,
    pub seed: u64,
    pub network: NetworkOptions,
    pub storage: StorageOptions,
}

/// Owns all the components of a simulated cluster.
pub struct Cluster {
    storages: Vec<Arc<SimulatedStorage>>,
    replica_failures: Vec<u32>,
    pub replicas:
        Vec<Replica<SimulatedMessageBus, HashingStateMachine, SimulatedStorage, DeterministicTime>>,
    pub clients: Vec<Client<SimulatedMessageBus>>,
    replica_receivers: Vec<Receiver<PooledMessage>>,
    client_receivers: Vec<Receiver<PooledMessage>>,
    _network_handle: JoinHandle<()>,
}

impl Cluster {
    pub async fn new(options: ClusterOptions) -> Self {
        let mut rng = StdRng::seed_from_u64(options.seed);
        let replica_count = options.replica_count;
        let client_count = options.client_count;
        let replica_pool = MessagePool::new(ProcessType::Replica);
        let client_pool = MessagePool::new(ProcessType::Client);

        let mut network = Network::new(replica_pool.clone(), options.network.clone());

        let mut replicas = Vec::new();
        let mut replica_receivers = Vec::new();
        let mut storages = Vec::with_capacity(replica_count as usize);
        let replica_failures = vec![0u32; replica_count as usize];
        let state_seed = options.seed;

        let faulty_areas = storage::generate_faulty_areas(
            &mut rng,
            config::JOURNAL_SIZE_MAX as u64,
            replica_count,
        );
        for i in 0..replica_count {
            let (bus, rx) = network.register_process(vsr::ProcessType::Replica(i));
            let bus = Arc::new(bus);
            let storage = Arc::new(SimulatedStorage::new(
                config::JOURNAL_SIZE_MAX as u64,
                StorageOptions {
                    seed: rng.random(),
                    ..options.storage
                },
                faulty_areas
                    .get(i as usize)
                    .copied()
                    .unwrap_or(FaultyAreas {
                        first_offset: 0,
                        period: 0,
                    }),
            ));
            let time_seed = rng.random();
            let time = DeterministicTime::with_linear_model(
                (config::TICK_MS as u64) * 1_000_000,
                time_seed,
            );
            let sm = HashingStateMachine::new(state_seed);

            let replica = Replica::new(
                bus.clone(),
                replica_pool.clone(),
                storage.clone(),
                time,
                sm,
                options.cluster,
                replica_count,
                i,
            );
            storages.push(storage);
            replicas.push(replica);
            replica_receivers.push(rx);
        }

        let mut clients = Vec::new();
        let mut client_receivers = Vec::new();
        for i in 0..client_count {
            let client_id = i as u128 + 1;
            let (bus, rx) = network.register_process(vsr::ProcessType::Client(client_id));
            let client = Client::new(
                Arc::new(bus),
                client_pool.clone(),
                client_id,
                options.cluster,
                replica_count,
            );
            clients.push(client);
            client_receivers.push(rx);
        }

        let network_handle = tokio::spawn(async move {
            network.run().await;
        });

        Self {
            storages,
            replica_failures,
            replicas,
            clients,
            replica_receivers,
            client_receivers,
            _network_handle: network_handle,
        }
    }

    /// Ticks every component in the cluster forward by one step.
    pub async fn tick(&mut self) {
        for (index, (replica, rx)) in self
            .replicas
            .iter_mut()
            .zip(self.replica_receivers.iter_mut())
            .enumerate()
        {
            if let Some(downtime) = self.replica_failures.get_mut(index)
                && *downtime > 0
            {
                *downtime = downtime.saturating_sub(1);
                while let Ok(msg) = rx.try_recv() {
                    drop(msg);
                }
                continue;
            }

            loop {
                match rx.try_recv() {
                    Ok(msg) => {
                        println!(
                            "delivering {:?} to replica {}",
                            msg.header().command,
                            replica.replica_id()
                        );
                        replica.on_message(msg).await
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }
            replica.tick().await;
        }
        for (client, rx) in self
            .clients
            .iter_mut()
            .zip(self.client_receivers.iter_mut())
        {
            loop {
                match rx.try_recv() {
                    Ok(msg) => client.on_message(msg),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }
            client.tick();
        }
    }
}

impl Cluster {
    pub fn replica_count(&self) -> u8 {
        self.replicas.len() as u8
    }

    pub fn replica_is_down(&self, replica: u8) -> bool {
        self.replica_failures
            .get(replica as usize)
            .map(|ticks| *ticks > 0)
            .unwrap_or(false)
    }

    pub fn active_replica_count(&self) -> usize {
        self.replica_failures
            .iter()
            .filter(|ticks| **ticks == 0)
            .count()
    }

    /// Schedules a replica to be unavailable for the specified duration in
    /// ticks. Returns `true` if the replica transitioned from healthy to
    /// failing.
    pub fn schedule_replica_failure(&mut self, replica: u8, downtime: u32) -> bool {
        let Some(slot) = self.replica_failures.get_mut(replica as usize) else {
            return false;
        };

        if *slot > 0 {
            return false;
        }

        if let Some(storage) = self.storages.get(replica as usize) {
            storage.reset();
        }

        *slot = downtime;
        true
    }
}
