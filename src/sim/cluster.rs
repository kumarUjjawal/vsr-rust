use crate::vpr::client::Client;
use crate::config;
use crate::message_pool::{MessagePool, ProcessType, PooledMessage};
use crate::replica::Replica;
use crate::sim::network::{Network, SimulatedMessageBus};
use crate::sim::state_machine::HashingStateMachine;
use crate::sim::storage::{FaultyAreas, Options as StorageOptions, SimulatedStorage};
use crate::sim::time::DeterministicTime;
use crate::vsr;
use rand::prelude::*;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::error::TryRecvError;

/// Owns all the components of a simulated cluster.
pub struct Cluster {
    pub replicas: Vec<Replica<SimulatedMessageBus, HashingStateMachine, SimulatedStorage, DeterministicTime>>,
    pub clients: Vec<Client<SimulatedMessageBus>>,
    replica_receivers: Vec<Receiver<PooledMessage>>,
    client_receivers: Vec<Receiver<PooledMessage>>,
    _network_handle: JoinHandle<()>,
}

impl Cluster {
    pub async fn new(replica_count: u8, client_count: u8, mut rng: StdRng) -> Self {
        let replica_pool = MessagePool::new(ProcessType::Replica);
        let client_pool = MessagePool::new(ProcessType::Client);
        
        let mut network = Network::new(replica_count, client_count, replica_pool.clone());

        let mut replicas = Vec::new();
        let mut replica_receivers = Vec::new();
        for i in 0..replica_count {
            let (bus, rx) = network.register_process(vsr::ProcessType::Replica(i));
            let storage = Arc::new(SimulatedStorage::new(
                config::JOURNAL_SIZE_MAX as u64,
                StorageOptions {
                    seed: rng.random(),
                    read_latency_min: 1,
                    read_latency_mean: 3,
                    write_latency_min: 1,
                    write_latency_mean: 3,
                    read_fault_probability: 0,
                    write_fault_probability: 0,
                },
                FaultyAreas { first_offset: 0, period: 0 },
            ));
            let time = DeterministicTime::new(rng.random());
            let sm = HashingStateMachine::new(rng.random());
            
            let replica = Replica::new(
                Arc::new(bus), replica_pool.clone(), storage, time, sm,
                0, replica_count, i
            );
            replicas.push(replica);
            replica_receivers.push(rx);
        }

        let mut clients = Vec::new();
        let mut client_receivers = Vec::new();
        for i in 0..client_count {
            let client_id = i as u128 + 1;
            let (bus, rx) = network.register_process(vsr::ProcessType::Client(client_id));
            let client = Client::new(
                Arc::new(bus), client_pool.clone(), client_id, 0, replica_count
            );
            clients.push(client);
            client_receivers.push(rx);
        }

        let network_handle = tokio::spawn(async move { network.run().await; });

        Self {
            replicas,
            clients,
            replica_receivers,
            client_receivers,
            _network_handle: network_handle,
        }
    }

    /// Ticks every component in the cluster forward by one step.
    pub async fn tick(&mut self) {
        for (replica, rx) in self
            .replicas
            .iter_mut()
            .zip(self.replica_receivers.iter_mut())
        {
            loop {
                match rx.try_recv() {
                    Ok(msg) => replica.on_message(msg).await,
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
