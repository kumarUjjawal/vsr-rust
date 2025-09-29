use crate::vpr::client::Client;
use crate::config;
use crate::message_pool::{MessagePool, ProcessType};
use crate::services::{MessageBus, StateMachine, Storage, TimeSource};
use crate::replica::Replica;
use crate::sim::network::{Network, SimulatedMessageBus};
use crate::sim::storage::{FaultyAreas, Options as StorageOptions, SimulatedStorage};
use crate::sim::time::{DeterministicTime, OffsetType};
use crate::sim::state_machine::HashingStateMachine;
use crate::vsr;
use rand::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Receiver};
use tokio::task::JoinHandle;

/// Owns all the components of a simulated cluster.
pub struct Cluster {
    pub replicas: Vec<Replica<SimulatedMessageBus, HashingStateMachine, SimulatedStorage, DeterministicTime>>,
    pub clients: Vec<Client<SimulatedMessageBus>>,
    // Handles for the spawned tasks to ensure they are properly managed.
    _replica_handles: Vec<JoinHandle<()>>,
    _client_handles: Vec<JoinHandle<()>>,
    _network_handle: JoinHandle<()>,
}

impl Cluster {
    pub async fn new(replica_count: u8, client_count: u8, mut rng: StdRng) -> Self {
        let replica_pool = MessagePool::new(ProcessType::Replica).unwrap();
        let client_pool = MessagePool::new(ProcessType::Client).unwrap();
        
        let mut network = Network::new(replica_count, client_count, replica_pool.clone());

        let mut replicas = Vec::new();
        let mut replica_handles = Vec::new();
        for i in 0..replica_count {
            let (bus, mut rx) = network.register_process(vsr::ProcessType::Replica(i));
            let storage = Arc::new(SimulatedStorage::new(
                config::JOURNAL_SIZE_MAX as u64,
                StorageOptions { /* ... default options ... */ },
                FaultyAreas { first_offset: 0, period: 0 },
            ));
            let time = DeterministicTime::new(rng.gen());
            let sm = HashingStateMachine::new(rng.gen());
            
            let mut replica = Replica::new(
                Arc::new(bus), replica_pool.clone(), storage, time, sm,
                0, replica_count, i
            );
            
            let handle = tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    replica.on_message(msg).await;
                }
            });
            replicas.push(replica);
            replica_handles.push(handle);
        }

        let mut clients = Vec::new();
        let mut client_handles = Vec::new();
        for i in 0..client_count {
            let client_id = i as u128 + 1;
            let (bus, mut rx) = network.register_process(vsr::ProcessType::Client(client_id));
            let mut client = Client::new(
                Arc::new(bus), client_pool.clone(), client_id, 0, replica_count
            );
            
            let handle = tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    client.on_message(msg);
                }
            });
            clients.push(client);
            client_handles.push(handle);
        }

        let network_handle = tokio::spawn(async move { network.run().await; });

        Self {
            replicas,
            clients,
            _replica_handles: replica_handles,
            _client_handles: client_handles,
            _network_handle: network_handle,
        }
    }

    /// Ticks every component in the cluster forward by one step.
    pub fn tick(&mut self) {
        for replica in &mut self.replicas {
            replica.tick();
        }
        for client in &mut self.clients {
            client.tick();
        }
    }
}
