use crate::config;
use crate::message_pool::{MessagePool, PooledMessage};
use crate::services::MessageBus;
use crate::sim::packet_simulator::{self, PacketSimulator, PartitionMode};
use crate::vsr;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::{self, Duration, Interval};

#[derive(Debug, Clone)]
pub struct Packet {
    pub buffer: Vec<u8>,
}

#[derive(Debug)]
enum NetworkCommand {
    Send {
        path: packet_simulator::Path,
        packet: Packet,
    },
    Deliver {
        path: packet_simulator::Path,
        packet: Packet,
    },
}

#[derive(Debug, Clone)]
pub struct SimulatedMessageBus {
    sender: Sender<NetworkCommand>,
    process_id: vsr::ProcessType,
}

#[async_trait]
impl MessageBus for SimulatedMessageBus {
    async fn send_message_to_replica(&self, replica_id: u8, message: PooledMessage) {
        let source_addr = self.process_id.to_address();
        let path = packet_simulator::Path {
            source: source_addr,
            target: replica_id,
        };
        let packet = Packet {
            buffer: message.buffer.to_vec(),
        };
        self.sender
            .send(NetworkCommand::Send { path, packet })
            .await
            .ok();
    }

    async fn send_message_to_client(&self, client_id: u128, message: PooledMessage) {
        let target_addr = (config::REPLICAS_MAX as u128 + client_id) as u8;
        let path = packet_simulator::Path {
            source: self.process_id.to_address(),
            target: target_addr,
        };
        let packet = Packet {
            buffer: message.buffer.to_vec(),
        };
        self.sender
            .send(NetworkCommand::Send { path, packet })
            .await
            .ok();
    }
}

pub struct Network {
    packet_simulator: PacketSimulator<Packet>,
    address_to_process: HashMap<u8, vsr::ProcessType>,
    process_to_sender: HashMap<vsr::ProcessType, Sender<PooledMessage>>,
    command_receiver: Receiver<NetworkCommand>,
    command_sender: Sender<NetworkCommand>,
    message_pool: MessagePool,
    tick_interval: Interval,
}

impl Network {
    pub fn new(replica_count: u8, client_count: u8, message_pool: MessagePool) -> Self {
        let node_count = replica_count + client_count;
        let options = packet_simulator::Options {
            one_way_delay_mean: 10,
            one_way_delay_min: 1,
            packet_loss_probability: 5,
            packet_replay_probability: 1,
            seed: 1234,
            replica_count,
            node_count,
            partition_mode: PartitionMode::IsolateSingle,
            partition_probability: 1,
            unpartition_probability: 10,
            partition_stability: 200,
            unpartition_stability: 50,
        };

        let (command_sender, command_receiver) = mpsc::channel(256);
        let network_sender_clone = command_sender.clone();

        let delivery_callback = Box::new(move |packet: Packet, path: packet_simulator::Path| {
            network_sender_clone
                .try_send(NetworkCommand::Deliver { path, packet })
                .ok();
        });

        let packet_simulator = PacketSimulator::new(options, delivery_callback);

        Self {
            packet_simulator,
            address_to_process: HashMap::new(),
            process_to_sender: HashMap::new(),
            command_receiver,
            command_sender,
            message_pool,
            tick_interval: time::interval(Duration::from_millis(config::TICK_MS as u64)),
        }
    }

    pub fn register_process(
        &mut self,
        process: vsr::ProcessType,
    ) -> (SimulatedMessageBus, Receiver<PooledMessage>) {
        let address = process.to_address();
        assert!(
            !self.address_to_process.contains_key(&address),
            "Process already registered"
        );

        let (tx, rx) = mpsc::channel(128);

        self.address_to_process.insert(address, process);
        self.process_to_sender.insert(process, tx);

        let bus = SimulatedMessageBus {
            sender: self.command_sender.clone(),
            process_id: process,
        };
        (bus, rx)
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
                _ = self.tick_interval.tick() => {
                    self.packet_simulator.tick();
                }
                Some(command) = self.command_receiver.recv() => {
                    match command {
                        NetworkCommand::Send { path, packet } => {
                            self.packet_simulator.submit_packet(packet, path);
                        }
                        NetworkCommand::Deliver { path, packet } => {
                            if let Some(process) = self.address_to_process.get(&path.target) {
                                if let Some(sender) = self.process_to_sender.get(process) {
                                    let mut pooled_message = self.message_pool.get_message().expect("Network ran out of messages");
                                    pooled_message.buffer[..packet.buffer.len()].copy_from_slice(&packet.buffer);

                                    if sender.send(pooled_message).await.is_err() {
                                         // Receiver was dropped, process likely terminated.
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl vsr::ProcessType {
    fn to_address(&self) -> u8 {
        match self {
            vsr::ProcessType::Replica(id) => *id,
            vsr::ProcessType::Client(id) => (config::REPLICAS_MAX as u128 + *id) as u8,
        }
    }
}
