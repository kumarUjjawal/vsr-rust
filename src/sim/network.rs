use crate::message_pool::{MessagePool, PooledMessage};
use crate::services::MessageBus;
use crate::sim::packet_simulator::{self, PacketSimulator};
use crate::vsr;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::yield_now;

#[derive(Clone)]
pub struct NetworkOptions {
    pub packet_simulator: packet_simulator::Options,
}

#[derive(Debug, Clone)]
pub struct Packet {
    pub buffer: Vec<u8>,
}

#[derive(Debug)]
enum NetworkCommand {
    Send {
        source: vsr::ProcessType,
        target: vsr::ProcessType,
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
        let packet = Packet {
            buffer: message.buffer.to_vec(),
        };
        println!(
            "bus {:?} sending to replica {}",
            self.process_id, replica_id
        );
        self.sender
            .send(NetworkCommand::Send {
                source: self.process_id,
                target: vsr::ProcessType::Replica(replica_id),
                packet,
            })
            .await
            .ok();
    }

    async fn send_message_to_client(&self, client_id: u128, message: PooledMessage) {
        let packet = Packet {
            buffer: message.buffer.to_vec(),
        };
        self.sender
            .send(NetworkCommand::Send {
                source: self.process_id,
                target: vsr::ProcessType::Client(client_id),
                packet,
            })
            .await
            .ok();
    }
}

pub struct Network {
    packet_simulator: PacketSimulator<Packet>,
    address_to_process: HashMap<u8, vsr::ProcessType>,
    process_to_sender: HashMap<vsr::ProcessType, Sender<PooledMessage>>,
    process_to_address: HashMap<vsr::ProcessType, u8>,
    command_receiver: Receiver<NetworkCommand>,
    command_sender: Sender<NetworkCommand>,
    message_pool: MessagePool,
}

impl Network {
    pub fn new(message_pool: MessagePool, options: NetworkOptions) -> Self {
        let (command_sender, command_receiver) = mpsc::channel(256);
        let network_sender_clone = command_sender.clone();

        let delivery_callback = Box::new(move |packet: Packet, path: packet_simulator::Path| {
            network_sender_clone
                .try_send(NetworkCommand::Deliver { path, packet })
                .ok();
        });

        let packet_simulator = PacketSimulator::new(options.packet_simulator, delivery_callback);

        Self {
            packet_simulator,
            address_to_process: HashMap::new(),
            process_to_sender: HashMap::new(),
            process_to_address: HashMap::new(),
            command_receiver,
            command_sender,
            message_pool,
        }
    }

    pub fn register_process(
        &mut self,
        process: vsr::ProcessType,
    ) -> (SimulatedMessageBus, Receiver<PooledMessage>) {
        let address = self.address_to_process.len() as u8;
        assert!(
            !self.process_to_address.contains_key(&process),
            "Process already registered"
        );

        let (tx, rx) = mpsc::channel(128);

        self.address_to_process.insert(address, process);
        self.process_to_sender.insert(process, tx);
        self.process_to_address.insert(process, address);

        let bus = SimulatedMessageBus {
            sender: self.command_sender.clone(),
            process_id: process,
        };
        (bus, rx)
    }

    pub async fn run(&mut self) {
        loop {
            match self.command_receiver.try_recv() {
                Ok(command) => match command {
                    NetworkCommand::Send {
                        source,
                        target,
                        packet,
                    } => {
                        if let (Some(&source_addr), Some(&target_addr)) = (
                            self.process_to_address.get(&source),
                            self.process_to_address.get(&target),
                        ) {
                            let path = packet_simulator::Path {
                                source: source_addr,
                                target: target_addr,
                            };
                            self.packet_simulator.submit_packet(packet, path);
                        }
                    }
                    NetworkCommand::Deliver { path, packet } => {
                        if let Some(process) = self.address_to_process.get(&path.target)
                            && let Some(sender) = self.process_to_sender.get(process)
                        {
                            let mut pooled_message = self
                                .message_pool
                                .get_message()
                                .expect("Network ran out of messages");
                            pooled_message.buffer[..packet.buffer.len()]
                                .copy_from_slice(&packet.buffer);

                            let _ = sender.send(pooled_message).await;
                        }
                    }
                },
                Err(TryRecvError::Empty) => {
                    self.packet_simulator.tick();
                    yield_now().await;
                }
                Err(TryRecvError::Disconnected) => break,
            }
        }
    }
}
