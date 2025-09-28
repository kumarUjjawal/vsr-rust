use crate::message_pool::{Message, PooledMessage};
use crate::{message_pool::MessagePool, sim::network::Network, vsr::ProcessType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Process {
    Replica(u8),
    Client(u128),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageBus {
    network: Network,
    pool: MessagePool,

    cluster: u32,
    process: Process,
    //TODO
    //callback for when a message is received
}

impl MessageBus {
    pub fn new(cluster: u32, process: Process, network: &Network) -> MessageBus {
        MessageBus {
            network: network,
            pool: MessagePool::new(process),
            cluster: cluster,
            process: process,
        }
    }

    pub fn set_on_message(bus: MessageBus) -> Message {}
}
