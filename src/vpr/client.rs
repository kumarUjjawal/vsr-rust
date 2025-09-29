use crate::config;
use crate::message_pool::MessagePool;
use crate::message_pool::{Message, PooledMessage};
use crate::ring_buffer::RingBuffer;
use crate::services::MessageBus;
use crate::timeout::Timeout;
use crate::vsr::{Command, Header, Operation};
use rand::prelude::*;
use std::fmt::Debug;
use std::sync::Arc;

pub type RequestCallback = Box<dyn FnOnce(u128, Operation, Result<&[u8], ClientError>) + Send>;

/// An item in the client's internal request queue.
struct Request {
    user_data: u128,
    callback: RequestCallback,
    message: PooledMessage,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum ClientError {
    TooManyOutstandingRequests,
}

pub struct Client<MB: MessageBus> {
    message_bus: Arc<MB>,
    message_pool: MessagePool,
    id: u128,
    cluster: u32,
    replica_count: u8,
    ticks: u64,
    parent: u128,
    session: u64,
    request_number: u32,
    view: u32,
    request_queue: RingBuffer<Request, { config::CLIENT_REQUEST_QUEUE_MAX }>,
    request_timeout: Timeout,
    ping_timeout: Timeout,
    prng: StdRng,
}

impl<MB: MessageBus + Debug + 'static> Client<MB> {
    pub fn new(
        message_bus: Arc<MB>,
        message_pool: MessagePool,
        id: u128,
        cluster: u32,
        replica_count: u8,
    ) -> Self {
        assert!(id > 0, "Client ID must not be zero");
        assert!(replica_count > 0, "Replica count must be positive");

        let mut client = Self {
            message_bus,
            message_pool,
            id,
            cluster,
            replica_count,
            ticks: 0,
            parent: 0,
            session: 0,
            request_number: 0,
            view: 0,
            request_queue: RingBuffer::new(),
            request_timeout: Timeout::new(
                "request_timeout",
                id,
                (config::RTT_TICKS * config::RTT_MULTIPLE) as u64,
            ),
            ping_timeout: Timeout::new("ping_timeout", id, 30000 / config::TICK_MS as u64),
            prng: StdRng::seed_from_u64(id as u64),
        };
        client.ping_timeout.start();
        client
    }

    pub fn id(&self) -> u128 {
        self.id
    }

    pub fn request(
        &mut self,
        user_data: u128,
        callback: RequestCallback,
        operation: Operation,
        mut message: PooledMessage,
        message_body_size: usize,
    ) {
        self.register_if_needed();

        {
            let header = message.header_mut();
            header.client = self.id;
            header.request = self.request_number;
            header.cluster = self.cluster;
            header.command = Command::Request;
            header.operation = operation;
            header.size = (std::mem::size_of::<Header>() + message_body_size) as u32;
        }

        self.request_number += 1;

        if self.request_queue.is_full() {
            callback(
                user_data,
                operation,
                Err(ClientError::TooManyOutstandingRequests),
            );
            return;
        }

        let was_empty = self.request_queue.is_empty();
        self.request_queue.push_assume_capacity(Request {
            user_data,
            callback,
            message,
        });

        if was_empty {
            let mut network_message = self.message_pool.get_message().unwrap();
            network_message
                .buffer
                .copy_from_slice(&self.request_queue.front().unwrap().message.buffer);
            self.send_request_for_the_first_time(network_message);
        }
    }

    pub fn tick(&mut self) {
        self.ticks += 1;
        self.ping_timeout.tick();
        self.request_timeout.tick();

        if self.ping_timeout.fired() {
            self.on_ping_timeout();
        }
        if self.request_timeout.fired() {
            self.on_request_timeout();
        }
    }

    pub fn on_message(&mut self, message: PooledMessage) {
        if message.header().invalid().is_some() {
            return;
        }
        if message.header().cluster != self.cluster {
            return;
        }

        match message.header().command {
            Command::Pong => self.on_pong(&message),
            Command::Reply => self.on_reply(message),
            Command::Eviction => self.on_eviction(&message),
            _ => {}
        }
    }

    fn on_eviction(&mut self, eviction: &Message) {
        if eviction.header().client != self.id {
            return;
        }
        if eviction.header().view < self.view {
            return;
        }
        panic!(
            "Session evicted: client ID {} was evicted by the cluster.",
            self.id
        );
    }

    fn on_pong(&mut self, pong: &Message) {
        if pong.header().client != 0 {
            return;
        }
        if pong.header().view > self.view {
            self.view = pong.header().view;
        }
        self.register_if_needed();
    }

    fn on_reply(&mut self, reply_message: PooledMessage) {
        let reply_header = *reply_message.header();

        if reply_header.client != self.id {
            return;
        }

        let inflight_request_number = match self.request_queue.front() {
            Some(req) => req.message.header().request,
            None => return,
        };

        if reply_header.request < inflight_request_number {
            return;
        }

        let inflight = self.request_queue.pop().unwrap();

        assert!(reply_header.is_checksum_valid());
        assert!(reply_header.is_checksum_body_valid(reply_message.body()));
        assert!(reply_header.parent == self.parent);
        assert!(reply_header.request == inflight.message.header().request);
        assert!(reply_header.operation == inflight.message.header().operation);

        self.parent = reply_header.checksum;
        if reply_header.view > self.view {
            self.view = reply_header.view;
        }

        self.request_timeout.stop();

        if inflight.message.header().operation == Operation::Register {
            assert!(self.session == 0);
            assert!(reply_header.commit > 0);
            self.session = reply_header.commit;
        }

        if let Some(next_request) = self.request_queue.front() {
            let mut network_message = self.message_pool.get_message().unwrap();
            network_message
                .buffer
                .copy_from_slice(&next_request.message.buffer);
            self.send_request_for_the_first_time(network_message);
        }

        if inflight.message.header().operation != Operation::Register {
            (inflight.callback)(
                inflight.user_data,
                inflight.message.header().operation,
                Ok(reply_message.body()),
            );
        }
    }

    fn on_ping_timeout(&mut self) {
        self.ping_timeout.reset();
        let mut message = self.message_pool.get_message().expect("Pool exhausted");
        let header = message.header_mut();
        header.command = Command::Ping;
        header.cluster = self.cluster;
        header.client = self.id;
        header.set_checksums(&[]);

        for i in 0..self.replica_count {
            let mut network_message = self.message_pool.get_message().expect("Pool exhausted");
            network_message.buffer.copy_from_slice(&message.buffer);
            let _ = self
                .message_bus
                .send_message_to_replica(i, network_message);
        }
    }

    fn on_request_timeout(&mut self) {
        self.request_timeout.backoff(&mut self.prng);

        if let Some(inflight) = self.request_queue.front() {
            let replica_index =
                (self.view as u8 + self.request_timeout.attempts) % self.replica_count;

            let mut network_message = self.message_pool.get_message().expect("Pool exhausted");
            network_message
                .buffer
                .copy_from_slice(&inflight.message.buffer);

            network_message.update_checksums();

            let _ = self
                .message_bus
                .send_message_to_replica(replica_index, network_message);
        }
    }

    fn send_request_for_the_first_time(&mut self, mut network_message: PooledMessage) {
        {
            let header = network_message.header_mut();
            header.parent = self.parent;
            header.context = self.session as u128;
            header.view = self.view;
        }
        network_message.update_checksums();

        self.parent = network_message.header().checksum;
        self.request_timeout.start();

        let leader_index = (self.view % self.replica_count as u32) as u8;
        let _ = self
            .message_bus
            .send_message_to_replica(leader_index, network_message);
    }

    fn register_if_needed(&mut self) {
        if self.request_number > 0 {
            return;
        }

        let mut message = self.message_pool.get_message().expect("Pool exhausted");

        {
            let header = message.header_mut();
            header.client = self.id;
            header.request = self.request_number; // Will be 0
            header.cluster = self.cluster;
            header.command = Command::Request;
            header.operation = Operation::Register;
            header.size = std::mem::size_of::<Header>() as u32;
        }

        self.request_number += 1;
        assert!(self.request_queue.is_empty());
        let was_empty = self.request_queue.is_empty();

        self.request_queue.push_assume_capacity(Request {
            user_data: 0,
            callback: Box::new(|_, _, _| {}),
            message,
        });

        if was_empty {
            let mut network_message = self.message_pool.get_message().unwrap();
            network_message
                .buffer
                .copy_from_slice(&self.request_queue.front().unwrap().message.buffer);
            self.send_request_for_the_first_time(network_message);
        }
    }
}
