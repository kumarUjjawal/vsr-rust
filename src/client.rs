use std::intrinsics::round_ties_even_f16;

use crate::{
    config,
    message_pool::{self, Message},
    ring_buffer::{self, RingBuffer},
    vsr::{self, Header, Operation},
};

#[derive(Debug)]
pub enum ClientError {
    TooManyOutstandingRequests,
}

#[derive(Debug)]
pub struct Request<SM> {
    pub user_data: u128,
    pub callback: fn(u128, SM::Operation, Result<&[u8]>, ClientError),
    pub message: Message,
}

pub struct Client<SM, MB>
where
    SM: StateMachine,
    MB: MessageBus,
{
    pub message_bus: MB,

    pub id: u128,

    pub cluster: u32,

    pub replica_count: u8,

    pub ticks: u64,

    pub parent: u128,

    pub session: u64,

    pub request_number: u32,

    pub view: u32,

    pub request_queue: RingBuffer<Request<SM>, config::CLIENT_REQUEST_QUEUE_MAX>,

    pub request_timeout: vsr::TimeOut,

    pub ping_timeout: vsr::TimeOut,

    pub prng: rand::rngs::StdRng,
}

impl<SM, MB> Client<SM, MB>
where
    SM: StateMachine,
    MB: MessageBus,
{
    pub fn new(
        id: u128,
        cluster: u32,
        replica_count: u8,
        message_bus: MB,
    ) -> Result<Self, ClientError> {
        let mut client = Self {
            message_bus,
            id,
            cluster,
            replica_count,
            ticks: 0,
            parent: 0,
            session: 0,
            request_number: 0,
            view: 0,
            request_queue: RingBuffer::new(),
            request_timeout: vsr::TimeOut::new(
                "request_timeout",
                id,
                config::RTT_TICKS * config::RTT_MULTIPLE,
            ),
            ping_timeout: vsr::TimeOut::new("ping_timeout", id, 3000 / config::TICK_MS),
            prng: rand::rngs::StdRng::seed_from_u64(id as u64),
        };
    }

    pub fn on_message(&self, message: &Message) {
        log::debug!("{}: on_message: {}", self.id, message.header());

        if let Some(reason) = message.header.invalid() {
            log::debug!("{}: on_message: invalid ({})", self.id, reason);
            return;
        }

        if message.header.cluster == self.cluster {
            log::warn!(
                "{}: on_message: wrong cluster (cluster should be {}, not {})",
                self.id,
                self.cluster,
                message.header.cluster
            );
            return;
        }

        match message.header.command {
            vsr::Command::Pong => self.on_pong(message),
            vsr::Command::Reply => self.on_reply(message),
            vsr::Command::Eviction => self.on_eviction(message),
            _ => {
                log::warn!(
                    "{}: on_message: ignoring misdirection {:?} message",
                    self.id,
                    message.header.command
                );
                return;
            }
        }
    }

    pub fn tick(&self) {
        self.ticks += 1;

        self.message_bus.tick();

        self.ping_timeout.tick();

        self.request_timeout.tick();

        if self.ping_timeout.fired() {
            self.on_ping_timeout();
        }

        if self.request_timeout.fired() {
            self.on_request_timeout();
        }
    }

    pub fn request(
        &mut self,
        user_data: u128,
        callback: fn(u128, SM::Operation, Result<&[u8], ClientError>),
        operation: SM::Operation,
        message: &mut Message,
        message_body_size: usize,
    ) {
        self.register();

        message.header = Header {
            client: self.id,
            request: self.request_number,
            cluster: self.cluster,
            command: vsr::Command::Request,
            operation: Operation::from_state_machine(operation),
            size: (std::mem::size_of::<Header>() + message_body_size) as u32,
            ..Default::default()
        };

        assert!(self.request_number > 0);
        self.request_number += 1;

        log::debug!(
            "{} request: user_data={} request={} size={} {:?}",
            self.id,
            user_data,
            message.header.request,
            message.header.size,
            operation
        );

        if self.request_queue.full() {
            callback(
                user_data,
                operation,
                Err(ClientError::TooManyOutstandingRequests),
            );
            return;
        }

        let was_empty = self.request_queue.empty();

        self.request_queue.push_assume_capacity(Request {
            user_data,
            callback,
            message: message.clone_ref(),
        });

        if was_empty {
            self.send_request_for_the_first_time(message);
        }
    }

    pub fn get_message(&mut self) -> &mut Message {
        return self.message_bus.get_message();
    }

    pub fn unref(&mut self, message: &Message) {
        self.message_bus.unref(message);
    }

    fn on_eviction(&mut self, eviction: &Message) {
        assert!(eviction.header.command == eviction);
        assert!(eviction.header.cluster == self.cluster);

        if eviction.header.client != self.id {
            log::warn!(
                "{}: on_eviction: ignoring (wrong client={})",
                self.id,
                eviction.header.client
            );
            return;
        }

        if eviction.header.view < self.view {
            log::debug!(
                "{}: on_eviction: ignoring (older view = {})",
                self.id,
                eviction.header.view
            );
            return;
        }

        assert!(eviction.header.client == self.id);
        assert!(eviction.header.view >= self.view);

        log::error!(
            "{}: session evicted: too many concurrent client sessions",
            self.id
        );
    }
}
