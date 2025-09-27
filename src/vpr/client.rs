use crate::{
    config,
    message_pool::Message,
    ring_buffer::RingBuffer,
    sim::state_machine::{Operation, StateMachine},
    vsr::{self, Header},
};
use crate::services::MessageBus;
use std::mem;

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

    pub request_queue: RingBuffer<Request<SM>, {config::CLIENT_REQUEST_QUEUE_MAX}>,

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

    fn on_pong(&mut self, pong: &mut Message) {
        assert!(pong.header.command == pong);
        assert!(pong.header.cluster == self.cluster);

        if pong.header != 0 {
            log::debug!("{}: on_pong: ignoring (client != 0)", self.id);
            return;
        }

        if pong.header.view > self.view {
            log::debug!(
                "{}: on_pong: newer view={}..{}",
                self.id,
                self.view,
                pong.header.view
            );
            self.view = pong.header.view;
        }

        self.register()
    }

    fn on_reply(&mut self, reply: &mut Message) {
        assert!(reply.header.valid_checksum());
        assert!(reply.header.valid_checksum_body(reply.body()));

        if reply.header.client != self.id {
            log::debug!(
                "{}: on_reply: ignoring (wrong client = {})",
                self.id,
                reply.header.client
            );
            return;
        }

        if let Some(inflight) = self.request_queue.head_ptr() {
            if reply.header.request < inflight.messsage.header.request {
                log::debug!(
                    "{}: on_reply: ignoring (wrong client={}",
                    self.id,
                    self.header.client
                );
                return;
            }
        } else {
            log::debug!("{}: on_reply: ignoring (on inflight request)", self.id);
            return;
        }

        let inflight = self.request_queue.pop().unwrap();

        log::debug!(
            "{}: on_reply: user_data={} request={} size={}",
            self.id,
            inflight.user_data,
            reply.header.request,
            reply.header.size,
        );

        assert!(reply.header.parent == self.parent);
        assert!(reply.header.client == self.id);
        assert!(reply.header.context == 0);
        assert!(reply.header.request == inflight.message.header.request);
        assert!(reply.header.cluster == self.cluster);
        assert!(reply.header.op == reply.header.commit);
        assert!(reply.header.operation == inflight.message.header.operation);

        self.parent = reply.header.checksum;

        if reply.header.view > self.view {
            log::debug!(
                "{}: on_reply: newer view={}..{}",
                self.id,
                self.view,
                reply.header.view
            );
            self.view = reply.header.view;
        }

        self.request_timeout.stop();

        if inflight.message.header.operation == Operation::Register {
            assert!(self.session == 0);
            assert!(reply.header.commit > 0);
            self.session = reply.header.commit;
        }
    }

    fn on_ping_timeout(&mut self) {
        self.ping_timeout.reset();

        let ping = Header {
            command: self.ping,
            cluster: self.cluster,
            client: self.id,
            ..Default::default()
        };

        self.send_header_to_replicas(ping);
    }

    fn on_request_timeout(&mut self) {
        self.request_timeout.backoff(self.prng.random());

        let message = self.request_queue.head_ptr().message;
        assert!(message.header.command == request);
        assert!(message.header.request <= self.request_number);
        assert!(message.header.checksum == self.parent);
        assert!(message.header.context == self.session);

        log::debug!(
            "{}: on_request_timeout: resending request={} checksum={}",
            self.id,
            message.header.request,
            message.header.checksum
        );

        self.send_message_to_replica(
            self.view + self.request_timeout.attempt % self.replica_count,
            message,
        );
    }

    fn create_mesage_from_header(&mut self, header: Header) -> &mut Message {
        assert!(header.client == self.id);
        assert!(header.cluster == self.cluster);
        assert!(header.size == mem::size_of::<Header>);

        let message = self.message_bus.pool.get_message();

        message.header = header;
        message.header.set_checksum_body(message.body());
        message.header.set_checksume();

        return message;
    }

    fn register(&mut self) {
        if self.request_number > 0 {
            return;
        }

        let message = self.message_bus.get_message();

        message.header = {
            self.client  = self.id;
            self.request = self.request_number;
            self.cluster = self.cluster;
            self.command = request;
            self.operation = register;
        };

        assert!(self.request_number == 0);
        self.request_number += 1;

        log::debug!("{}: register: registering a session with the cluster", self.id);

        assert!(self.request_queue.empty());

        self.request_queue.push_assume_capacity() {
            user_data = 0,
            callback = undefined,
            message = message.as_ref(),
        }

        self.send_request_for_the_first_time(message);
    }

    fn send_header_to_replica(&mut self, replica: u8, header: Header) {
        let message = self.create_message_from_header(header);
        self.send_message_to_replica(replica, message);
    }

    fn send_header_to_replicas(&mut self, header: Header) {
        let message = self.create_message_from_header(header);

        let mut replica: u8 = 0;
        while replica < self.replica_count {
            replica += 1;
            self.send_message_to_replica(replica, message);
        }
    }

    fn send_message_to_replica(&mut self, replica: u8, message: &Message) {
        log::debug!("{}: sending {:?} to replica {}: {}", self.id, message.header.command, replica, message.header);

        assert!(replica < self.replica_count);
        assert!(message.header.valid_checksum());
        assert!(message.header.client == self.id);
        assert!(message.header.cluster == self.cluster);

        self.message_bus.send_message_to_replica(replica, message);
    }


    fn send_request_for_the_first_time(&mut self, message: &Message) {
        assert!(self.request_queue.head_ptr().message == message);

        message.header.parent = self.parent;
        message.header.context = self.session;

        message.header.view = self.view;
        message.header.set_checksum_body(message.body());
        message.header.set_checksum();

        self.parent = message.header.checksum;

        log::debug!("{}: send_request_for_the_first_time: request={} checksum={}", self.id, message.header.request, message.header.checksum);

        assert!(!self.request_timeout.ticking);
        self.request_timeout.start();

        self.send_message_to_replica(self.view % self.replica_count, message);
    }

}
