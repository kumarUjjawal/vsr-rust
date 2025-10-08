use crate::config;
use crate::message_pool::{MessagePool, PooledMessage};
use crate::ring_buffer::RingBuffer;
use crate::services::{MessageBus, StateMachine, Storage, TimeSource};
use crate::timeout::Timeout;
use crate::vpr::clock::Clock;
use crate::vpr::journal::Journal;
use crate::vsr::{Command, Header, Operation, sector_ceil};
use log::{debug, error, warn};
use rand::prelude::*;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::sync::Arc;

/// The operational status of the Replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Normal,
    ViewChange,
    Recovering,
}

/// An entry in the client table, caching the latest reply for a client's session.
struct ClientTableEntry {
    session: u64,
    last_request: u32,
    reply: PooledMessage,
}

/// An in-flight prepare operation in the leader's pipeline.
struct Prepare {
    message: PooledMessage,
    ok_from_replicas: QuorumCounter,
    ok_quorum_received: bool,
}

/// A compact bitset for tracking quorum messages from up to 8 replicas.
#[derive(Debug, Default)]
struct QuorumCounter(u8);

impl QuorumCounter {
    fn set(&mut self, replica_id: u8) {
        self.0 |= 1 << replica_id;
    }
    fn is_set(&self, replica_id: u8) -> bool {
        (self.0 >> replica_id) & 1 == 1
    }
    fn count(&self) -> u32 {
        self.0.count_ones()
    }
    fn clear(&mut self) {
        self.0 = 0;
    }
}

/// The core Replica state machine that implements the Viewstamped Replication protocol.
pub struct Replica<MB: MessageBus, SM: StateMachine, S: Storage, T: TimeSource> {
    message_bus: Arc<MB>,
    message_pool: MessagePool,
    journal: Journal<S>,
    clock: Clock<T>,
    state_machine: SM,

    log: BTreeMap<u64, PooledMessage>,
    next_journal_offset: u64,

    cluster: u32,
    replica_count: u8,
    replica_id: u8,
    quorum_replication: u8,
    quorum_view_change: u8,

    status: Status,
    view: u32,
    op: u64,
    commit: u64,

    client_table: HashMap<u128, ClientTableEntry>,
    pipeline: RingBuffer<Prepare, { config::PIPELINING_MAX }>,

    view_normal: u32,
    start_view_change_from_other_replicas: QuorumCounter,
    do_view_change_from_all_replicas: Vec<Option<PooledMessage>>,
    start_view_change_quorum: bool,
    do_view_change_quorum: bool,

    ping_timeout: Timeout,
    prepare_timeout: Timeout,
    commit_timeout: Timeout,
    normal_status_timeout: Timeout,
    view_change_status_timeout: Timeout,

    prng: StdRng,
}

impl<MB, SM, S, T> Replica<MB, SM, S, T>
where
    MB: MessageBus + Debug + 'static,
    SM: StateMachine + Send + 'static,
    S: Storage + Send + Sync + 'static,
    T: TimeSource + Send + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        message_bus: Arc<MB>,
        message_pool: MessagePool,
        storage: Arc<S>,
        time_source: T,
        state_machine: SM,
        cluster: u32,
        replica_count: u8,
        replica_id: u8,
    ) -> Self {
        assert!(replica_count > 0 && (replica_id < replica_count));

        // --- Quorum Size Calculation ---
        let majority = (replica_count / 2) + 1;
        let quorum_replication = std::cmp::min(config::QUORUM_REPLICATION_MAX, majority);
        let quorum_view_change = std::cmp::max(replica_count - quorum_replication + 1, majority);
        assert!(
            quorum_replication + quorum_view_change > replica_count,
            "Flexible quorums must intersect"
        );

        // --- Genesis Log Entry (Op 0) ---
        let mut init_prepare_header = Header {
            cluster,
            command: Command::Prepare,
            operation: Operation::Init,
            size: std::mem::size_of::<Header>() as u32,
            ..Default::default()
        };
        init_prepare_header.set_checksums(&[]);

        // --- Component Initialization ---
        let clock = Clock::new(time_source, replica_count, replica_id);
        let journal = Journal::new(
            storage,
            (config::JOURNAL_SIZE_MAX) as u64,
            16384,
            &init_prepare_header,
        );

        // --- Replica State Initialization ---
        let mut replica = Self {
            message_bus,
            message_pool,
            journal,
            clock,
            state_machine,
            log: BTreeMap::new(),
            next_journal_offset: sector_ceil(init_prepare_header.size as u64),
            cluster,
            replica_count,
            replica_id,
            quorum_replication,
            quorum_view_change,
            status: Status::Normal,
            view: init_prepare_header.view,
            op: init_prepare_header.op,
            commit: init_prepare_header.commit,
            client_table: HashMap::with_capacity(config::CLIENTS_MAX),
            pipeline: RingBuffer::new(),
            view_normal: init_prepare_header.view,
            start_view_change_from_other_replicas: QuorumCounter::default(),
            do_view_change_from_all_replicas: std::iter::repeat_with(|| None)
                .take(replica_count as usize)
                .collect(),
            start_view_change_quorum: false,
            do_view_change_quorum: false,
            ping_timeout: Timeout::new("ping_timeout", replica_id as u128, 100),
            prepare_timeout: Timeout::new("prepare_timeout", replica_id as u128, 50),
            commit_timeout: Timeout::new("commit_timeout", replica_id as u128, 100),
            normal_status_timeout: Timeout::new("normal_status_timeout", replica_id as u128, 500),
            view_change_status_timeout: Timeout::new(
                "view_change_status_timeout",
                replica_id as u128,
                500,
            ),
            prng: StdRng::seed_from_u64(replica_id as u64),
        };

        // --- Start Initial Timeouts ---
        if replica.is_leader() {
            replica.ping_timeout.start();
            replica.commit_timeout.start();
        } else {
            replica.ping_timeout.start();
            replica.normal_status_timeout.start();
        }

        replica
    }

    fn is_leader(&self) -> bool {
        (self.view % self.replica_count as u32) as u8 == self.replica_id
    }

    fn leader_index(&self, view: u32) -> u8 {
        (view % self.replica_count as u32) as u8
    }

    fn advance_journal_offset(&mut self, header: &Header) {
        let end = header.offset + sector_ceil(header.size as u64);
        if end > self.next_journal_offset {
            self.next_journal_offset = end;
        }
    }

    pub async fn tick(&mut self) {
        self.clock.tick();
        self.ping_timeout.tick();
        self.prepare_timeout.tick();
        self.commit_timeout.tick();
        self.normal_status_timeout.tick();
        self.view_change_status_timeout.tick();

        if self.ping_timeout.fired() {
            self.on_ping_timeout().await;
        }
        if self.prepare_timeout.fired() {
            self.on_prepare_timeout().await;
        }
        if self.commit_timeout.fired() {
            self.on_commit_timeout().await;
        }
        if self.normal_status_timeout.fired() {
            self.on_normal_status_timeout().await;
        }
        if self.view_change_status_timeout.fired() {
            self.on_view_change_status_timeout().await;
        }
    }

    pub fn state_machine_hash(&self) -> u128 {
        self.state_machine.state_hash()
    }

    pub fn replica_id(&self) -> u8 {
        self.replica_id
    }

    pub async fn on_message(&mut self, message: PooledMessage) {
        match message.header().command {
            Command::Ping => self.handle_ping(message).await,
            Command::Pong => self.handle_pong(message).await,
            Command::Request => self.handle_request(message).await,
            Command::Prepare => self.handle_prepare(message).await,
            Command::PrepareOk => self.handle_prepare_ok(message).await,
            Command::Commit => self.handle_commit(message).await,
            Command::StartViewChange => self.handle_start_view_change(message).await,
            Command::DoViewChange => self.handle_do_view_change(message).await,
            Command::StartView => self.handle_start_view(message).await,
            Command::RequestStartView => self.handle_request_start_view(message).await,
            _ => {}
        }
    }

    async fn handle_request(&mut self, mut message: PooledMessage) {
        if self.ignore_request_message(&message).await {
            return;
        }

        let request_header = *message.header();
        let request_checksum = request_header.checksum;
        let realtime = match self.clock.realtime_synchronized() {
            Some(ts) => ts,
            None => {
                error!(
                    "replica {}: dropping request {} (clock not synchronized)",
                    self.replica_id, request_checksum
                );
                return;
            }
        };

        debug!(
            "replica {}: on_request: processing request {}",
            self.replica_id, request_checksum
        );

        self.state_machine
            .prepare(realtime, request_header.operation, message.body());

        let parent_checksum = self
            .journal
            .entry_for_op_exact(self.op)
            .map(|header| header.checksum)
            .expect("missing journal entry for current op");
        let op = self.op + 1;

        {
            let prepare_header = message.header_mut();
            prepare_header.command = Command::Prepare;
            prepare_header.view = self.view;
            prepare_header.replica = self.replica_id;
            prepare_header.op = op;
            prepare_header.commit = self.commit;
            prepare_header.parent = parent_checksum;
            prepare_header.context = request_checksum;
            if prepare_header.offset == 0 {
                prepare_header.offset = self.next_journal_offset;
            }
        }
        message.update_checksums();
        self.persist_prepare_message(&mut message).await;

        let log_clone = message.clone_from_pool();
        self.log.insert(op, log_clone);
        let pipeline_was_empty = self.pipeline.is_empty();
        let mut entry = Prepare {
            message,
            ok_from_replicas: QuorumCounter::default(),
            ok_quorum_received: false,
        };
        entry.ok_from_replicas.set(self.replica_id);
        if entry.ok_from_replicas.count() as u8 >= self.quorum_replication {
            entry.ok_quorum_received = true;
        }
        self.pipeline.push_assume_capacity(entry);
        self.op = op;

        if pipeline_was_empty {
            if self.prepare_timeout.ticking {
                self.prepare_timeout.reset();
            } else {
                self.prepare_timeout.start();
            }
        } else if !self.prepare_timeout.ticking {
            self.prepare_timeout.start();
        }

        let mut outbound = Vec::new();
        if let Some(stored) = self.log.get(&op) {
            for replica_id in 0..self.replica_count {
                if replica_id == self.replica_id {
                    continue;
                }
                let mut clone = stored.clone_from_pool();
                clone.header_mut().replica = self.replica_id;
                outbound.push((replica_id, clone));
            }
        }

        for (replica_id, msg) in outbound {
            self.message_bus
                .send_message_to_replica(replica_id, msg)
                .await;
        }

        if self.is_leader() {
            self.commit_ready_prepares().await;
        }
    }

    async fn ignore_request_message(&mut self, message: &PooledMessage) -> bool {
        if self.status != Status::Normal {
            debug!(
                "replica {}: on_request: ignoring ({:?})",
                self.replica_id, self.status
            );
            return true;
        }

        if self.ignore_request_message_follower(message).await {
            return true;
        }
        if self.ignore_request_message_duplicate(message).await {
            return true;
        }
        if self.ignore_request_message_preparing(message) {
            return true;
        }

        false
    }

    async fn ignore_request_message_follower(&mut self, message: &PooledMessage) -> bool {
        let header = *message.header();

        if header.view > self.view {
            debug!(
                "replica {}: on_request: ignoring (newer view)",
                self.replica_id
            );
            return true;
        }

        if self.is_leader() {
            return false;
        }

        if header.operation == Operation::Register {
            debug!(
                "replica {}: on_request: ignoring (follower, register)",
                self.replica_id
            );
            return true;
        }

        if header.view < self.view {
            debug!(
                "replica {}: on_request: forwarding (follower)",
                self.replica_id
            );

            let leader = self.leader_index(self.view);
            let mut forwarded = message.clone_from_pool();
            forwarded.header_mut().replica = self.replica_id;
            forwarded.update_checksums();

            self.message_bus
                .send_message_to_replica(leader, forwarded)
                .await;
        } else {
            debug!(
                "replica {}: on_request: ignoring (follower, same view)",
                self.replica_id
            );
        }

        true
    }

    async fn ignore_request_message_duplicate(&mut self, message: &PooledMessage) -> bool {
        let header = *message.header();
        let client_id = header.client;

        if client_id == 0 {
            error!(
                "replica {}: on_request: ignoring (client id = 0)",
                self.replica_id
            );
            return true;
        }

        if let Some(entry) = self.client_table.get(&client_id) {
            if header.operation != Operation::Register {
                let session = header.context as u64;
                if entry.session > session {
                    error!(
                        "replica {}: on_request: ignoring older session (client bug)",
                        self.replica_id
                    );
                    return true;
                }
                if entry.session < session {
                    error!(
                        "replica {}: on_request: ignoring newer session (client bug)",
                        self.replica_id
                    );
                    return true;
                }
            }

            let reply_header = *entry.reply.header();

            if reply_header.request > header.request {
                debug!(
                    "replica {}: on_request: ignoring older request",
                    self.replica_id
                );
                if header.request < 10 {
                    let req = header.request;
                    let prev_req = reply_header.request;
                    println!(
                        "debug: replica {} ignoring older request {} (> {})",
                        self.replica_id, req, prev_req
                    );
                }
                true
            } else if reply_header.request == header.request {
                if header.checksum == reply_header.parent {
                    let mut reply = entry.reply.clone_from_pool();
                    reply.header_mut().replica = self.replica_id;
                    reply.update_checksums();

                    debug!(
                        "replica {}: on_request: replying to duplicate request",
                        self.replica_id
                    );
                    self.message_bus
                        .send_message_to_client(client_id, reply)
                        .await;
                } else {
                    error!(
                        "replica {}: on_request: request collision (client bug)",
                        self.replica_id
                    );
                }
                true
            } else if reply_header.request + 1 == header.request {
                if header.parent == reply_header.checksum {
                    debug!("replica {}: on_request: new request", self.replica_id);
                    false
                } else {
                    error!(
                        "replica {}: on_request: ignoring new request (client bug)",
                        self.replica_id
                    );
                    if header.request < 10 {
                        let parent = header.parent;
                        let expected = reply_header.checksum;
                        println!(
                            "debug: replica {} new request parent mismatch: got {} expect {}",
                            self.replica_id, parent, expected
                        );
                    }
                    true
                }
            } else {
                error!(
                    "replica {}: on_request: ignoring newer request (client bug)",
                    self.replica_id
                );
                if header.request < 10 {
                    let req = header.request;
                    let prev_req = reply_header.request;
                    println!(
                        "debug: replica {} rejecting request {} with entry request {}",
                        self.replica_id, req, prev_req
                    );
                }
                true
            }
        } else if header.operation == Operation::Register {
            debug!("replica {}: on_request: new session", self.replica_id);
            false
        } else if self.pipeline_prepare_for_client(client_id).is_some() {
            debug!(
                "replica {}: on_request: waiting for session to commit",
                self.replica_id
            );
            true
        } else {
            error!("replica {}: on_request: no session", self.replica_id);
            self.send_eviction_message_to_client(client_id).await;
            true
        }
    }

    fn ignore_request_message_preparing(&self, message: &PooledMessage) -> bool {
        assert!(self.status == Status::Normal);
        assert!(self.is_leader());

        let header = message.header();

        if let Some(prepare) = self.pipeline_prepare_for_client(header.client) {
            let prepare_header = prepare.message.header();
            if header.checksum == prepare_header.context {
                debug!(
                    "replica {}: on_request: ignoring (already preparing)",
                    self.replica_id
                );
            } else {
                error!(
                    "replica {}: on_request: ignoring (client forked)",
                    self.replica_id
                );
            }
            true
        } else if self.pipeline.is_full() {
            debug!(
                "replica {}: on_request: ignoring (pipeline full)",
                self.replica_id
            );
            true
        } else {
            false
        }
    }

    fn pipeline_prepare_for_client(&self, client_id: u128) -> Option<&Prepare> {
        self.pipeline
            .iter()
            .find(|prepare| prepare.message.header().client == client_id)
    }

    async fn send_eviction_message_to_client(&mut self, client_id: u128) {
        warn!(
            "replica {}: too many sessions, sending eviction to client {}",
            self.replica_id, client_id
        );

        let mut message = self
            .message_pool
            .get_message()
            .expect("message pool exhausted");
        {
            let header = message.header_mut();
            header.command = Command::Eviction;
            header.cluster = self.cluster;
            header.replica = self.replica_id;
            header.view = self.view;
            header.client = client_id;
            header.size = std::mem::size_of::<Header>() as u32;
        }
        message.update_checksums();

        self.message_bus
            .send_message_to_client(client_id, message)
            .await;
    }

    async fn handle_prepare(&mut self, mut message: PooledMessage) {
        let header = *message.header();

        if header.op <= self.commit {
            return;
        }

        if self.status == Status::Normal && !self.is_leader() {
            if self.normal_status_timeout.ticking {
                self.normal_status_timeout.reset();
            } else {
                self.normal_status_timeout.start();
            }
        }

        self.op = self.op.max(header.op);
        self.persist_prepare_message(&mut message).await;

        self.log.insert(header.op, message.clone_from_pool());

        let mut ack = self
            .message_pool
            .get_message()
            .expect("message pool exhausted");

        {
            let ack_header = ack.header_mut();
            ack_header.command = Command::PrepareOk;
            ack_header.operation = header.operation;
            ack_header.cluster = self.cluster;
            ack_header.client = header.client;
            ack_header.context = header.checksum;
            ack_header.request = header.request;
            ack_header.view = header.view;
            ack_header.op = header.op;
            ack_header.commit = self.commit;
            ack_header.replica = self.replica_id;
            ack_header.size = std::mem::size_of::<Header>() as u32;
            ack_header.set_checksums(&[]);
        }

        let target = header.replica;
        self.message_bus.send_message_to_replica(target, ack).await;
    }

    async fn handle_prepare_ok(&mut self, message: PooledMessage) {
        if !self.is_leader() {
            return;
        }

        let header = *message.header();

        let op = header.op;

        if let Some(prepare) = self.pipeline.find_mut(|p| p.message.header().op == op) {
            let sender = header.replica;
            if !prepare.ok_from_replicas.is_set(sender) {
                prepare.ok_from_replicas.set(sender);
                if !prepare.ok_quorum_received
                    && prepare.ok_from_replicas.count() as u8 >= self.quorum_replication
                {
                    prepare.ok_quorum_received = true;
                }
            }
        }

        self.commit_ready_prepares().await;
    }

    async fn handle_commit(&mut self, message: PooledMessage) {
        let target_commit = message.header().commit;
        if target_commit <= self.commit {
            return;
        }

        if self.status == Status::Normal && !self.is_leader() {
            if self.normal_status_timeout.ticking {
                self.normal_status_timeout.reset();
            } else {
                self.normal_status_timeout.start();
            }
        }

        self.commit_range(target_commit).await;

        if message.header().op > self.op {
            self.op = message.header().op;
        }
    }

    async fn handle_start_view_change(&mut self, message: PooledMessage) {
        let header = *message.header();
        if header.replica == self.replica_id {
            return;
        }

        if header.view > self.view {
            self.transition_to_view_change_status(header.view).await;
        }

        if self.status != Status::ViewChange || header.view != self.view {
            return;
        }

        if !self
            .start_view_change_from_other_replicas
            .is_set(header.replica)
        {
            self.start_view_change_from_other_replicas
                .set(header.replica);
        }

        let others = self.start_view_change_from_other_replicas.count() as u8;
        let threshold = self.quorum_view_change.saturating_sub(1);
        if !self.start_view_change_quorum && others >= threshold {
            self.start_view_change_quorum = true;
            self.send_do_view_change().await;
        }
    }

    async fn handle_do_view_change(&mut self, message: PooledMessage) {
        let header = *message.header();
        if !self.is_leader() {
            return;
        }

        if header.view > self.view {
            self.transition_to_view_change_status(header.view).await;
        }

        if self.status != Status::ViewChange || header.view != self.view {
            return;
        }

        if let Some(existing) = &mut self.do_view_change_from_all_replicas[header.replica as usize]
        {
            *existing = message;
        } else {
            self.do_view_change_from_all_replicas[header.replica as usize] = Some(message);
        }

        let received = self
            .do_view_change_from_all_replicas
            .iter()
            .flatten()
            .count() as u8;

        if !self.do_view_change_quorum && received >= self.quorum_view_change {
            self.do_view_change_quorum = true;
            let mut best_view = self.view_normal;
            let mut best_op = self.op;
            let mut best_commit = self.commit;
            let mut best_payload = self.clone_outstanding_prepares();

            for msg in self.do_view_change_from_all_replicas.iter().flatten() {
                let candidate_view = msg.header().offset as u32;
                let candidate_commit = msg.header().commit;
                let candidate_prepares = self.prepares_from_payload(msg);
                let candidate_op = candidate_prepares
                    .last()
                    .map(|p| p.header().op)
                    .unwrap_or(candidate_commit);

                if (candidate_view, candidate_op) > (best_view, best_op) {
                    best_view = candidate_view;
                    best_op = candidate_op;
                    best_commit = candidate_commit;
                    best_payload = candidate_prepares;
                }
            }

            self.commit_range(best_commit).await;
            self.op = self.op.max(best_op);
            self.view_normal = best_view;

            self.log.retain(|op, _| *op <= self.commit);
            for mut prepare in best_payload {
                let op = prepare.header().op;
                self.persist_prepare_message(&mut prepare).await;
                if op <= self.commit {
                    self.apply_commit(op, Some(prepare), false).await;
                } else {
                    self.log.insert(op, prepare.clone_from_pool());
                }
            }

            self.commit_range(best_commit).await;

            self.enter_normal_status().await;
            self.send_start_view().await;
        }
    }

    async fn handle_start_view(&mut self, message: PooledMessage) {
        let header = *message.header();
        if header.replica == self.replica_id {
            return;
        }

        if header.view > self.view {
            self.transition_to_view_change_status(header.view).await;
        }

        let target_commit = header.commit;
        let target_op = header.op;
        let mut pending = Vec::new();

        self.log.retain(|op, _| *op <= self.commit);
        let prepares = self.prepares_from_payload(&message);
        for mut prepare in prepares {
            let op = prepare.header().op;
            self.persist_prepare_message(&mut prepare).await;
            if op <= target_commit {
                self.apply_commit(op, Some(prepare), false).await;
            } else {
                pending.push((op, prepare.clone_from_pool()));
            }
        }

        for (op, prepare) in pending {
            self.log.insert(op, prepare);
        }

        if target_commit > self.commit {
            self.commit_range(target_commit).await;
        }

        self.op = self.op.max(target_op);
        self.view = header.view;

        self.enter_normal_status().await;
    }

    async fn handle_request_start_view(&mut self, message: PooledMessage) {
        if !self.is_leader() || self.status != Status::Normal {
            return;
        }

        let prepares = self.clone_outstanding_prepares();
        let header_size = std::mem::size_of::<Header>();
        let mut cursor = header_size;

        let mut reply = self
            .message_pool
            .get_message()
            .expect("message pool exhausted");

        for prepare in &prepares {
            let size = prepare.header().size as usize;
            reply.buffer[cursor..cursor + size].copy_from_slice(&prepare.buffer[..size]);
            cursor += size;
        }

        let header = Header {
            command: Command::StartView,
            cluster: self.cluster,
            replica: self.replica_id,
            view: self.view,
            op: self.op,
            commit: self.commit,
            size: cursor as u32,
            ..Header::default()
        };
        *reply.header_mut() = header;
        reply.update_checksums();

        self.message_bus
            .send_message_to_replica(message.header().replica, reply)
            .await;
    }

    async fn fetch_prepare_from_storage(&self, op: u64) -> Option<PooledMessage> {
        let entry = self.journal.entry_for_op_exact(op)?;
        let mut message = self.message_pool.get_message()?;
        if self
            .journal
            .read_prepare(op, entry.checksum, &mut message)
            .await
            .is_err()
        {
            return None;
        }
        Some(message)
    }

    async fn commit_range(&mut self, target: u64) {
        let mut next = self.commit + 1;
        while next <= target {
            let message_override = if let Some(msg) = self.log.remove(&next) {
                Some(msg)
            } else {
                self.fetch_prepare_from_storage(next).await
            };

            let Some(msg) = message_override else { break };
            self.apply_commit(next, Some(msg), false).await;
            next += 1;
        }
    }

    async fn handle_ping(&mut self, message: PooledMessage) {
        let header = *message.header();

        if header.client != 0 {
            if self.status == Status::Normal {
                let mut pong = self
                    .message_pool
                    .get_message()
                    .expect("message pool exhausted");
                {
                    let pong_header = pong.header_mut();
                    pong_header.command = Command::Pong;
                    pong_header.cluster = self.cluster;
                    pong_header.replica = self.replica_id;
                    pong_header.view = self.view;
                    pong_header.size = std::mem::size_of::<Header>() as u32;
                }
                pong.update_checksums();

                self.message_bus
                    .send_message_to_client(header.client, pong)
                    .await;
            }
            return;
        }

        if header.replica == self.replica_id {
            return;
        }

        if self.status == Status::Normal && !self.is_leader() {
            if self.normal_status_timeout.ticking {
                self.normal_status_timeout.reset();
            } else {
                self.normal_status_timeout.start();
            }
        }

        let realtime = {
            let ts = self.clock.time_source_mut();
            ts.realtime()
        };

        let mut pong = self
            .message_pool
            .get_message()
            .expect("message pool exhausted");
        {
            let pong_header = pong.header_mut();
            pong_header.command = Command::Pong;
            pong_header.cluster = self.cluster;
            pong_header.replica = self.replica_id;
            pong_header.view = self.view;
            pong_header.op = header.op;
            pong_header.offset = u64::from_ne_bytes(realtime.to_ne_bytes());
            pong_header.size = std::mem::size_of::<Header>() as u32;
        }
        pong.update_checksums();

        self.message_bus
            .send_message_to_replica(header.replica, pong)
            .await;
    }

    async fn handle_pong(&mut self, message: PooledMessage) {
        let header = *message.header();

        if header.client != 0 {
            return;
        }

        if header.replica == self.replica_id {
            return;
        }

        let m0 = header.op;
        let t1 = i64::from_ne_bytes(header.offset.to_ne_bytes());
        let m2 = {
            let ts = self.clock.time_source_mut();
            ts.monotonic()
        };

        self.clock.learn(header.replica, m0, t1, m2);
    }

    async fn commit_ready_prepares(&mut self) {
        let mut committed_any = false;
        loop {
            let ready = match self.pipeline.front() {
                Some(prepare) if prepare.ok_quorum_received => Some(prepare.message.header().op),
                _ => None,
            };

            let Some(op) = ready else { break };
            if let Some(prepare) = self.pipeline.pop() {
                self.apply_commit(op, Some(prepare.message), true).await;
                committed_any = true;
            } else {
                break;
            }
        }

        if committed_any && self.pipeline.is_empty() && self.prepare_timeout.ticking {
            self.prepare_timeout.stop();
        }
    }

    async fn apply_commit(
        &mut self,
        op: u64,
        message_override: Option<PooledMessage>,
        reply_to_client: bool,
    ) {
        let prepare_message = match message_override {
            Some(msg) => {
                self.log.remove(&op);
                msg
            }
            None => match self.log.remove(&op) {
                Some(msg) => msg,
                None => return,
            },
        };

        let header = *prepare_message.header();
        if header.operation == Operation::Init {
            self.commit = op;
            return;
        }

        let header_size = std::mem::size_of::<Header>();
        let body_len = header.size.saturating_sub(header_size as u32) as usize;
        let body = &prepare_message.buffer[header_size..][..body_len];

        let mut reply_message = self
            .message_pool
            .get_message()
            .expect("message pool exhausted");
        let output = &mut reply_message.buffer[header_size..];
        let written = self
            .state_machine
            .commit(header.client, header.operation, body, output);

        {
            let reply_header = reply_message.header_mut();
            reply_header.command = Command::Reply;
            reply_header.operation = header.operation;
            reply_header.parent = header.context;
            reply_header.client = header.client;
            reply_header.request = header.request;
            reply_header.cluster = self.cluster;
            reply_header.replica = self.replica_id;
            reply_header.view = self.view;
            reply_header.op = op;
            reply_header.commit = op;
            reply_header.size = (header_size + written) as u32;
        }
        reply_message.update_checksums();

        self.commit = op;

        if op <= 5 {
            println!(
                "replica {} applied commit op {} (reply_to_client={})",
                self.replica_id, op, reply_to_client
            );
        }

        if reply_to_client {
            let stored_reply = reply_message.clone_from_pool();
            match header.operation {
                Operation::Register => {
                    let client_id = header.client;
                    self.client_table.insert(
                        client_id,
                        ClientTableEntry {
                            session: op,
                            last_request: header.request,
                            reply: stored_reply,
                        },
                    );
                }
                _ => {
                    let client_id = header.client;
                    if let Some(entry) = self.client_table.get_mut(&client_id) {
                        entry.last_request = header.request;
                        entry.reply = stored_reply;
                    } else {
                        // Session entry may have been evicted; deliver the reply but do not insert.
                        drop(stored_reply);
                    }
                }
            }

            let client_id = header.client;
            self.message_bus
                .send_message_to_client(client_id, reply_message)
                .await;

            let mut commit_messages = Vec::new();
            for replica_id in 0..self.replica_count {
                if replica_id == self.replica_id {
                    continue;
                }
                let mut commit_msg = self
                    .message_pool
                    .get_message()
                    .expect("message pool exhausted");
                {
                    let commit_header = commit_msg.header_mut();
                    commit_header.command = Command::Commit;
                    commit_header.cluster = self.cluster;
                    commit_header.view = self.view;
                    commit_header.op = op;
                    commit_header.commit = op;
                    commit_header.replica = self.replica_id;
                    commit_header.size = header_size as u32;
                }
                commit_msg.update_checksums();
                commit_messages.push((replica_id, commit_msg));
            }

            for (replica_id, commit_msg) in commit_messages {
                self.message_bus
                    .send_message_to_replica(replica_id, commit_msg)
                    .await;
            }
        }
    }

    fn clone_outstanding_prepares(&self) -> Vec<PooledMessage> {
        let mut messages = Vec::new();
        if self.op <= self.commit {
            return messages;
        }

        for op in (self.commit + 1)..=self.op {
            if let Some(prepare) = self.log.get(&op) {
                messages.push(prepare.clone_from_pool());
            }
        }
        messages
    }

    fn prepares_from_payload(&self, message: &PooledMessage) -> Vec<PooledMessage> {
        let mut result = Vec::new();
        let header_size = std::mem::size_of::<Header>();
        let mut offset = header_size;
        let total = message.header().size as usize;

        while offset + header_size <= total {
            let prepare_header = unsafe { &*(message.buffer[offset..].as_ptr() as *const Header) };
            let size = prepare_header.size as usize;
            if size == 0 || offset + size > total {
                break;
            }

            let mut cloned = self
                .message_pool
                .get_message()
                .expect("message pool exhausted");
            cloned.buffer[..size].copy_from_slice(&message.buffer[offset..offset + size]);
            cloned.update_checksums();
            result.push(cloned);
            offset += size;
        }

        result
    }

    async fn persist_prepare_message(&mut self, message: &mut PooledMessage) {
        if message.header().op > 0 && message.header().offset == 0 {
            message.header_mut().offset = self.next_journal_offset;
        }
        self.advance_journal_offset(message.header());
        let header_copy = *message.header();
        self.journal.set_entry_as_dirty(&header_copy);
        self.journal
            .write_prepare(message)
            .await
            .expect("failed to persist prepare");
    }

    async fn on_ping_timeout(&mut self) {
        self.ping_timeout.reset();

        if self.status != Status::Normal {
            return;
        }

        let monotonic = self.clock.time_source_mut().monotonic();
        let header = Header {
            command: Command::Ping,
            cluster: self.cluster,
            view: self.view,
            op: monotonic,
            ..Header::default()
        };

        self.broadcast_header(header).await;
    }

    async fn on_prepare_timeout(&mut self) {
        if self.status != Status::Normal || !self.is_leader() {
            self.prepare_timeout.reset();
            return;
        }

        let Some(prepare) = self.pipeline.front() else {
            self.prepare_timeout.reset();
            return;
        };

        if prepare.ok_quorum_received {
            self.prepare_timeout.reset();
            self.commit_ready_prepares().await;
            return;
        }

        let mut to_send = Vec::new();
        for replica_id in 0..self.replica_count {
            if replica_id == self.replica_id {
                continue;
            }
            if !prepare.ok_from_replicas.is_set(replica_id) {
                to_send.push((replica_id, prepare.message.clone_from_pool()));
            }
        }

        if to_send.is_empty() {
            self.prepare_timeout.reset();
            return;
        }

        self.prepare_timeout.backoff(&mut self.prng);

        for (replica_id, message) in to_send {
            self.message_bus
                .send_message_to_replica(replica_id, message)
                .await;
        }
    }

    async fn on_commit_timeout(&mut self) {
        self.commit_timeout.reset();

        if self.status != Status::Normal || !self.is_leader() {
            return;
        }

        let header = Header {
            command: Command::Commit,
            cluster: self.cluster,
            view: self.view,
            op: self.op,
            commit: self.commit,
            ..Header::default()
        };

        self.broadcast_header(header).await;
    }

    async fn on_normal_status_timeout(&mut self) {
        self.normal_status_timeout.reset();
    }

    async fn on_view_change_status_timeout(&mut self) {
        self.view_change_status_timeout.reset();
    }

    async fn broadcast_header(&self, mut header: Header) {
        header.replica = self.replica_id;
        header.size = std::mem::size_of::<Header>() as u32;
        header.set_checksums(&[]);

        let mut messages = Vec::new();
        for replica_id in 0..self.replica_count {
            if replica_id == self.replica_id {
                continue;
            }
            let mut message = self
                .message_pool
                .get_message()
                .expect("message pool exhausted");
            *message.header_mut() = header;
            message.update_checksums();
            messages.push((replica_id, message));
        }

        for (replica_id, message) in messages {
            self.message_bus
                .send_message_to_replica(replica_id, message)
                .await;
        }
    }

    async fn transition_to_view_change_status(&mut self, new_view: u32) {
        if new_view <= self.view {
            return;
        }

        self.view = new_view;
        self.status = Status::ViewChange;
        self.ping_timeout.stop();
        self.commit_timeout.stop();
        self.normal_status_timeout.stop();
        self.view_change_status_timeout.start();

        self.reset_pipeline();
        self.reset_view_change_tracking();

        self.send_start_view_change().await;
    }

    fn reset_pipeline(&mut self) {
        while self.pipeline.pop().is_some() {}
    }

    fn reset_view_change_tracking(&mut self) {
        self.start_view_change_from_other_replicas.clear();
        for slot in &mut self.do_view_change_from_all_replicas {
            if let Some(msg) = slot.take() {
                drop(msg);
            }
        }
        self.start_view_change_quorum = false;
        self.do_view_change_quorum = false;
    }

    fn rebuild_pipeline_from_log(&mut self) {
        self.pipeline.clear();
        if !self.is_leader() {
            return;
        }
        if self.op <= self.commit {
            return;
        }

        for op in (self.commit + 1)..=self.op {
            if let Some(prepare) = self.log.get(&op) {
                let mut entry = Prepare {
                    message: prepare.clone_from_pool(),
                    ok_from_replicas: QuorumCounter::default(),
                    ok_quorum_received: false,
                };
                entry.ok_from_replicas.set(self.replica_id);
                let _ = self.pipeline.push(entry);
            }
        }
    }

    async fn send_prepare_oks_after_view_change(&mut self) {
        if self.is_leader() {
            return;
        }
        if self.op <= self.commit {
            return;
        }

        let leader = self.leader_index(self.view);
        for op in (self.commit + 1)..=self.op {
            if let Some(prepare) = self.log.get(&op) {
                let mut ack = self
                    .message_pool
                    .get_message()
                    .expect("message pool exhausted");
                {
                    let ack_header = ack.header_mut();
                    ack_header.command = Command::PrepareOk;
                    ack_header.operation = prepare.header().operation;
                    ack_header.cluster = self.cluster;
                    ack_header.client = prepare.header().client;
                    ack_header.context = prepare.header().checksum;
                    ack_header.request = prepare.header().request;
                    ack_header.view = self.view;
                    ack_header.op = op;
                    ack_header.commit = self.commit;
                    ack_header.replica = self.replica_id;
                    ack_header.size = std::mem::size_of::<Header>() as u32;
                }
                ack.update_checksums();
                self.message_bus.send_message_to_replica(leader, ack).await;
            }
        }
    }

    async fn send_start_view_change(&mut self) {
        let header = Header {
            command: Command::StartViewChange,
            cluster: self.cluster,
            view: self.view,
            ..Default::default()
        };
        self.broadcast_header(header).await;
    }

    async fn send_do_view_change(&mut self) {
        self.start_view_change_quorum = true;

        let prepares = self.clone_outstanding_prepares();
        let header_size = std::mem::size_of::<Header>();
        let mut cursor = header_size;

        let mut message = self
            .message_pool
            .get_message()
            .expect("message pool exhausted");

        for prepare in &prepares {
            let size = prepare.header().size as usize;
            message.buffer[cursor..cursor + size].copy_from_slice(&prepare.buffer[..size]);
            cursor += size;
        }

        let leader = self.leader_index(self.view);
        *message.header_mut() = Header {
            command: Command::DoViewChange,
            cluster: self.cluster,
            view: self.view,
            op: self.op,
            commit: self.commit,
            offset: self.view_normal as u64,
            replica: self.replica_id,
            size: cursor as u32,
            ..Default::default()
        };
        message.update_checksums();

        let stored = message.clone_from_pool();
        self.do_view_change_from_all_replicas[self.replica_id as usize] = Some(stored);

        self.message_bus
            .send_message_to_replica(leader, message)
            .await;
    }

    async fn send_start_view(&mut self) {
        let prepares = self.clone_outstanding_prepares();

        let header_size = std::mem::size_of::<Header>();
        let mut cursor = header_size;

        let mut message = self
            .message_pool
            .get_message()
            .expect("message pool exhausted");

        for prepare in &prepares {
            let size = prepare.header().size as usize;
            message.buffer[cursor..cursor + size].copy_from_slice(&prepare.buffer[..size]);
            cursor += size;
        }

        *message.header_mut() = Header {
            command: Command::StartView,
            cluster: self.cluster,
            view: self.view,
            op: self.op,
            commit: self.commit,
            replica: self.replica_id,
            size: cursor as u32,
            ..Default::default()
        };
        message.update_checksums();

        let mut messages = Vec::new();
        for replica in 0..self.replica_count {
            if replica == self.replica_id {
                continue;
            }
            let clone = message.clone_from_pool();
            messages.push((replica, clone));
        }

        for (replica, msg) in messages {
            self.message_bus.send_message_to_replica(replica, msg).await;
        }
    }

    async fn enter_normal_status(&mut self) {
        self.status = Status::Normal;
        self.view_normal = self.view;
        self.view_change_status_timeout.stop();
        self.reset_view_change_tracking();

        if self.is_leader() {
            self.ping_timeout.start();
            self.commit_timeout.start();
            self.rebuild_pipeline_from_log();
        } else {
            self.normal_status_timeout.start();
            self.send_prepare_oks_after_view_change().await;
        }
    }
}
