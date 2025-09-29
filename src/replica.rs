use crate::config;
use crate::message_pool::{MessagePool, PooledMessage};
use crate::ring_buffer::RingBuffer;
use crate::services::{MessageBus, StateMachine, Storage, TimeSource};
use crate::timeout::Timeout;
use crate::vpr::clock::Clock;
use crate::vpr::journal::Journal;
use crate::vsr::{Command, Header, Operation};
use rand::prelude::*;
use std::collections::HashMap;
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
    // Core Dependencies (Generic over Traits)
    message_bus: Arc<MB>,
    message_pool: MessagePool,
    journal: Journal<S>,
    clock: Clock<T>,
    state_machine: SM,

    // Replica Configuration
    cluster: u32,
    replica_count: u8,
    replica_id: u8,
    quorum_replication: u8,
    quorum_view_change: u8,

    // Protocol State
    status: Status,
    view: u32,
    op: u64,
    commit: u64,

    // Leader-Specific State
    client_table: HashMap<u128, ClientTableEntry>,
    pipeline: RingBuffer<Prepare, { config::PIPELINING_MAX }>,

    // View Change State
    view_normal: u32, // The last view in which status was Normal.
    start_view_change_from_other_replicas: QuorumCounter,
    do_view_change_from_all_replicas: Vec<Option<PooledMessage>>,
    start_view_change_quorum: bool,
    do_view_change_quorum: bool,

    // Timeouts
    ping_timeout: Timeout,
    prepare_timeout: Timeout,
    commit_timeout: Timeout,
    normal_status_timeout: Timeout,
    view_change_status_timeout: Timeout,

    // Miscellaneous
    prng: StdRng,
}

impl<MB, SM, S, T> Replica<MB, SM, S, T>
where
    MB: MessageBus + Debug + 'static,
    SM: StateMachine + Send + 'static,
    S: Storage + Send + Sync + 'static,
    T: TimeSource + Send + 'static,
{
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
            do_view_change_from_all_replicas: vec![None; replica_count as usize],
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

    /// Helper function to determine if this replica is the leader in the current view.
    fn is_leader(&self) -> bool {
        (self.view % self.replica_count as u32) as u8 == self.replica_id
    }
}
