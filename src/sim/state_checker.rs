use crate::config;
use crate::ring_buffer::RingBuffer;
use crate::sim::cluster::Cluster;
use crate::sim::state_machine::HashingStateMachine;
use std::collections::HashMap;

/// A queue to track the hash of each in-flight client request.
pub type RequestQueue = RingBuffer<u128, { config::CLIENT_REQUEST_QUEUE_MAX }>;

/// Verifies that all replicas in the cluster maintain state machine consensus.
pub struct StateChecker {
    /// A queue for each client tracking the hash of its in-flight requests.
    pub client_requests: Vec<RequestQueue>,
    /// The last known state for each replica's state machine.
    state_machine_states: Vec<u128>,
    /// A history of all canonical states reached by the cluster to detect regressions.
    history: HashMap<u128, u64>,
    /// The highest canonical state reached by the cluster.
    state: u128,
    /// The number of times the canonical state has transitioned.
    pub transitions: u64,
}

impl StateChecker {
    pub fn new(cluster: &Cluster) -> Self {
        let state = cluster.replicas[0].state_machine_hash();
        let mut state_machine_states = vec![0; cluster.replicas.len()];

        for (i, replica) in cluster.replicas.iter().enumerate() {
            assert_eq!(
                replica.state_machine_hash(),
                state,
                "All replicas must start with the same state"
            );
            state_machine_states[i] = state;
        }

        let mut history = HashMap::new();
        history.insert(state, 0);

        Self {
            client_requests: std::iter::repeat_with(RequestQueue::new)
                .take(cluster.clients.len())
                .collect(),
            state_machine_states,
            history,
            state,
            transitions: 0,
        }
    }

    /// Checks the state of a replica. Panics if the state is invalid or divergent.
    pub fn check_state(&mut self, replica_id: u8, cluster: &Cluster) {
        let replica_state = cluster.replicas[replica_id as usize].state_machine_hash();
        let previous_replica_state = self.state_machine_states[replica_id as usize];

        if replica_state == previous_replica_state {
            return; // No change
        }

        self.state_machine_states[replica_id as usize] = replica_state;

        if self.history.contains_key(&replica_state) {
            return;
        }

        for queue in &mut self.client_requests {
            if let Some(input_hash) = queue.front() {
                let expected_next_state =
                    HashingStateMachine::hash(self.state, &input_hash.to_le_bytes());
                if replica_state == expected_next_state {
                    self.state = replica_state;
                    self.transitions += 1;
                    self.history.insert(self.state, self.transitions);
                    queue.pop();
                    return;
                }
            }
        }

        panic!(
            "Replica {} transitioned to an invalid state: {:#x}",
            replica_id, replica_state
        );
    }

    pub fn convergence(&self) -> bool {
        self.state_machine_states.windows(2).all(|w| w[0] == w[1])
    }
}
