use crate::services::StateMachine;
use crate::vsr::Operation;

/// A simple state machine that maintains a hash chain of operations.
///
/// This serves as a concrete implementation of the `StateMachine` trait for use
/// in the simulator. Its state is a single `u128` value, which is updated
/// by hashing the previous state with the current operation's input.
#[derive(Debug, Clone)]
pub struct HashingStateMachine {
    state: u128,
}

fn hash(state: u128, input: &[u8]) -> u128 {
    let mut key = [0u8; 32];
    key[..16].copy_from_slice(&state.to_le_bytes());
    let hash = blake3::keyed_hash(&key, input);
    let mut result_bytes = [0u8; 16];
    result_bytes.copy_from_slice(&hash.as_bytes()[..16]);
    u128::from_le_bytes(result_bytes)
}

impl HashingStateMachine {
    pub fn new(seed: u64) -> Self {
        Self {
            state: hash(0, &seed.to_le_bytes()),
        }
    }
}

impl StateMachine for HashingStateMachine {
    fn prepare(&mut self, _realtime: i64, _operation: Operation, _input: &[u8]) {}

    fn commit(
        &mut self,
        client: u128,
        operation: Operation,
        input: &[u8],
        output: &mut [u8],
    ) -> usize {
        match operation {
            Operation::Register => 0,
            Operation::Hash => {
                let client_input = hash(client, input);
                let new_state = hash(self.state, &client_input.to_le_bytes());
                self.state = new_state;

                let state_bytes = self.state.to_le_bytes();
                let output_len = state_bytes.len();
                output[..output_len].copy_from_slice(&state_bytes);

                output_len
            }

            Operation::Reserved | Operation::Init => {
                unreachable!("Reserved/Init operations should not be committed by state machine");
            }
        }
    }
}
