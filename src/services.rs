use crate::message_pool::PooledMessage;
use crate::vsr::Operation;
use async_trait::async_trait;
use std::fmt::Debug;

/// A trait representing the application-specific state machine.
///
/// This is the Rust equivalent of the `StateMachine` comptime parameter in Zig.
/// The `Replica` will call these methods to apply committed operations.
pub trait StateMachine: Send + Sync {
    /// Called by the leader to inform the state machine of a potential operation,
    /// allowing it to perform any necessary pre-processing or validation
    fn prepare(&mut self, realtime: i64, operation: Operation, input: &[u8]);

    /// Applies a committed operation to the state machine and writes the result
    /// into the output buffer
    /// Returns the number of bytes written to the output buffer
    fn commit(
        &mut self,
        client: u128,
        operation: Operation,
        input: &[u8],
        output: &mut [u8],
    ) -> usize;
}

#[derive(Debug)]
pub struct StorageError(pub String);

#[async_trait]
pub trait Storage: Send + Sync {
    async fn read_sectors(&self, buffer: &mut [u8], offset: u64) -> Result<(), StorageError>;

    async fn write_sectors(&self, buffer: &[u8], offset: u64) -> Result<(), StorageError>;
}

pub trait MessageBus: Send + Sync + Debug {
    fn send_message_to_replica(&self, replica_id: u8, message: PooledMessage);

    fn send_message_to_client(&self, client_id: u128, message: PooledMessage);
}

/// This abstracts away system time, allowing for deterministic time during testing,
pub trait TimeSource: Send + Sync {
    fn monotonic(&self) -> u64;

    fn realtime(&self) -> i64;

    fn tick(&mut self);
}
