use crate::{config, vsr::Header};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

const MESSAGE_SIZE_MAX_PADDED: usize = config::MESSAGE_SIZE_MAX + config::SECTOR_SIZE;

pub const MESSAGES_MAX_REPLICA: usize = {
    let mut sum = 0;

    sum += config::IO_DEPTH_READ + config::IO_DEPTH_WRITE; // Journal I/O
    sum += config::CLIENTS_MAX; // Replica.client_table
    sum += 1; // Replica.loopback_queue
    sum += config::PIPELINING_MAX; // Replica.pipeline
    sum += config::REPLICAS_MAX; // Replica.do_view_change_from_all_replicas quorum
    sum += config::CONNECTION_MAX; // Connection.recv_message
    sum += config::CONNECTION_MAX * config::CONNECTION_SEND_QUEUE_MAX_REPLICA; // Connection.send_queue
    sum += 1; // Handle bursts (e.g. Connection.parse_message)
    sum += 1; // Handle Replica.commit_op's reply
    sum += 20; // Network simulator allows up to 20 messages for path_capacity_max

    sum
};

pub const MESSAGES_MAX_CLIENT: usize = {
    let mut sum = 0;

    sum += config::REPLICAS_MAX; // Connection.recv_message
    sum += config::REPLICAS_MAX * config::CONNECTION_SEND_QUEUE_MAX_CLIENT; // Connection.send_queue
    sum += config::CLIENT_REQUEST_QUEUE_MAX; // Client.request_queue
    sum += 1; // Handle bursts
    sum += 20; // Network simulator

    sum
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessType {
    Replica,
    Client,
}

#[repr(align(4096))]
pub struct Message {
    pub buffer: [u8; MESSAGE_SIZE_MAX_PADDED],
}

impl Message {
    fn new() -> Self {
        Message {
            buffer: [0; MESSAGE_SIZE_MAX_PADDED],
        }
    }

    pub fn header(&self) -> &Header {
        unsafe { &*(self.buffer.as_ptr() as *const Header) }
    }

    pub fn header_mut(&mut self) -> &mut Header {
        unsafe { &mut *(self.buffer.as_mut_ptr() as *mut Header) }
    }

    pub fn body(&self) -> &[u8] {
        let header_size = std::mem::size_of::<Header>();
        let total_size = self.header().size as usize;
        let body_size = total_size.saturating_sub(header_size);
        &self.buffer[header_size.header_size + body_size]
    }

    pub fn body_mut(&mut self) -> &mut [u8] {
        let header_size = std::mem::size_of::<Header>();
        let total_size = self.header().size as usize;
        let body_size = total_size.saturating_sub(header_size);
        &mut self.buffer[header_size.header_size + body_size]
    }

    pub fn update_checksums(&mut self) {
        // First, we create the body slice. The immutable borrow for this ends immediately.
        let body_slice = {
            let header_size = std::mem::size_of::<Header>();
            let total_size = self.header().size as usize;
            let body_size = total_size.saturating_sub(header_size);
            &self.buffer[header_size..header_size + body_size]
        };

        // Calculate the body checksum with no active borrows on `self`.
        let body_checksum = Header::calculate_checksum_body(body_slice);

        // Now, create a mutable borrow of the header to update it.
        let header = self.header_mut();
        header.checksum_body = body_checksum;

        // The `calculate_checksum` method on Header borrows the header, not the whole message.
        header.checksum = header.calculate_checksum();
    }
}
/// A smart pointer that provides access to a `Message` from a `MessagePool`.
///
/// When a `PooledMessage` is dropped, it automatically returns the underlying `Message`
/// back to the pool for reuse, preventing new memory allocations.
pub struct PooledMessage {
    // This must not be dropped normally, as we are returning it to the pool.
    pub message: std::mem::ManuallyDrop<Box<Message>>,
    pub pool: MessagePool,
}

impl Deref for PooledMessage {
    type Target = Message;
    fn deref(&self) -> &Self::Target {
        &self.message
    }
}

impl DerefMut for PooledMessage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.message
    }
}

impl Drop for PooledMessage {
    // Returns the message to its pool when dropped.
    fn drop(&mut self) {
        // Safely take the message out of the ManuallyDrop wrapper.
        let message = unsafe { std::mem::ManuallyDrop::take(&mut self.message) };
        self.pool.release(message);
    }
}

/// Inner state of the MessagePool, managed by a Mutex for thread safety.
struct MessagePoolInner {
    /// A list of pre-allocated message ready for use.
    free_list: Vec<Box<Message>>,
}

/// A thread-safe pool of pre-allocated, reference-counted `Message` objects.
///
/// This structure avoids frequent memory allocations in high-throughput code paths
/// by reusing a fixed number of `Message` buffers. It is designed to be cloned
/// and shared across multiple threads.
#[derive(Clone)]
pub struct MessagePool {
    inner: Arc<Mutex<MessagePoolInner>>,
}

impl MessagePool {
    /// Initialize the message pool for the specified process type
    pub fn new(process_type: ProcessType) -> Self {
        let messages_max = match process_type {
            ProcessType::Replica => MESSAGES_MAX_REPLICA,
            ProcessType::Client => MESSAGES_MAX_CLIENT,
        };

        let mut free_list = Vec::with_capacity(messages_max);

        for _ in 0..messages_max {
            free_list.push(Box::new(Message::new()));
        }

        Self {
            inner: Arc::new(Mutex::new(MessagePoolInner { free_list })),
        }
    }

    /// Retrieves a message from the pool, if one is available.
    ///
    /// Returns a `PooledMessage` smart pointer. When this pointer goes out of scope,
    /// the message is automatically returned to the pool. Returns `None` if the pool is exhausted.
    pub fn get_message(&self) -> Option<PooledMessage> {
        let mut inner = self.inner.lock().unwrap();

        inner.free_list.pop().map(|mut message| {
            *message.header_mut() = Header::default();

            PooledMessage {
                message: std::mem::ManuallyDrop::new(message),
                pool: self.clone(),
            }
        })
    }

    /// Returns a message to the pool's free list.
    /// This is automatically called by `PooledMessage`'s `Drop` implementation.
    fn release(&self, message: Box<Message>) {
        let mut inner = self.inner.lock().unwrap();
        inner.free_list.push(message);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn test_message_struct_layout() {
        assert_eq!(mem::size_of::<Message>(), MESSAGE_SIZE_MAX_PADDED);
        assert_eq!(mem::align_of::<Message>(), config::SECTOR_SIZE);
    }

    #[test]
    fn test_pool_creation_and_exhaustion() {
        let pool = MessagePool::new(ProcessType::Client);
        let mut messages = Vec::new();

        // Exhaust the pool
        for _ in 0..MESSAGES_MAX_CLIENT {
            messages.push(pool.get_message().expect("Pool should have messages"));
        }

        // Pool should now be empty
        assert!(pool.get_message().is_none());

        // Drop one message, returning it to the pool
        messages.pop();

        // Should be able to get one message again
        let msg = pool.get_message();
        assert!(msg.is_some());
        assert!(pool.get_message().is_none());
    }

    #[test]
    fn test_message_header_and_body_access() {
        let pool = MessagePool::new(ProcessType::Replica);
        let mut msg = pool.get_message().unwrap();

        // Check initial state
        assert_eq!(msg.header().size as usize, mem::size_of::<Header>());
        assert_eq!(msg.body().len(), 0);

        // Modify header
        msg.header_mut().op = 42;
        msg.header_mut().size = (mem::size_of::<Header>() + 100) as u32;

        // Verify changes
        assert_eq!(msg.body().len(), 100);

        // Modify body
        msg.body_mut().copy_from_slice(&[7u8; 100]);
        assert_eq!(msg.body()[0], 7);
    }

    #[test]
    fn test_message_is_returned_to_pool_on_drop() {
        let pool = MessagePool::new(ProcessType::Replica);

        {
            let _msg1 = pool.get_message();
            let _msg2 = pool.get_message();
            // Both messages are dropped here
        }

        // Should be able to get two messages again
        let msg1 = pool.get_message();
        let msg2 = pool.get_message();
        assert!(msg1.is_some());
        assert!(msg2.is_some());
    }
}
