use crate::{config, vsr::Header};
use std::mem;

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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProcessType {
    Replica,
    Client,
}

pub struct Message {
    header: Header,
    buffer: Vec<u8>,
    references: u32,
    next: Option<Box<Message>>,
}

impl Message {
    pub fn reference_message(mut message: Message) -> Message {
        message.references += 1;
        message
    }

    fn with_capacity(capacity: usize) -> Self {
        const HEADER_SIZE: usize = 128;

        let mut buffer = vec![0u8; capacity];
        let header = Header::default();

        let header_bytes = header_to_bytes(&header);
        buffer[..HEADER_SIZE].copy_from_slice(&header_bytes);

        Message {
            header,
            buffer,
            references: 0,
            next: None,
        }
    }

    pub fn add_ref(&mut self) -> &mut Self {
        self.references += 1;
        self
    }

    pub fn body(&self) -> &[u8] {
        const HEADER_SIZE: usize = 128;

        let total_size = self.header.size as usize;

        let end_pos = total_size.min(self.buffer.len());
        if end_pos <= HEADER_SIZE {
            return &[];
        }

        &self.buffer[HEADER_SIZE..end_pos]
    }

    pub fn body_mut(&mut self) -> &mut [u8] {
        const HEADER_SIZE: usize = 128;

        let total_size = self.header.size as usize;
        let end_pos = total_size.min(self.buffer.len());
        if end_pos <= HEADER_SIZE {
            return &mut [];
        }

        &mut self.buffer[HEADER_SIZE..end_pos]
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn header_mut(&mut self) -> &mut Header {
        &mut self.header
    }

    pub fn set_body_size(&mut self, body_len: usize) {
        const HEADER_SIZE: usize = 128;
        self.header.size = (HEADER_SIZE + body_len) as u32;

        let header_bytes = header_to_bytes(&self.header);
        self.buffer[..HEADER_SIZE].copy_from_slice(&header_bytes);
    }

    pub fn total_size(&self) -> u32 {
        self.header.size
    }

    pub fn reference_count(&self) -> u32 {
        self.references
    }

    pub fn sync_header_from_buffer(&mut self) -> Result<(), &'static str> {
        const HEADER_SIZE: usize = 128;
        if self.buffer.len() > HEADER_SIZE {
            return Err("Buffer too small to contain header");
        }

        self.header = bytes_to_header(&self.buffer[..HEADER_SIZE])?;
        Ok(())
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    pub fn buffer_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
    }
}

pub struct MessagePool {
    /// List of currently unused messages of MESSAGE_SIZE_MAX_PADDED
    free_list: Option<Box<Message>>,
}

impl MessagePool {
    /// Initialize the message pool for the specified process type
    pub fn new(process_type: ProcessType) -> Result<Self, &'static str> {
        let messages_max = match process_type {
            ProcessType::Replica => MESSAGES_MAX_REPLICA,
            ProcessType::Client => MESSAGES_MAX_CLIENT,
        };

        let mut pool = MessagePool { free_list: None };

        // Pre-allocate all messages
        for _ in 0..messages_max {
            let message = Box::new(Message::with_capacity(MESSAGE_SIZE_MAX_PADDED));

            // Add to free list
            let mut boxed_message = message;
            boxed_message.next = pool.free_list.take();
            pool.free_list = Some(boxed_message);
        }

        Ok(pool)
    }

    /// Get an unused message with a buffer of MESSAGE_SIZE_MAX.
    /// The returned message has exactly one reference.
    pub fn get_message(&mut self) -> Option<Box<Message>> {
        if let Some(mut message) = self.free_list.take() {
            self.free_list = message.next.take();

            // Reset message state
            message.references = 1;
            message.next = None;

            // Clear buffer in debug mode for security
            if cfg!(debug_assertions) {
                message.buffer.fill(0);
                let header = Header::default();
                let header_bytes = header_to_bytes(&header);
                message.buffer[..128].copy_from_slice(&header_bytes);
                message.header = header;
            }

            Some(message)
        } else {
            None // Pool exhausted
        }
    }

    /// Decrement the reference count of the message, possibly freeing it.
    pub fn unref(&mut self, mut message: Box<Message>) {
        message.references = message.references.saturating_sub(1);

        if message.references == 0 {
            // Clear buffer in debug mode for security
            if cfg!(debug_assertions) {
                message.buffer.fill(0);
            }

            // Return to free list
            message.next = self.free_list.take();
            self.free_list = Some(message);
        }
        // If references > 0, the message is still in use elsewhere
        // In the original Zig code, this would be handled by the caller
        // keeping track of the message pointer
    }
}

// Safe serialization functions with minimal unsafe code
fn header_to_bytes(header: &Header) -> [u8; 128] {
    // This is safe because Header is repr(C, packed) and has no padding
    unsafe { mem::transmute(*header) }
}

fn bytes_to_header(bytes: &[u8]) -> Result<Header, &'static str> {
    if bytes.len() < 128 {
        return Err("Not enough bytes for header");
    }

    let mut header_bytes = [0u8; 128];
    header_bytes.copy_from_slice(&bytes[..128]);

    // This is safe because we're transmuting from [u8; 128] to Header
    // and Header is repr(C, packed) with exactly 128 bytes
    let header: Header = unsafe { mem::transmute(header_bytes) };
    Ok(header)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vsr::Command;
    use crate::vsr::Operation;

    #[test]
    fn test_header_size() {
        assert_eq!(mem::size_of::<Header>(), 128);
    }

    #[test]
    fn test_message_pool_creation() {
        let mut pool = MessagePool::new(ProcessType::Client).unwrap();

        // Should be able to get a message
        let msg = pool.get_message();
        assert!(msg.is_some());

        if let Some(msg) = msg {
            assert_eq!(msg.reference_count(), 1);
            assert_eq!(msg.total_size(), 128); // Just header initially
        }
    }

    #[test]
    fn test_message_body() {
        let mut pool = MessagePool::new(ProcessType::Client).unwrap();
        let mut msg = pool.get_message().unwrap();

        // Set some body data
        let body = msg.body_mut();
        assert_eq!(body.len(), 0); // No body initially

        msg.set_body_size(100);
        let body = msg.body();
        assert_eq!(body.len(), 100);
    }

    #[test]
    fn test_header_serialization() {
        let header = Header {
            checksum: 0x123456789abcdef0,
            request: 42,
            cluster: 1,
            command: Command::Request,
            operation: Operation::Init,
            ..Default::default()
        };

        let bytes = header_to_bytes(&header);
        let deserialized = bytes_to_header(&bytes).unwrap();

        // Copy values to avoid packed field reference issues
        let orig_checksum = header.checksum;
        let orig_request = header.request;
        let orig_cluster = header.cluster;
        let orig_command = header.command;
        let orig_operation = header.operation;

        let deser_checksum = deserialized.checksum;
        let deser_request = deserialized.request;
        let deser_cluster = deserialized.cluster;
        let deser_command = deserialized.command;
        let deser_operation = deserialized.operation;

        assert_eq!(orig_checksum, deser_checksum);
        assert_eq!(orig_request, deser_request);
        assert_eq!(orig_cluster, deser_cluster);
        assert_eq!(orig_command, deser_command);
        assert_eq!(orig_operation, deser_operation);
    }
}
