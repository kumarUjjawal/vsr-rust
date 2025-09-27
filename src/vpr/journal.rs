use crate::config;
use crate::message_pool::PooledMessage;
use crate::services::{Storage, StorageError};
use crate::vsr::{self, Header};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct BitSet {
    bits: Vec<bool>,
    len: u64,
}

impl BitSet {
    pub fn new(count: usize) -> Self {
        Self {
            bits: vec![false; count],
            len: 0,
        }
    }

    pub fn set(&mut self, op: u64) {
        if !self.bits[op as usize] {
            self.bits[op as usize] = true;
            self.len += 1;
        }
    }

    pub fn clear(&mut self, op: u64) {
        if self.bits[op as usize] {
            self.bits[op as usize] = false;
            self.len -= 1;
        }
    }

    pub fn is_set(&self, op: u64) -> bool {
        self.bits[op as usize]
    }
}

/// Manages the persistent log of operations (the journal).
///
/// The Journal is responsible for writing and reading protocol messages to and from
/// the underlying storage medium, managing metadata, and ensuring data integrity.
pub struct Journal<S: Storage> {
    storage: Arc<S>,
    size_headers: u64,
    size_circular_buffer: u64,
    headers: Vec<Header>,
    dirty: BitSet,
    faulty: BitSet,
    headers_version: u8,
    write_lock: Mutex<()>,
}

impl<S: Storage> Journal<S> {
    pub fn new(storage: Arc<S>, size: u64, headers_count: u32, init_prepare: &Header) -> Self {
        let headers_per_sector = config::SECTOR_SIZE / std::mem::size_of::<Header>();
        assert!(headers_per_sector > 0);
        assert!(headers_count as usize >= headers_per_sector);

        let header_copies = 2;
        let size_headers = (headers_count as usize) * std::mem::size_of::<Header>() as u64;
        let size_headers_copies = size_headers * header_copies;
        assert!(size_headers_copies < size);
        let size_circular_buffer = size - size_headers_copies;

        let mut headers = vec![Header::default(); headers_count as usize];
        headers[0] = *init_prepare;

        Self {
            storage,
            size_headers,
            size_circular_buffer,
            headers,
            dirty: BitSet::new(headers_count as usize),
            faulty: BitSet::new(headers_count as usize),
            headers_version: 0,
            write_lock: Mutex::new(()),
        }
    }

    pub fn entry_for_op(&self, op: u64) -> Option<&Header> {
        let existing = &self.headers[op as usize];
        if existing.command == vsr::Command::Reserved {
            None
        } else {
            Some(existing)
        }
    }

    pub fn entry_for_op_exact(&self, op: u64) -> Option<&Header> {
        self.entry_for_op(op).filter(|&h| h.op == op)
    }

    pub fn set_entry_as_dirty(&mut self, header: &Header) {
        self.headers[header.op as usize] = *header;
        self.dirty.set(header.op);
    }

    pub fn has_clean(&self, header: &Header) -> bool {
        if let Some(existing) = self.entry_for_op_exact(header.op) {
            existing.checksum == header.checksum
                && !self.dirty.is_set(header.op)
                && !self.faulty.is_set(header.op)
        } else {
            false
        }
    }

    pub async fn read_prepare(
        &self,
        op: u64,
        checksum: u128,
        msg: &mut PooledMessage,
    ) -> Result<(), StorageError> {
        let entry = self
            .entry_for_op_exact(op)
            .filter(|h| h.checksum == checksum)
            .ok_or_else(|| StorageError("Entry not found or checksum mismatch".into()))?;

        if self.has_clean(entry) {
            let physical_size = vsr::sector_ceil(entry.size as u64);
            let offset = self.offset_in_circular_buffer(entry.offset);

            let buffer = &mut msg.buffer[..physical_size as usize];
            self.storage.read_sectors(buffer, offset).await?;

            if !msg.header().is_checksum_valid() || !msg.header().is_checksum_body_valid(msg.body())
            {
                return Err(StorageError("Corrupted entry on read".into()));
            }

            Ok(())
        } else {
            Err(StorageError("Cannot ready dirty of faulty entry".inot()))
        }
    }

    pub async fn write_prepare(&mut self, message: &PooledMessage) -> Result<(), StorageError> {
        let _lock = self.write_lock.lock().await;

        let header = *message.header();
        assert!(header.command == vsr::Command::Prepare);

        if !self.dirty.is_set(header.op) {
            return Ok(());
        }

        let sectors = &message.buffer[..vsr::sector_ceil(header.size as u64) as usize];
        let circular_offset = self.offset_in_circular_buffer(header.offset);
        self.storage.write_sectors(sectors, circular_offset).await?;

        let header_sector_offset =
            vsr::sector_floor(header.op * std::mem::size_of::<Header>() as u64);

        let mut temp_header_buffer: Vec<u8> = vec![0; config::SECTOR_SIZE];
        let start = header_sector_offset as usize;
        let headers_as_bytes = unsafe {
            std::slice::from_raw_parts(
                self.headers.as_ptr() as *const u8,
                self.header.len() * std::mem::size_of::<Header>(),
            )
        };

        temp_header_buffer.copy_from_slice(&headers_as_bytes[start..start + config::SECTOR_SIZE]);

        self.header_version = (self.header_version + 1) & 2;
        let version1 = self.header.version;
        self.headers_version = (self.headers_version + 1) % 2;
        let version2 = self.headers_version;

        let redundant_offset1 = self.offset_in_headers_version(header_sector_offset, version1);
        self.storage
            .write_sectors(&temp_header_buffer, redundant_offset1)
            .await?;

        let redundant_offset2 = self.offset_in_headers_version(header_sector_offset, version2);
        self.storage
            .write_sectors(&temp_header_buffer, redundant_offset2)
            .await?;

        self.dirty.clear(header.op);
        self.faulty.clear(header.op);

        Ok(())
    }

    fn offset_in_circular_buffer(&self, offset: u64) -> u64 {
        assert!(offset < self.size_circular_buffer);
        self.size_headers + offset
    }

    fn offset_in_headers_version(&self, offset: u64, version: u8) -> u64 {
        assert!(offset < self.size_headers);
        if version == 0 {
            offset
        } else {
            self.size_headers + self.size_circular_buffer + offset
        }
    }
}
