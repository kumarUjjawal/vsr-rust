use crate::config;
use std::mem;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessType {
    Replica(u8),
    Client(u128),
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Command {
    Reserved = 0,
    Ping,
    Pong,
    Request,
    Prepare,
    PrepareOk,
    Reply,
    Commit,
    StartViewChange,
    DoViewChange,
    StartView,
    Recovery,
    RecoveryResponse,
    RequestStartView,
    RequestHeaders,
    RequestPrepare,
    Headers,
    NackPrepare,
    Eviction,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Reserved = 0,
    Init = 1,
    Register = 2,
    Hash = 3,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub checksum: u128,
    pub checksum_body: u128,
    pub parent: u128,
    pub client: u128,
    pub context: u128,
    pub request: u32,
    pub cluster: u32,
    pub epoch: u32,
    pub view: u32,
    pub op: u64,
    pub commit: u64,
    pub offset: u64,
    pub size: u32, // Size of header + body
    pub replica: u8,
    pub command: Command,
    pub operation: Operation,
    pub version: u8,
}

impl Default for Header {
    fn default() -> Self {
        Self {
            checksum: 0,
            checksum_body: 0,
            parent: 0,
            client: 0,
            context: 0,
            request: 0,
            cluster: 0,
            epoch: 0,
            view: 0,
            op: 0,
            commit: 0,
            offset: 0,
            size: mem::size_of::<Header>() as u32, // Initially just header size
            replica: 0,
            command: Command::Reserved,
            operation: Operation::Reserved,
            version: 0,
        }
    }
}

impl Header {
    const _HEADER_SIZE_CHECK: () = assert!(mem::size_of::<Header>() == 128);

    /// Calculate the checksum for the header itself
    pub fn calculate_checksum(&self) -> u128 {
        let mut temp = *self;
        temp.checksum = 0;
        let bytes: &[u8] = unsafe {
            std::slice::from_raw_parts((&temp as *const Self) as *const u8, mem::size_of::<Self>())
        };

        let hash: [u8; 16] = blake3::hash(bytes).as_bytes()[..16].try_into().unwrap();
        u128::from_le_bytes(hash)
    }

    pub fn set_checksums(&mut self, body: &[u8]) {
        self.checksum_body = Self::calculate_checksum_body(body);
        self.checksum = self.calculate_checksum();
    }

    pub fn calculate_checksum_body(body: &[u8]) -> u128 {
        let hash: [u8; 16] = blake3::hash(body).as_bytes()[..16].try_into().unwrap();
        u128::from_le_bytes(hash)
    }

    pub fn is_checksum_valid(&self) -> bool {
        self.checksum == self.calculate_checksum()
    }

    pub fn is_checksum_body_valid(&self, body: &[u8]) -> bool {
        self.checksum_body == Self::calculate_checksum_body(body)
    }

    /// Validates the header fields against the rules for its command type.
    /// Returns `None` if valid, or a `Some(&'static str)` with the reason if invalid.
    /// This should be called only after `is_checksum_valid()` passes.
    pub fn invalid(&self) -> Option<&'static str> {
        if self.version != 0 {
            return Some("version != Version");
        }
        if self.size < mem::size_of::<Header>() as u32 {
            return Some("size < @sizeOf(Header)");
        }
        if self.epoch != 0 {
            return Some("epoch != 0");
        }

        match self.command {
            Command::Reserved => {
                if self.parent != 0 {
                    return Some("parent != 0");
                }
                if self.client != 0 {
                    return Some("client != 0");
                }
                if self.context != 0 {
                    return Some("context != 0");
                }
                if self.request != 0 {
                    return Some("request != 0");
                }
                if self.cluster != 0 {
                    return Some("cluster != 0");
                }
                if self.view != 0 {
                    return Some("view != 0");
                }
                if self.op != 0 {
                    return Some("op != 0");
                }
                if self.commit != 0 {
                    return Some("commit != 0");
                }
                if self.offset != 0 {
                    return Some("offset != 0");
                }
                if self.replica != 0 {
                    return Some("replica != 0");
                }
                if self.operation != Operation::Reserved {
                    return Some("operation != .reserved");
                }
            }
            Command::Ping => {
                if self.parent != 0 {
                    return Some("parent != 0");
                }
                if self.context != 0 {
                    return Some("context != 0");
                }
                if self.request != 0 {
                    return Some("request != 0");
                }
                if self.commit != 0 {
                    return Some("commit != 0");
                }
                if self.offset != 0 {
                    return Some("offset != 0");
                }
                if self.operation != Operation::Reserved {
                    return Some("operation != .reserved");
                }
            }
            Command::Pong => {
                if self.parent != 0 {
                    return Some("parent != 0");
                }
                if self.client != 0 {
                    return Some("client != 0");
                }
                if self.context != 0 {
                    return Some("context != 0");
                }
                if self.request != 0 {
                    return Some("request != 0");
                }
                if self.commit != 0 {
                    return Some("commit != 0");
                }
                if self.operation != Operation::Reserved {
                    return Some("operation != .reserved");
                }
            }
            Command::Request => {
                if self.client == 0 {
                    return Some("client == 0");
                }
                if self.op != 0 {
                    return Some("op != 0");
                }
                if self.commit != 0 {
                    return Some("commit != 0");
                }
                if self.offset != 0 {
                    return Some("offset != 0");
                }
                if self.replica != 0 {
                    return Some("replica != 0");
                }
                match self.operation {
                    Operation::Reserved => return Some("operation == .reserved"),
                    Operation::Init => return Some("operation == .init"),
                    Operation::Register => {
                        if self.parent != 0 {
                            return Some("parent != 0");
                        }
                        if self.context != 0 {
                            return Some("context != 0");
                        }
                        if self.request != 0 {
                            return Some("request != 0");
                        }
                        if self.size != mem::size_of::<Header>() as u32 {
                            return Some("size != @sizeOf(Header)");
                        }
                    }
                    _ => {
                        // All other operations
                        if self.context == 0 {
                            return Some("context == 0");
                        }
                        if self.request == 0 {
                            return Some("request == 0");
                        }
                    }
                }
            }
            Command::Prepare => {
                match self.operation {
                    Operation::Reserved => return Some("operation == .reserved"),
                    Operation::Init => {
                        if self.parent != 0 {
                            return Some("init: parent != 0");
                        }
                        if self.client != 0 {
                            return Some("init: client != 0");
                        }
                        if self.context != 0 {
                            return Some("init: context != 0");
                        }
                        if self.request != 0 {
                            return Some("init: request != 0");
                        }
                        if self.view != 0 {
                            return Some("init: view != 0");
                        }
                        if self.op != 0 {
                            return Some("init: op != 0");
                        }
                        if self.commit != 0 {
                            return Some("init: commit != 0");
                        }
                        if self.offset != 0 {
                            return Some("init: offset != 0");
                        }
                        if self.size != mem::size_of::<Header>() as u32 {
                            return Some("init: size != @sizeOf(Header)");
                        }
                        if self.replica != 0 {
                            return Some("init: replica != 0");
                        }
                    }
                    _ => {
                        // All other operations
                        if self.client == 0 {
                            return Some("client == 0");
                        }
                        if self.op == 0 {
                            return Some("op == 0");
                        }
                        if self.op <= self.commit {
                            return Some("op <= commit");
                        }
                        if self.operation == Operation::Register {
                            if self.request != 0 {
                                return Some("request != 0");
                            }
                        } else if self.request == 0 {
                            return Some("request == 0");
                        }
                    }
                }
            }
            Command::PrepareOk => {
                if self.size != mem::size_of::<Header>() as u32 {
                    return Some("size != @sizeOf(Header)");
                }
                // Logic is otherwise identical to Prepare
                match self.operation {
                    Operation::Reserved => return Some("operation == .reserved"),
                    Operation::Init => {
                        if self.parent != 0 {
                            return Some("init: parent != 0");
                        }
                        if self.client != 0 {
                            return Some("init: client != 0");
                        }
                        if self.context != 0 {
                            return Some("init: context != 0");
                        }
                        if self.request != 0 {
                            return Some("init: request != 0");
                        }
                        if self.view != 0 {
                            return Some("init: view != 0");
                        }
                        if self.op != 0 {
                            return Some("init: op != 0");
                        }
                        if self.commit != 0 {
                            return Some("init: commit != 0");
                        }
                        if self.offset != 0 {
                            return Some("init: offset != 0");
                        }
                        if self.replica != 0 {
                            return Some("init: replica != 0");
                        }
                    }
                    _ => {
                        if self.client == 0 {
                            return Some("client == 0");
                        }
                        if self.op == 0 {
                            return Some("op == 0");
                        }
                        if self.op <= self.commit {
                            return Some("op <= commit");
                        }
                        if self.operation == Operation::Register {
                            if self.request != 0 {
                                return Some("request != 0");
                            }
                        } else if self.request == 0 {
                            return Some("request == 0");
                        }
                    }
                }
            }
            Command::Reply => {
                if self.client == 0 {
                    return Some("client == 0");
                }
                if self.context != 0 {
                    return Some("context != 0");
                }
                if self.op != self.commit {
                    return Some("op != commit");
                }
                if self.operation == Operation::Register {
                    if self.commit == 0 {
                        return Some("commit == 0");
                    }
                    if self.request != 0 {
                        return Some("request != 0");
                    }
                } else {
                    if self.commit == 0 {
                        return Some("commit == 0");
                    }
                    if self.request == 0 {
                        return Some("request == 0");
                    }
                }
            }
            Command::Commit => {
                if self.parent != 0 {
                    return Some("parent != 0");
                }
                if self.client != 0 {
                    return Some("client != 0");
                }
                if self.request != 0 {
                    return Some("request != 0");
                }
                if self.op != 0 {
                    return Some("op != 0");
                }
                if self.offset != 0 {
                    return Some("offset != 0");
                }
                if self.operation != Operation::Reserved {
                    return Some("operation != .reserved");
                }
            }
            Command::StartViewChange => {
                if self.parent != 0 {
                    return Some("parent != 0");
                }
                if self.client != 0 {
                    return Some("client != 0");
                }
                if self.context != 0 {
                    return Some("context != 0");
                }
                if self.request != 0 {
                    return Some("request != 0");
                }
                if self.op != 0 {
                    return Some("op != 0");
                }
                if self.commit != 0 {
                    return Some("commit != 0");
                }
                if self.offset != 0 {
                    return Some("offset != 0");
                }
                if self.operation != Operation::Reserved {
                    return Some("operation != .reserved");
                }
            }
            Command::DoViewChange => {
                if self.parent != 0 {
                    return Some("parent != 0");
                }
                if self.client != 0 {
                    return Some("client != 0");
                }
                if self.context != 0 {
                    return Some("context != 0");
                }
                if self.request != 0 {
                    return Some("request != 0");
                }
                if self.operation != Operation::Reserved {
                    return Some("operation != .reserved");
                }
            }
            Command::StartView => {
                if self.parent != 0 {
                    return Some("parent != 0");
                }
                if self.client != 0 {
                    return Some("client != 0");
                }
                if self.context != 0 {
                    return Some("context != 0");
                }
                if self.request != 0 {
                    return Some("request != 0");
                }
                if self.offset != 0 {
                    return Some("offset != 0");
                }
                if self.operation != Operation::Reserved {
                    return Some("operation != .reserved");
                }
            }
            Command::Recovery | Command::RecoveryResponse => {
                // TODO: Implement validation once message is used
            }
            Command::RequestStartView => {
                if self.parent != 0 {
                    return Some("parent != 0");
                }
                if self.client != 0 {
                    return Some("client != 0");
                }
                if self.context != 0 {
                    return Some("context != 0");
                }
                if self.request != 0 {
                    return Some("request != 0");
                }
                if self.op != 0 {
                    return Some("op != 0");
                }
                if self.commit != 0 {
                    return Some("commit != 0");
                }
                if self.offset != 0 {
                    return Some("offset != 0");
                }
                if self.operation != Operation::Reserved {
                    return Some("operation != .reserved");
                }
            }
            Command::RequestHeaders => {
                if self.parent != 0 {
                    return Some("parent != 0");
                }
                if self.client != 0 {
                    return Some("client != 0");
                }
                if self.context != 0 {
                    return Some("context != 0");
                }
                if self.request != 0 {
                    return Some("request != 0");
                }
                if self.offset != 0 {
                    return Some("offset != 0");
                }
                if self.op == 0 {
                    return Some("op == 0");
                }
                if self.commit > self.op {
                    return Some("op_min > op_max");
                }
                if self.operation != Operation::Reserved {
                    return Some("operation != .reserved");
                }
            }
            Command::RequestPrepare => {
                if self.parent != 0 {
                    return Some("parent != 0");
                }
                if self.client != 0 {
                    return Some("client != 0");
                }
                if self.request != 0 {
                    return Some("request != 0");
                }
                if self.commit != 0 {
                    return Some("commit != 0");
                }
                if self.offset != 0 {
                    return Some("offset != 0");
                }
                if self.operation != Operation::Reserved {
                    return Some("operation != .reserved");
                }
            }
            Command::Headers => {
                if self.parent != 0 {
                    return Some("parent != 0");
                }
                if self.client != 0 {
                    return Some("client != 0");
                }
                if self.request != 0 {
                    return Some("request != 0");
                }
                if self.op != 0 {
                    return Some("op != 0");
                }
                if self.commit != 0 {
                    return Some("commit != 0");
                }
                if self.offset != 0 {
                    return Some("offset != 0");
                }
                if self.operation != Operation::Reserved {
                    return Some("operation != .reserved");
                }
            }
            Command::NackPrepare => {
                if self.parent != 0 {
                    return Some("parent != 0");
                }
                if self.client != 0 {
                    return Some("client != 0");
                }
                if self.request != 0 {
                    return Some("request != 0");
                }
                if self.commit != 0 {
                    return Some("commit != 0");
                }
                if self.offset != 0 {
                    return Some("offset != 0");
                }
                if self.operation != Operation::Reserved {
                    return Some("operation != .reserved");
                }
            }
            Command::Eviction => {
                if self.parent != 0 {
                    return Some("parent != 0");
                }
                if self.context != 0 {
                    return Some("context != 0");
                }
                if self.request != 0 {
                    return Some("request != 0");
                }
                if self.op != 0 {
                    return Some("op != 0");
                }
                if self.commit != 0 {
                    return Some("commit != 0");
                }
                if self.offset != 0 {
                    return Some("offset != 0");
                }
                if self.operation != Operation::Reserved {
                    return Some("operation != .reserved");
                }
            }
        }

        None
    }
}

pub struct TimeOut;

/// Calculates the size of a buffer rounded up to the nearest sector size.
pub fn sector_ceil(size: u64) -> u64 {
    size
        .div_ceil(config::SECTOR_SIZE as u64)
        * config::SECTOR_SIZE as u64
}

/// Calculates the offset rounded down to the nearest sector boundary.
pub fn sector_floor(offset: u64) -> u64 {
    (offset / config::SECTOR_SIZE as u64) * config::SECTOR_SIZE as u64
}
