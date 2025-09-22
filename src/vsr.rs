use std::mem;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Command {
    Reserved = 0,
    Ping = 1,
    Pong = 2,
    Request = 3,
    Prepare = 4,
    PrepareOk = 5,
    Reply = 6,
    Commit = 7,
    StartViewChange = 8,
    DoViewChange = 9,
    StartView = 10,
    Recovery = 11,
    RecoveryResponse = 12,
    RequestStartView = 13,
    RequestHeaders = 14,
    RequestPrepare = 15,
    Headers = 16,
    NackPrepare = 17,
    Eviction = 18,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Operation {
    Reserved = 0,
    Init = 1,
    Register = 2,
}

#[repr(C, packed)]
#[derive(Debug, Clone, Copy)]
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
            version: 1,
        }
    }
}

pub struct TimeOut;
