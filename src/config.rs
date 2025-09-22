// The maximum log level in increasing order of verbosity (emergency=0, debug=3):
pub const LOG_LEVEL: usize = 2;

// maximum number of replicas allowed in a cluster
pub const REPLICAS_MAX: usize = 6;

pub const CLIENTS_MAX: usize = 3;

pub const QUORUM_REPLICATION_MAX: u8 = 3;

pub const PORT: usize = 3001;

pub const ADDRESS: &str = "127.0.0.1";

pub const DIRECTORY: &str = "./";

pub const ACCOUNTS_MAX: i32 = 100_000;

pub const TRANSFER_MAX: i32 = 1_000_000;

pub const COMMITS_MAX: i32 = TRANSFER_MAX;

pub const JOURNAL_SIZE_MAX: i32 = 128 * 1024 * 1024;

pub const CONNECTION_MAX: usize = REPLICAS_MAX + CLIENTS_MAX;

pub const MESSAGE_SIZE_MAX: usize = 1 * 1024 * 1024;

pub const PIPELINING_MAX: usize = CLIENTS_MAX;

pub const CONNECTION_DELAY_MIN_MS: i32 = 50;
pub const CONNECTION_DELAY_MAX_MS: i32 = 100;

pub const CONNECTION_SEND_QUEUE_MAX_REPLICA: usize = {
    let x = CLIENTS_MAX;
    if x < 2 {
        2
    } else if x > 4 {
        4
    } else {
        x
    }
};

pub const CONNECTION_SEND_QUEUE_MAX_CLIENT: usize = 2;

pub const CLIENT_REQUEST_QUEUE_MAX: usize = 32;

pub const TCP_BACKLOG: u8 = 64;

pub const TCP_RCVBUF: i32 = 4 * 1024 * 1024;

pub const TCP_SNDBUF_REPLICA: usize = CONNECTION_SEND_QUEUE_MAX_REPLICA * MESSAGE_SIZE_MAX;
pub const TCP_SNDBUF_CLIENT: usize = CONNECTION_SEND_QUEUE_MAX_REPLICA * MESSAGE_SIZE_MAX;

pub const TCP_KEEPALIVE: bool = true;

pub const TCP_KEEPIDLE: i32 = 5;

pub const TCP_KEEPINTVL: i32 = 4;

pub const TCP_KEEPCNT: i32 = 3;

pub const TCP_USER_TIMEOUT: i32 = (TCP_KEEPIDLE * TCP_KEEPINTVL * TCP_KEEPCNT) * 1000;

pub const TCP_NODELAY: bool = true;

pub const SECTOR_SIZE: usize = 4096;

pub const DIRECT_IO: bool = true;

pub const IO_DEPTH_READ: usize = 8;
pub const IO_DEPTH_WRITE: usize = 8;

pub const TICK_MS: u16 = 10;

pub const RTT_TICKS: u16 = 300 / TICK_MS;

pub const RTT_MULTIPLE: u16 = 2;

pub const BACKOFF_MIN_TICKS: u16 = 100 / TICK_MS;

pub const BACKOFF_MAX_TICKS: u16 = 1000 / TICK_MS;

pub const CLOCK_OFFSET_TOLERANCE_MAX_MS: u64 = 10000;

pub const CLOCK_EPOCH_MAX_MS: u64 = 60000;

pub const CLOCK_SYNCHRONIZATION_WINDOW_MIN_MS: u64 = 2000;

pub const CLOCK_SYNCHRONIZATION_WINDOW_MAX_MS: u64 = 20000;
