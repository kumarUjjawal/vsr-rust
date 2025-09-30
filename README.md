# Viewstamped Replication in Rust

## Consensus Protocols
Consensus protocols coordinate a group of replicas so they agree on an ordered log of commands despite process crashes or network faults. They rely on quorums, stable storage, and deterministic state machines to keep every node in step with the decisions made by the group. Safety ensures the chosen log never forks, while liveness guarantees progress when a quorum of replicas can communicate.

## Viewstamped Replication
Viewstamped Replication follows a primary/backup model that advances through numbered views. Within a view, a leader assigns log slots to client operations and gathers acknowledgements from a quorum before committing the result. When the leader fails or communication stalls, replicas initiate a view change to nominate a new leader while preserving the already committed prefix. The protocol balances leader-driven ordering with rapid recovery so clients continue to observe a single coherent history.

## Project Overview
This repository contains an experimental, async Rust implementation of Viewstamped Replication along with a deterministic network, storage, and state-machine simulator. The goal is to create a Rust-idiomatic port of the original TigerBeetle reference implementation ([tigerbeetle/viewstamped-replication-made-famous](https://github.com/tigerbeetle/viewstamped-replication-made-famous)). The codebase is under active development and may diverge from the paper or the TigerBeetle version while functionality is still being validated.

### Components
- `src/replica.rs`: replica logic, leader election, log management, and timeout handling.
- `src/sim`: in-memory simulation of clients, network conditions, storage latency, and protocol invariants.
- `src/main.rs`: entry point that drives a multi-replica simulation and validates convergence.

### Accuracy Notice
The protocol details and simulator behaviour are still evolving. This implementation should be considered work in progress and might not be 100% faithful to the original Viewstamped Replication paper or the TigerBeetle code yet.

## Getting Started

### Prerequisites
- Rust toolchain with the `cargo` CLI and `rustup` components (tested with stable toolchain supporting edition 2024).

### Build
```bash
cargo build
```

### Lint
```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### Test
```bash
cargo test
```

### Run the Simulation
```bash
cargo run --release -- <optional-seed>
```
If no seed is supplied, a random one is chosen. The simulation prints the seed so test runs can be reproduced.

## References
- [tigerbeetle/viewstamped-replication-made-famous](https://github.com/tigerbeetle/viewstamped-replication-made-famous)
- [Viewstamped Replication Made Famous – TigerBeetle Lightning Talk](https://www.youtube.com/watch?v=_Jlikdtm4OA)
- [Bruno Bonacci – Viewstamped Replication Explained](https://blog.brunobonacci.com/2018/07/15/viewstamped-replication-explained/)
- [Distributed Computing Musings – Implementing Viewstamped Replication Protocol](https://distributed-computing-musings.com/2023/10/implementing-viewstamped-replication-protocol/)
- [Viewstamped Replication Lecture Series](https://www.youtube.com/watch?v=1EzNa-zAYS8&list=PLodh43ZY9Fy5H42HL347paAQofzzxfVnl)
