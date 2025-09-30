use rand::prelude::*;
use std::env;
use vsr_rust::message_pool::{MessagePool, ProcessType};
use vsr_rust::sim::cluster::{Cluster, ClusterOptions};
use vsr_rust::sim::network::NetworkOptions;
use vsr_rust::sim::packet_simulator::{Options as PacketSimulatorOptions, PartitionMode};
use vsr_rust::sim::state_checker::StateChecker;
use vsr_rust::sim::state_machine::HashingStateMachine;
use vsr_rust::sim::storage::Options as StorageOptions;
use vsr_rust::vsr::{Header, Operation};

#[tokio::main]
async fn main() {
    println!("Starting VSR simulation...");

    // 1. Simulation Setup
    let args: Vec<String> = env::args().collect();
    let seed = args
        .get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| rand::rng().random());
    println!("Using seed: {}", seed);
    let mut rng = StdRng::seed_from_u64(seed);

    let replica_count = 3u8;
    let client_count = 1u8;
    let node_count = replica_count + client_count;

    let packet_simulator_options = PacketSimulatorOptions {
        one_way_delay_mean: 5,
        one_way_delay_min: 1,
        packet_loss_probability: 1,
        packet_replay_probability: 0,
        seed: rng.random(),
        replica_count,
        client_count,
        node_count,
        partition_mode: PartitionMode::UniformPartition,
        partition_probability: 0,
        unpartition_probability: 100,
        partition_stability: 0,
        unpartition_stability: 0,
        path_maximum_capacity: 4,
        path_clog_duration_mean: 0,
        path_clog_probability: 0,
    };

    let storage_options = StorageOptions {
        seed: rng.random(),
        read_latency_min: 1,
        read_latency_mean: 3,
        write_latency_min: 1,
        write_latency_mean: 3,
        read_fault_probability: 0,
        write_fault_probability: 0,
    };

    let cluster_options = ClusterOptions {
        cluster: 0,
        replica_count,
        client_count,
        seed: rng.random(),
        network: NetworkOptions {
            packet_simulator: packet_simulator_options,
        },
        storage: storage_options,
    };

    let client_pool = MessagePool::new(ProcessType::Client);

    // 2. Create and run the Cluster
    let transitions_max = 20;
    let mut requests_sent: u64 = 0;

    let mut cluster = Cluster::new(cluster_options).await;
    let mut state_checker = StateChecker::new(&cluster);

    // 3. Main Simulation Loop
    let ticks_max = 50_000;
    let warmup_ticks = 200;

    for tick in 0..ticks_max {
        // Drive the simulation forward
        cluster.tick().await;

        // Check replica states for consensus
        for i in 0..replica_count {
            state_checker.check_state(i, &cluster);
        }

        // Randomly send a client request if we haven't reached the goal
        if tick >= warmup_ticks
            && state_checker.transitions < transitions_max
            && requests_sent < transitions_max
        {
            let client_idx = 0; // Only one client
            let client = &mut cluster.clients[client_idx];
            let request_queue = &mut state_checker.client_requests[client_idx];

            if request_queue.is_empty() {
                let mut message = client_pool.get_message().unwrap();
                let body_len = 256;
                {
                    let header = message.header_mut();
                    header.size = (std::mem::size_of::<Header>() + body_len) as u32;
                }

                let input_hash;
                {
                    let body = message.body_mut();
                    rng.fill(body); // Fill body with random data
                    input_hash = HashingStateMachine::hash(client.id(), body);
                }
                request_queue.push_assume_capacity(input_hash);

                client.request(
                    0,
                    Box::new(|_, _, _| {}), // Dummy callback
                    Operation::Hash,
                    message,
                    body_len,
                );
                requests_sent += 1;
                if state_checker.transitions == 0 {
                    println!("submitted client request");
                }
            }
        }

        // Check for completion
        if state_checker.transitions >= transitions_max && state_checker.convergence() {
            println!(
                "\nSUCCESS: Cluster completed {} transitions and converged in {} ticks.",
                state_checker.transitions, tick
            );
            println!(
                "Final state hash: {:#x}",
                cluster.replicas[0].state_machine_hash()
            );
            return;
        }
    }

    panic!(
        "\nFAILURE: Simulation timed out after {} ticks. Transitions completed: {}. Outstanding requests: {}",
        ticks_max,
        state_checker.transitions,
        state_checker.client_requests[0].len()
    );
}
