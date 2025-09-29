use rand::prelude::*;
use std::env;
use vsr_rust::message_pool::{MessagePool, ProcessType};
use vsr_rust::sim::cluster::Cluster;
use vsr_rust::sim::state_checker::StateChecker;
use vsr_rust::sim::state_machine::HashingStateMachine;
use vsr_rust::vsr::Operation;

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

    let replica_count = 3;
    let client_count = 1;
    let transitions_max = 100;

    let client_pool = MessagePool::new(ProcessType::Client);
    
    // 2. Create and run the Cluster
    let mut cluster = Cluster::new(replica_count, client_count, rng.clone()).await;
    let mut state_checker = StateChecker::new(&cluster);

    // 3. Main Simulation Loop
    let ticks_max = 1_000;
    for tick in 0..ticks_max {
        // Drive the simulation forward
        cluster.tick().await;
        
        // Check replica states for consensus
        for i in 0..replica_count {
            state_checker.check_state(i, &cluster);
        }

        // Randomly send a client request if we haven't reached the goal
        if state_checker.transitions < transitions_max && rng.random::<f64>() < 0.2 {
            let client_idx = 0; // Only one client
            let client = &mut cluster.clients[client_idx];
            let request_queue = &mut state_checker.client_requests[client_idx];

            if !request_queue.is_full() {
                let mut message = client_pool.get_message().unwrap();
                let input_hash;
                let body_len;
                {
                    let body = message.body_mut();
                    rng.fill(body); // Fill body with random data
                    input_hash = HashingStateMachine::hash(client.id(), body);
                    body_len = body.len();
                }
                request_queue.push_assume_capacity(input_hash);

                client.request(
                    0,
                    Box::new(|_, _, _| {}), // Dummy callback
                    Operation::Hash,
                    message,
                    body_len,
                );
                if state_checker.transitions == 0 {
                    println!("submitted client request");
                }
            }
        }
        
        // Check for completion
        if state_checker.transitions >= transitions_max && state_checker.convergence() {
            println!("\nSUCCESS: Cluster completed {} transitions and converged in {} ticks.", state_checker.transitions, tick);
            println!("Final state hash: {:#x}", cluster.replicas[0].state_machine_hash());
            return;
        }
    }

    panic!("\nFAILURE: Simulation timed out after {} ticks. Transitions completed: {}.", ticks_max, state_checker.transitions);
}
