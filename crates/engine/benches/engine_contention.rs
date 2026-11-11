//! Engine contention benchmark: how signer recovery and state-root work for queued outputs slow
//! the output that is executing.
//!
//! A placeholder for now: the target builds and runs, and measures nothing yet.
//!
//! Run with `cargo bench -p tn-engine --bench engine_contention`.

// A bench binary links every dependency and dev-dependency of its crate but uses only a few.
#![allow(unused_crate_dependencies)]

fn main() {
    println!("engine_contention: no benchmark groups yet");
}
