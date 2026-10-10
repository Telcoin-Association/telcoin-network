//! Compile the production policy tests without the network dependency graph.
//!
//! Run from the repository root with Rust 1.94:
//! `rustc +1.94 --edition=2021 --test crates/network-libp2p/src/peers/standalone_tests.rs`

mod penalty;
mod policy;
mod pending_inbound;
