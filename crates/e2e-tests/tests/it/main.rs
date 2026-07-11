//! CLI integration test

// ignore for lib
#![allow(unused_crate_dependencies)]

mod basefee;
mod common;
mod eject;
mod epochs;
#[cfg(feature = "faucet")]
mod faucet;
mod genesis_tests;
mod metrics;
mod restarts;
mod snapshots;
mod staking;
mod sync;

fn main() {}
