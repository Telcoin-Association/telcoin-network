//! CLI integration test

// ignore for lib
#![allow(unused_crate_dependencies)]

mod basefee;
mod common;
mod eject;
mod epoch_cert_recovery;
mod epochs;
#[cfg(feature = "faucet")]
mod faucet;
mod genesis_tests;
mod metrics;
mod restarts;
mod staking;
mod state_export_import;
mod sync;

fn main() {}
