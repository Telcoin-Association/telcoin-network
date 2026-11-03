//! CLI integration test

// ignore for lib
#![allow(unused_crate_dependencies)]

mod basefee;
mod clock_skew;
mod common;
mod eject;
mod epochs;
#[cfg(feature = "faucet")]
mod faucet;
mod forward_txs;
mod genesis_tests;
mod governance_safe_fork;
mod metrics;
mod restarts;
mod rpc_namespaces;
#[cfg(unix)]
mod sigkill;
mod staking;
mod state_export_import;
mod sync;

fn main() {}
