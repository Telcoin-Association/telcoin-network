//! CLI integration test

// ignore for lib
#![allow(unused_crate_dependencies)]

mod admission;
mod basefee;
mod clock_skew;
mod common;
mod eject;
mod epoch_drain;
mod epochs;
mod exec_block;
#[cfg(feature = "faucet")]
mod faucet;
mod forward_txs;
mod genesis_tests;
mod governance_safe_fork;
mod metrics;
mod persist_pipeline;
mod restarts;
mod rpc_namespaces;
mod sealed_txs;
#[cfg(unix)]
mod sigkill;
mod staking;
mod state_export_import;
mod sync;
mod upgrade;

fn main() {}
