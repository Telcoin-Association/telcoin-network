//! Pins what reth's process-global RPC server defaults resolve to after seeding.
//!
//! Reth reads `RpcServerArgs::default()` out of a process-wide `OnceLock` whose first read fixes
//! it for the life of the process, so this assertion only means something in a process where the
//! seeding call runs before anything reads the lock. That is why it lives in its own integration
//! test binary, mirroring `telcoin-network-cli/tests/txpool_defaults.rs`: every test here seeds
//! before it reads, so any interleaving of these tests yields the same lock contents.

// An integration test binary links every dependency of its crate but uses only a few.
#![allow(unused_crate_dependencies)]

use tn_reth::{init_reth_defaults, rpc_server_args::DEFAULT_IPC_ENDPOINT};

/// Reth's own IPC default, spelled out rather than imported.
///
/// Hard-coding it keeps the divergence assertion below honest if TN's default is ever changed to
/// coincide with reth's, which is the condition that made temp chains bind reth's socket in the
/// first place.
#[cfg(windows)]
const RETH_IPC_ENDPOINT: &str = r"\\.\pipe\reth.ipc";

/// Reth's own IPC default, spelled out rather than imported (non-Windows spelling).
#[cfg(not(windows))]
const RETH_IPC_ENDPOINT: &str = "/tmp/reth.ipc";

/// After seeding, a reth-side `RpcServerArgs::default()` carries TN's IPC endpoint, so every
/// construction path that fills from reth's defaults (`NodeConfig::default()` for temp chains,
/// the `..Default::default()` fill in TN's `From` impl) stops resurrecting reth's `reth.ipc`
/// (issue #1165). The seed leaves the IPC server enabled: only temp chains disable it.
#[test]
fn seeded_reth_rpc_default_carries_tn_ipcpath() {
    init_reth_defaults();
    let args = reth::args::RpcServerArgs::default();
    assert_eq!(args.ipcpath, DEFAULT_IPC_ENDPOINT);
    assert_ne!(args.ipcpath, RETH_IPC_ENDPOINT);
    assert!(!args.ipcdisable, "seeding must not disable IPC for parsed nodes");
}
