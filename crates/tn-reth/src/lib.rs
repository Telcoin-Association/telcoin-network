//! tn-reth: the sole boundary between the Telcoin Network node and reth.
//!
//! Every other TN crate reaches execution-layer functionality (storage, EVM,
//! transaction pool, RPC) through this crate — primarily the [`RethEnv`] facade — so
//! reth upgrades are absorbed here instead of rippling across the codebase. A few call
//! sites still consume the re-exports below; eventually everything should go through
//! this crate.
//!
//! Unlike upstream reth, the node is driven by Telcoin Network's BFT consensus output,
//! not by an engine API: certified consensus output is the only source of new blocks,
//! so there is no fork choice, there are no reorgs, and every canonical block is final
//! by construction (see `env/execution.rs` for the canonicalization pipeline).
//!
//! # Module map
//!
//! - `cli` — wrappers over reth's CLI/config types (`RethCommand`, `RethConfig`) and process-wide
//!   transaction-pool defaults.
//! - `dirs` — Telcoin Network data directory layout.
//! - `env` — the [`RethEnv`] facade itself, with methods split across `epoch` (epoch boundaries,
//!   ConsensusRegistry reads), `execution` (block building and atomic canonicalization), `genesis`
//!   (chain genesis, temp chains), `helpers` (read APIs), and `rpc` (RPC server support).
//! - `error` — `TnRethError`, wrapping the various reth error types.
//! - `evm` — TN EVM configuration: the custom handler (base-fee redirection and the gas-limit
//!   penalty), TEL/BLS precompiles, and the block executor.
//! - `forward` — forwards transactions from non-committee ("observer") workers to the committee.
//! - `metrics` — Prometheus metrics for the execution environment.
//! - `payload` — `TNPayload`, the per-block data derived from consensus output.
//! - `rpc_server_args` — the subset of reth RPC server args TN exposes.
//! - `snapshot` — export/restore of reth's plain EVM state.
//! - `system_calls` — solidity interfaces and epoch-boundary system calls (ConsensusRegistry,
//!   Issuance, worker configs).
//! - `traits` — compatibility glue between Telcoin node types and reth generics.
//! - `txn_pool` — abstraction over the reth transaction pool for workers.
//! - `types` — convenience type aliases and wrappers for execution.
//! - `worker` — node implementation pieces for reth compatibility on workers.
//! - `test_utils` (feature `test-utils`) — transaction factory and test helpers.
//!
//! # Feature flags
//!
//! - `faucet` — compiles the test-network faucet path of the TEL precompile: instant
//!   `mint(address,uint256)` with governance-managed mint roles, replacing the timelocked mint.
//!   This removes the 7-day mint timelock and MUST NOT ship in mainnet builds.
//! - `adiri` — testnet build. Implies `faucet`, enables `tn-types/adiri`, and compiles the
//!   in-protocol ConsensusRegistry fork (registry code swap plus validator-set migration at the
//!   configured fork epoch; see `apply_consensus_registry_fork` in `evm/block.rs`).
//! - `test-utils` — test helpers (pulls in `secp256k1`).
//! - `rocksdb` — opt back in to reth's RocksDB storage backend; storage is MDBX-only today, so this
//!   is off by default (see the feature comment in `Cargo.toml`).
//!
//! # Re-exports
//!
//! The `pub use reth_*` block below is a deliberate compatibility surface for callers
//! that still need raw reth types. It is expected to SHRINK over time: new code should
//! go through [`RethEnv`] rather than adding re-exports.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(
    missing_debug_implementations,
    missing_docs,
    unreachable_pub,
    rustdoc::all,
    unused_crate_dependencies
)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

// Used in tests
#[cfg(test)]
mod clippy {
    use proptest as _;
    use tn_reth as _;
}

use alloy::primitives::ChainId;
use reth_chainspec::EthChainSpec;
use std::sync::{Arc, OnceLock};
use system_calls::SYSTEM_ADDRESS;
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_types::{Address, BlockBody, Genesis, SealedBlock, SealedHeader};

// Reth stuff we are just re-exporting.  Need to reduce this over time.
pub use alloy::primitives::FixedBytes;
pub use reth::{
    chainspec::chain_value_parser, dirs::MaybePlatformPath, payload::BlobSidecars,
    rpc::builder::RpcServerHandle,
};
pub use reth_chain_state::{
    CanonicalInMemoryState, DeferredTrieData, ExecutedBlock, NewCanonicalChain,
};
pub use reth_chainspec::ChainSpec as RethChainSpec;
pub use reth_cli_util::{parse_duration_from_secs, parse_socket_address};
pub use reth_db::{
    mdbx::{open_db_read_only, DatabaseArguments, Error as RethMdbxError},
    static_file::iter_static_files,
    Database as RethDatabaseT, DatabaseEnv, Tables,
};
pub use reth_errors::{ProviderError, RethError};
pub use reth_node_core::{
    args::{ColorMode, LogArgs},
    node_config::DEFAULT_PERSISTENCE_THRESHOLD,
};
pub use reth_primitives_traits::crypto::secp256k1::sign_message;
/// Test-only re-export so engine tests can construct static-file provider errors.
#[cfg(feature = "test-utils")]
pub use reth_provider::StaticFileSegment;
pub use reth_provider::{
    providers::StaticFileProvider, CanonStateNotificationStream, ChangedAccount,
};
pub use reth_rpc_eth_types::EthApiError;
pub use reth_tracing::{FileWorkerGuard, Layers};
pub use reth_transaction_pool::{
    error::{InvalidPoolTransactionError, PoolError, PoolTransactionError},
    identifier::SenderIdentifiers,
    BestTransactions, EthPooledTransaction, TransactionPool as TransactionPoolT,
};

mod cli;
pub mod dirs;
pub mod payload;
pub mod traits;
pub mod txn_pool;
pub use txn_pool::*;
mod env;
pub mod error;
mod evm;
pub mod forward;
mod metrics;
pub mod rpc_server_args;
pub mod snapshot;
pub mod system_calls;
mod types;
pub mod worker;
pub use cli::{
    init_txpool_defaults, RethCommand, RethConfig, TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER,
};
pub use env::*;
#[cfg(feature = "faucet")]
pub use evm::faucet_mint_role_slot;
#[cfg(not(feature = "faucet"))]
pub use evm::TIMELOCK_DURATION;
pub use evm::{
    add_bls_precompile, add_telcoin_precompile, burnCall, calculate_gas_penalty, claimCall,
    grantMintRoleCall, hasMintRoleCall, mintCall, revokeMintRoleCall, totalSupplyCall,
    BLS_G1_PRECOMPILE_ADDRESS, TELCOIN_PRECOMPILE_ADDRESS,
};
pub use forward::WorkerRpcForwarder;
pub use metrics::report_db_metrics;
pub use types::*;

#[cfg(any(feature = "test-utils", test))]
pub mod test_utils;

/// True when this binary was compiled with the `faucet` cargo feature.
///
/// The faucet feature replaces the TEL precompile's timelocked, governance-only
/// `mint(uint256)` entry point with the instant, role-gated `mint(address,uint256)`
/// faucet variant, so a faucet build and a default build compute different state roots
/// for the same block once either mint selector is called. Startup code uses this
/// constant to refuse to join networks where that divergence would split consensus.
///
/// # Invariant
///
/// A runtime constant (rather than a `#[cfg]` in the caller) is the ground truth for
/// the whole binary: cargo feature unification means any crate in the build graph that
/// enables `tn-reth/faucet` flips this value for every consumer of this crate.
pub const FAUCET_ENABLED: bool = cfg!(feature = "faucet");

/// Process-global holder of the chain's base-fee recipient address.
///
/// It is a static [`OnceLock`] (rather than a `RethEnv` field) to work around the reth
/// lib interface: revm handlers are constructed by reth's traits without TN context, so
/// `TNEvmHandler::default()` (`src/evm/handler.rs`, built per transaction in
/// `transact_raw`) reads this global via [`basefee_address`] and credits the base-fee
/// portion of gas (`reward_beneficiary`) plus the gas-limit penalty
/// (`reimburse_caller`) to it. Those balance changes flow into the state root, which
/// makes this value CONSENSUS-CRITICAL.
///
/// # Invariant: first write wins — silently
///
/// The only writer is the private `set_basefee_address` in `env/mod.rs`, called from
/// `RethEnv::new`, and it discards the `OnceLock::set` result (`let _ =`): the FIRST
/// `RethEnv` constructed in the process pins the value for the process lifetime, and
/// every later write is silently dropped. Two constructors pass `None`, pinning the
/// default `GOVERNANCE_SAFE_ADDRESS`: `RethEnv::new_for_temp_chain` (`env/genesis.rs`,
/// also reached through genesis tooling) and `SnapshotRestorer::open` (`snapshot.rs`).
/// The production node passes the configured `parameters.basefee_address`
/// (`crates/node/src/manager/node.rs`). That field is a required key with no serde
/// default (`tn-config`), so a `parameters.yaml` that lost the key fails at parse
/// time instead of reaching the default here.
///
/// Consequence: if a `None` path runs first in a process that then starts a real node
/// configured with a non-default basefee address, the configured address is silently
/// ignored — every block this node builds or re-executes credits fees to the default
/// address, its state roots diverge from peers honoring the configured address, and
/// the node FORKS off the network.
static BASEFEE_ADDRESS: OnceLock<Address> = OnceLock::new();

/// Return the process-global base-fee recipient address.
///
/// Reads the `BASEFEE_ADDRESS` `OnceLock`, falling back to `GOVERNANCE_SAFE_ADDRESS`
/// while unset. The value is pinned by the first `RethEnv::new` in the process (first
/// write wins; later writes are silently ignored — see the `BASEFEE_ADDRESS` docs above
/// for the fork risk when a default-pinning path runs before the real node). Per chain
/// the address is fixed and only changes via a hard fork. The EVM handler reads it per
/// transaction and credits base fees and gas-limit penalties to it.
pub fn basefee_address() -> Address {
    *BASEFEE_ADDRESS.get().unwrap_or(&GOVERNANCE_SAFE_ADDRESS)
}

/// Wrapper for Reth ChainSpec, just a layer of abstraction.
#[derive(Clone, Debug)]
pub struct ChainSpec(Arc<RethChainSpec>);

impl ChainSpec {
    /// Return the contained Reth ChainSpec behind its `Arc` (refcount bump, no deep clone of
    /// the genesis alloc).
    pub(crate) fn reth_chain_spec(&self) -> Arc<RethChainSpec> {
        self.0.clone()
    }

    /// Return a reference to the ChainSpec's genesis.
    pub fn genesis(&self) -> &Genesis {
        self.0.genesis()
    }

    /// Return the sealed header for genesis.
    pub fn sealed_genesis_header(&self) -> SealedHeader {
        self.0.sealed_genesis_header()
    }

    /// Return the sealed genesis block: the sealed genesis header wrapped in an
    /// empty body (no transactions, no ommers, and empty — not absent — withdrawals).
    pub fn sealed_genesis_block(&self) -> SealedBlock {
        let header = self.sealed_genesis_header();
        let body = BlockBody {
            transactions: vec![],
            ommers: vec![],
            withdrawals: Some(Default::default()),
        };

        SealedBlock::from_sealed_parts(header, body)
    }

    /// Return the chain id.
    pub fn chain_id(&self) -> ChainId {
        self.0.chain_id()
    }
}
