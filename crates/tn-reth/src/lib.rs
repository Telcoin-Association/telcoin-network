//! This should allow for easier upgrades.
//! It still re-exports some stuff and a few places use Reth directly but eventually
//! it all should go through this crate.

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

/// This will contain the address to receive base fees.  It is set per chain and
/// will not change.  Implemented as a static OnceLock to work around the Reth lib interface.
static BASEFEE_ADDRESS: OnceLock<Address> = OnceLock::new();

/// Return the chains basefee address if set.
/// Note the basefee address is set once for the chain and will not change (outside of a hard fork).
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

    /// Return the sealed header for genesis.
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
