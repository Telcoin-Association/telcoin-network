// SPDX-License-Identifier: Apache-2.0
#![allow(missing_docs)]

// Used in tests
#[cfg(test)]
use proptest as _;
#[cfg(test)]
use tn_storage as _;
#[cfg(test)]
use tn_test_utils_committee as _;

mod canonical_reader;
mod codec;
#[allow(clippy::mutable_key_type)]
mod committee;
pub mod consensus_chain_traits;
mod crypto;
pub mod database_traits;
pub mod gas_accumulator;
mod genesis;
mod helpers;
mod notifier;
mod primary;
mod serde;
mod sync;
mod task_manager;
mod worker;
#[macro_use]
pub mod error;
pub mod forks;

pub use canonical_reader::*;
pub use codec::*;
pub use committee::*;
pub use consensus_chain_traits::*;
pub use crypto::*;
pub use database_traits::*;
pub use genesis::*;
pub use helpers::*;
pub use notifier::*;
pub use primary::*;
pub use sync::*;
pub use task_manager::*;
pub use worker::*;
#[cfg(feature = "test-utils")]
pub mod test_utils;

/// Test-only hook: suppress storing epoch certificates assembled from local vote quorums,
/// read once from `TN_TEST_SUPPRESS_EPOCH_CERTS` (`1` to suppress).
///
/// Lets an e2e harness manufacture the halted-fleet state where every node holds an epoch
/// record but no certificate exists anywhere — the state a whole-committee crash between an
/// epoch close and certification leaves behind — so recovery paths can be exercised against
/// real node processes. An environment variable rather than a process-global setter for the
/// same reason as [`forks::seed_signature_fork_epoch_override`]: e2e tests drive real spawned
/// node binaries that share no memory with the harness.
///
/// Compiled out entirely without `test-utils`, so a production binary always stores the
/// certificates it assembles and cannot be repointed at runtime by its environment.
#[cfg(feature = "test-utils")]
pub fn test_suppress_epoch_certs() -> bool {
    static SUPPRESS: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *SUPPRESS.get_or_init(|| {
        std::env::var("TN_TEST_SUPPRESS_EPOCH_CERTS").is_ok_and(|raw| raw.trim() == "1")
    })
}

/// Production builds never suppress certificate storage; see the `test-utils` variant.
#[cfg(not(feature = "test-utils"))]
#[inline]
pub const fn test_suppress_epoch_certs() -> bool {
    false
}

// re-exports for easier maintainability
pub use alloy::{
    consensus::{
        constants::{EMPTY_OMMER_ROOT_HASH, EMPTY_RECEIPTS, EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS},
        proofs::calculate_transaction_root,
        transaction::TransactionMeta,
        BlockHeader, Header as ExecHeader, SignableTransaction, Transaction as TransactionTrait,
        TxEip1559,
    },
    eips::{
        eip1559::{ETHEREUM_BLOCK_GAS_LIMIT_30M, MIN_PROTOCOL_BASE_FEE},
        eip2718::{Decodable2718, Encodable2718, Typed2718},
        eip4844::{env_settings::EnvKzgSettings, BlobAndProofV1, BlobTransactionSidecar},
        BlockHashOrNumber, BlockNumHash,
    },
    genesis::{Genesis, GenesisAccount},
    hex::{self, FromHex},
    primitives::{
        address, hex_literal, keccak256, Address, BlockHash, BlockNumber, Bloom, Bytes, Log,
        LogData, Sealable, TxHash, TxKind, TxNumber, B256, U160, U256,
    },
    rpc::types::{AccessList, Withdrawals},
    signers::Signature as EthSignature,
    sol,
    sol_types::{SolCall, SolType, SolValue},
};
pub use libp2p::{multiaddr::Protocol, Multiaddr};

/// Whether a transaction's EIP-2718 type is on the batch executable allowlist
/// (legacy, EIP-2930, EIP-1559).
///
/// One predicate shared by the batch validator, the batch builder, and the
/// worker gateway so producers and validators can never disagree on the set.
/// Deliberately fork-blind: uniform across chain configurations.
pub fn batch_allowlisted_tx_type<T: Typed2718>(tx: &T) -> bool {
    tx.is_legacy() || tx.is_eip2930() || tx.is_eip1559()
}
pub use reth_primitives::{
    Account, Block, BlockBody, EthPrimitives, NodePrimitives, PooledTransaction, Receipt,
    Recovered, RecoveredBlock, SealedBlock, SealedHeader, Transaction, TransactionSigned,
};
