// SPDX-License-Identifier: Apache-2.0

#![warn(unused_crate_dependencies)]

mod codec;
#[allow(clippy::mutable_key_type)]
mod committee;
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

pub use codec::*;
pub use committee::*;
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

// re-exports for easier maintainability
pub use alloy::{
    consensus::{
        constants::{EMPTY_OMMER_ROOT_HASH, EMPTY_RECEIPTS, EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS},
        proofs::calculate_transaction_root,
        BlockHeader, Header as ExecHeader, SignableTransaction, Transaction as TransactionTrait,
        TxEip1559,
    },
    eips::{
        eip1559::{ETHEREUM_BLOCK_GAS_LIMIT_30M, MIN_PROTOCOL_BASE_FEE},
        eip2718::Encodable2718,
        eip4844::{env_settings::EnvKzgSettings, BlobAndProofV1, BlobTransactionSidecar},
        BlockHashOrNumber, BlockNumHash,
    },
    genesis::{Genesis, GenesisAccount},
    hex::{self, FromHex},
    primitives::{
        address, hex_literal, keccak256, Address, BlockHash, BlockNumber, Bloom, Bytes, Sealable,
        TxHash, TxKind, B256, U160, U256,
    },
    rpc::types::{AccessList, Withdrawals},
    signers::Signature as EthSignature,
    sol,
    sol_types::{SolType, SolValue},
};
pub use libp2p::Multiaddr;
pub use reth_primitives::{
    Block, BlockBody, EthPrimitives, NodePrimitives, PooledTransaction, Receipt, Recovered,
    RecoveredBlock, SealedBlock, SealedHeader, Transaction, TransactionSigned,
};
