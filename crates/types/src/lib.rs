// SPDX-License-Identifier: Apache-2.0

mod codec;
#[allow(clippy::mutable_key_type)]
mod committee;
mod crypto;
pub mod database_traits;
mod genesis;
mod helpers;
mod multiaddr;
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
pub use multiaddr::*;
pub use notifier::*;
pub use primary::*;
pub use sync::*;
pub use task_manager::*;
pub use worker::*;

// re-exports for easier maintainability
pub use alloy::{
    consensus::{
        constants::{EMPTY_OMMER_ROOT_HASH, EMPTY_RECEIPTS, EMPTY_TRANSACTIONS, EMPTY_WITHDRAWALS},
        proofs::calculate_transaction_root,
        BlockHeader, Header as ExecHeader, Transaction as TransactionTrait, TxEip1559,
    },
    eips::{
        eip1559::{ETHEREUM_BLOCK_GAS_LIMIT, MIN_PROTOCOL_BASE_FEE},
        eip2718::Encodable2718,
        eip4844::{env_settings::EnvKzgSettings, BlobAndProofV1, BlobTransactionSidecar},
        BlockHashOrNumber, BlockNumHash,
    },
    genesis::{Genesis, GenesisAccount},
    hex::{self, FromHex},
    primitives::{
        hex_literal, keccak256, Address, BlockHash, BlockNumber, Bloom, Bytes, Sealable, TxHash,
        TxKind, B256, U160, U256,
    },
    rpc::types::{AccessList, Withdrawals},
    signers::Signature as EthSignature,
    sol,
    sol_types::{SolType, SolValue},
};
pub use reth_primitives::{
    public_key_to_address,
    transaction::{SignedTransactionIntoRecoveredExt, PARALLEL_SENDER_RECOVERY_THRESHOLD},
    Block, BlockBody, BlockExt, BlockWithSenders, EthPrimitives, NodePrimitives, PooledTransaction,
    Receipt, RecoveredTx, SealedBlock, SealedBlockWithSenders, SealedHeader, Transaction,
    TransactionSigned,
};
