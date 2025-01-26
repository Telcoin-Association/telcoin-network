// SPDX-License-Identifier: Apache-2.0

mod codec;
#[allow(clippy::mutable_key_type)]
mod committee;
mod crypto;
mod engine;
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
pub use engine::*;
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
        constants::{EMPTY_OMMER_ROOT_HASH, EMPTY_WITHDRAWALS},
        Header as ExecHeader, Transaction as TransactionTrait, TxEip1559,
    },
    eips::{
        eip1559::{ETHEREUM_BLOCK_GAS_LIMIT, MIN_PROTOCOL_BASE_FEE},
        eip4844::{BlobAndProofV1, BlobTransactionSidecar},
        BlockNumHash,
    },
    genesis::{Genesis, GenesisAccount},
    hex::{self, FromHex},
    primitives::{keccak256, Address, BlockHash, BlockNumber, TxHash, TxKind, B256, U160, U256},
    rpc::types::Withdrawals,
    signers::Signature as EthSignature,
    sol_types::SolType,
};
pub use reth_primitives::{
    public_key_to_address, Block, BlockBody, BlockWithSenders, NodePrimitives, Receipt,
    RecoveredTx, SealedBlock, SealedBlockWithSenders, SealedHeader, Transaction, TransactionSigned,
};
