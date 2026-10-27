//! Convenience types for execution.
//!
//! Type aliases over reth internals, re-exported at the crate root (`pub use types::*`) so
//! downstream crates name TN types instead of reth's directly. When reth moves, renames, or
//! deprecates one of these types, only this file has to change — callers keep compiling against
//! the alias.

use reth_rpc_builder::TransportRpcModules;
use reth_transaction_pool::{
    identifier::TransactionId, EthPooledTransaction, ValidPoolTransaction,
};
use tn_types::{EthPrimitives, RecoveredBlock};

/// Rpc Server type, used for getting the node started.
pub type RpcServer = TransportRpcModules<()>;

/// Type alias to replace deprecated reth struct with new generic type:
/// A block with senders recovered from the block’s transactions.
///
/// This type is a SealedBlock with a list of senders that match the transactions in the block.
pub type BlockWithSenders = RecoveredBlock<reth_ethereum_primitives::Block>;

/// The reth database (`RethDb`, an `Arc` of `RethDbEnv`): MDBX by default, tndb-b with the
/// `tndb-b` feature.
pub use crate::env::database::{RethDb, RethDbEnv};

/// A pooled transaction id.
pub type PoolTxnId = TransactionId;
/// A pooled transaction.
pub type PoolTxn = ValidPoolTransaction<EthPooledTransaction>;
/// The node's primitive types (block, transaction, receipt, header).
///
/// TN executes standard Ethereum primitives; this alias is what generic reth machinery
/// (`NodeTypes`, `ConfigureEvm`) is instantiated with throughout the crate.
pub type TNPrimitives = EthPrimitives;
