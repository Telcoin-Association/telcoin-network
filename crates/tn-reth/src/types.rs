//! Convenience types for execution.

use std::sync::Arc;

use reth::rpc::builder::TransportRpcModules;
use reth_db::DatabaseEnv;
use reth_transaction_pool::{EthPooledTransaction, ValidPoolTransaction, identifier::TransactionId};
use tn_types::{EthPrimitives, Recovered, RecoveredBlock};

/// Rpc Server type, used for getting the node started.
pub type RpcServer = TransportRpcModules<()>;

/// Type alias to replace deprecated reth struct with new generic type:
/// A block with senders recovered from the block’s transactions.
///
/// This type is a SealedBlock with a list of senders that match the transactions in the block.
pub type BlockWithSenders = RecoveredBlock<reth_ethereum_primitives::Block>;

/// Type wrapper for a Reth DB.
/// Used primary as a opaque type to allow
/// the node launcher to create the DB upfront and reuse.
pub type RethDb = Arc<DatabaseEnv>;

/// A pooled transaction id.
pub type PoolTxnId = TransactionId;
/// A pooled transaction.
pub type PoolTxn = ValidPoolTransaction<EthPooledTransaction>;
/// A recovered pooled transaction.
pub type RecoveredPoolTxn = Recovered<EthPooledTransaction>;
/// Type for primitives.
pub type TNPrimitives = EthPrimitives;
