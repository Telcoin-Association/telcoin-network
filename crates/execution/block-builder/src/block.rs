//! The logic for building batches.
//!
//! Transactions are pulled from the worker's pending pool and added to the block without being
//! executed. Block size is measured in bytes and a transaction's max gas limit. The block is sealed
//! when the pending pool devoid of transactions or the max block size is reached (wei or bytes).
//!
//! The mined transactions are returned with the built block so the worker can update the pool.

use reth_primitives::{IntoRecoveredTransaction, TxHash};
use reth_transaction_pool::TransactionPool;
use tn_types::{
    max_worker_block_gas, max_worker_block_size, now, PendingBlockConfig, WorkerBlock,
    WorkerBlockBuilderArgs,
};
use tracing::{debug, warn};

/// The output from building the next block.
///
/// Contains information needed to update the transaction pool.
#[derive(Debug)]
pub struct BlockBuilderOutput {
    /// The block info for the worker to propose.
    pub(crate) worker_block: WorkerBlock,
    /// The transaction hashes mined in this worker's block.
    ///
    /// NOTE: canonical changes update `ChangedAccount` and changed senders.
    /// Only the mined transactions are removed from the pool. Account nonce and state
    /// should only be updated on canonical changes so workers can validate
    /// each other's blocks off the canonical tip.
    ///
    /// This is less efficient when accounts have lots of transactions in the pending
    /// pool, but this approach is easier to implement in the short term.
    pub(crate) mined_transactions: Vec<TxHash>,
}

/// Construct an TN batch using the best transactions from the pool.
///
/// Returns the [`BlockBuilderOutput`] and cannot fail. The batch continues to add
/// transactions to the proposed block until either:
/// - accumulated transaction gas limit reached (measured by tx.gas_limit())
/// - max byte size of transactions (measured by tx.size())
///
/// NOTE: it's possible to under utilize resources if users submit transactions
/// with very high gas limits. It's impossible to know the amount of gas a transaction
/// will use without executing it, and the worker does not execute transactions.
#[inline]
pub fn build_worker_block<P>(args: WorkerBlockBuilderArgs<P>) -> BlockBuilderOutput
where
    P: TransactionPool,
{
    let WorkerBlockBuilderArgs { pool, block_config } = args;
    let gas_limit = max_worker_block_gas(block_config.parent_info.tip.timestamp);
    let max_size = max_worker_block_size(block_config.parent_info.tip.timestamp);
    let PendingBlockConfig { beneficiary, parent_info } = block_config;

    // NOTE: this obtains a `read` lock on the tx pool
    // pull best transactions and rely on watch channel to ensure basefee is current
    let mut best_txs = pool.best_transactions();

    // NOTE: batches always build off the latest finalized block
    let parent_hash = parent_info.tip.hash();

    // collect data for successful transactions
    // let mut sum_blob_gas_used = 0;
    let mut total_bytes_size = 0;
    let mut total_possible_gas = 0;
    let mut transactions = Vec::new();
    let mut mined_transactions = Vec::new();

    // begin loop through sorted "best" transactions in pending pool
    // and execute them to build the block
    while let Some(pool_tx) = best_txs.next() {
        // filter best transactions against Arc<hashset<TxHash>>

        // ensure block has capacity (in gas) for this transaction
        if total_possible_gas + pool_tx.gas_limit() > gas_limit {
            // the tx could exceed max gas limit for the block
            // marking as invalid within the context of the `BestTransactions` pulled in this
            // current iteration  all dependents for this transaction are now considered invalid
            // before continuing loop
            best_txs.mark_invalid(&pool_tx);
            debug!(target: "worker::block_builder", ?pool_tx, "marking tx invalid due to gas constraint");
            continue;
        }

        // convert tx to a signed transaction
        //
        // NOTE: `ValidPoolTransaction::size()` is private
        let tx = pool_tx.to_recovered_transaction();

        // ensure block has capacity (in bytes) for this transaction
        if total_bytes_size + tx.size() > max_size {
            // the tx could exceed max gas limit for the block
            // marking as invalid within the context of the `BestTransactions` pulled in this
            // current iteration  all dependents for this transaction are now considered invalid
            // before continuing loop
            best_txs.mark_invalid(&pool_tx);
            debug!(target: "worker::block_builder", ?pool_tx, "marking tx invalid due to bytes constraint");
            continue;
        }

        // txs are not executed, so use the gas_limit
        total_possible_gas += tx.gas_limit();
        total_bytes_size += tx.size();

        // append transaction to the list of executed transactions
        mined_transactions.push(tx.hash());
        transactions.push(tx.into_signed());
    }

    // TODO: use ms for batch and sec for final block?
    //
    // sometimes batch are produced too quickly in certain configs (<1s diff)
    // resulting in batch timestamp == parent timestamp
    //
    // TODO: check for this error at the quorum waiter level?
    let mut timestamp = now();
    if timestamp == parent_info.tip.timestamp {
        warn!(target: "worker::block_builder", "new block timestamp same as parent - setting offset by 1sec");
        timestamp = parent_info.tip.timestamp + 1;
    }

    // batch
    let worker_block = WorkerBlock {
        transactions,
        parent_hash,
        beneficiary,
        timestamp,
        base_fee_per_gas: Some(parent_info.pending_block_base_fee),
        received_at: None,
    };

    // return output
    BlockBuilderOutput { worker_block, mined_transactions }
}
