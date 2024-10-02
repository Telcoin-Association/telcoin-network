//! The logic for building worker blocks.
//!
//! This is called by the block_builder once pending transactions are picked up.
//!
//! The

use reth_primitives::{
    constants::EMPTY_WITHDRAWALS, proofs, Bloom, Bytes, Header, IntoRecoveredTransaction, TxHash,
    B256, EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::StateProviderFactory;
use reth_transaction_pool::{BestTransactionsAttributes, BlockInfo, TransactionPool};
use tn_types::{now, PendingBlockConfig, WorkerBlock, WorkerBlockBuilderArgs};
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::pool::LastCanonicalUpdate;

/// The output from building the next block.
///
/// Contains information needed to update the transaction pool.
pub(crate) struct BlockBuilderOutput {
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

/// Construct an TN worker block using the best transactions from the pool.
///
/// Returns the [`BlockBuilderOutput`] and cannot fail. The worker block continues to add
/// transactions to the proposed block until either:
/// - accumulated transaction gas limit reached (measured by tx.gas_limit())
/// - max byte size of transactions (measured by tx.size())
///
/// NOTE: it's possible to under utilize resources if users submit transactions
/// with very high gas limits. It's impossible to know the amount of gas a transaction
/// will use without executing it, and the worker does not execute transactions.
#[inline]
pub fn build_worker_block<Pool, Provider>(
    args: WorkerBlockBuilderArgs<Pool, Provider>,
    latest_update: &watch::Receiver<LastCanonicalUpdate>,
) -> BlockBuilderOutput
where
    Provider: StateProviderFactory,
    Pool: TransactionPool,
{
    let WorkerBlockBuilderArgs { provider, pool, block_config } = args;
    let PendingBlockConfig { chain_spec, beneficiary, gas_limit, max_size } = block_config;

    // now that pool is locked, read from watch channel for latest pool info
    //
    // this ensures that basefee is correct for the round because the pool
    // maintenance task can't update pool while this task holds the lock
    // let LastCanonicalUpdate { new_tip, pending_block_base_fee, pending_block_blob_fee } =
    //     *latest_update.borrow();
    let latest = latest_update.borrow();

    // NOTE: this holds a `read` lock on the tx pool
    // pull best transactions and rely on watch channel to ensure basefee is current
    let mut best_txs = pool.best_transactions();

    // NOTE: worker blocks always build off the latest finalized block
    let block_number = latest.new_tip.number + 1;
    let parent_hash = latest.new_tip.hash();

    // collect data for successful transactions
    // let mut sum_blob_gas_used = 0;
    let mut total_bytes_size = 0;
    let mut total_possible_gas = 0;
    let total_fees = U256::ZERO;
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

    let transactions_root = proofs::calculate_transaction_root(&transactions);

    // TODO: use ms for worker block and sec for final block?
    //
    // sometimes worker block are produced too quickly in certain configs (<1s)
    // resulting in batch timestamp == parent timestamp
    //
    // TODO: check for this error at the quorum waiter level?
    let mut timestamp = now();
    if timestamp == latest.new_tip.timestamp {
        warn!(target: "worker::block_builder", "new block timestamp same as parent - setting offset by 1sec");
        timestamp = latest.new_tip.timestamp + 1;
    }

    // create header
    //
    // NOTE: workers do not execute transactions. Peers validate:
    // - calculated transaction root
    // - all other roots are defaults
    // - use ZERO for hashes
    let header = Header {
        parent_hash,
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        beneficiary,
        state_root: B256::ZERO,
        transactions_root,
        receipts_root: B256::ZERO,
        withdrawals_root: Some(EMPTY_WITHDRAWALS),
        logs_bloom: Bloom::default(),
        timestamp,
        mix_hash: B256::ZERO,
        nonce: 0,
        base_fee_per_gas: Some(latest.pending_block_base_fee),
        number: block_number,
        gas_limit,
        difficulty: U256::ZERO,
        gas_used: total_possible_gas,
        extra_data: Bytes::default(),
        parent_beacon_block_root: None,
        blob_gas_used: None,
        excess_blob_gas: None,
        requests_root: None,
    };

    // worker block
    let worker_block =
        WorkerBlock { transactions, sealed_header: header.seal_slow(), received_at: None };

    // return output
    BlockBuilderOutput { worker_block, mined_transactions }
}
