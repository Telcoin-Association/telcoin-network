//! The logic for building batches.
//!
//! Transactions are pulled from the worker's pending pool and added to the block without being
//! executed. Block size is measured in bytes and a transaction's max gas limit. The block is sealed
//! when the pending pool devoid of transactions or the max block size is reached (wei or bytes).
//!
//! The mined transactions are returned with the built block so the worker can update the pool.

use std::collections::HashMap;
use tn_reth::{ChangedAccount, TxPool, TxnSize};
use tn_types::{
    max_batch_gas, max_batch_size, Address, Batch, BatchBuilderArgs, Encodable2718 as _,
    TransactionTrait as _, TxHash, WorkerId, U256,
};
use tracing::debug;

/// The output from building the next block.
///
/// Contains information needed to update the transaction pool.
#[derive(Debug)]
pub struct BatchBuilderOutput {
    /// The batch info for the worker to propose.
    pub(crate) batch: Batch,
    /// The transaction hashes mined in this worker's batch.
    pub(crate) mined_transactions: Vec<TxHash>,
    /// Per-sender pool updates applied after mining.
    ///
    /// Each entry advances the sender's nonce past the mined transactions so remaining
    /// transactions from the same sender stay in the pending sub-pool rather than being demoted
    /// to queued over a perceived nonce gap. The balance is the sender's real canonical balance
    /// minus the just-mined cost, so parked insufficient-funds transactions are not spuriously
    /// promoted (see `build_batch`).
    pub(crate) changed_accounts: Vec<ChangedAccount>,
}

/// Construct an TN batch using the best transactions from the pool.
///
/// Returns the [`BatchBuilderOutput`] and cannot fail. The batch continues to add
/// transactions to the proposed block until either:
/// - accumulated transaction gas limit reached (measured by tx.gas_limit())
/// - max byte size of transactions (measured by tx.size())
///
/// NOTE: it's possible to under utilize resources if users submit transactions
/// with very high gas limits. It's impossible to know the amount of gas a transaction
/// will use without executing it, and the worker does not execute transactions.
#[inline]
pub fn build_batch<P: TxPool>(
    args: BatchBuilderArgs<P>,
    worker_id: WorkerId,
    base_fee: u64,
) -> BatchBuilderOutput {
    let BatchBuilderArgs { mut pool, beneficiary, epoch } = args;
    let gas_limit = max_batch_gas(epoch);
    let max_size = max_batch_size(epoch);
    let base_fee_per_gas = base_fee;

    // NOTE: this obtains a `read` lock on the tx pool
    // pull best transactions and rely on watch channel to ensure basefee is current
    let mut best_txs = pool.best_transactions();

    // NOTE: batches always build off the latest finalized block

    // collect data for successful transactions
    // let mut sum_blob_gas_used = 0;
    let mut total_bytes_size = 0;
    let mut total_possible_gas = 0;
    let mut transactions = Vec::new();
    let mut mined_transactions = Vec::new();
    let mut blob_transactions = Vec::new();
    let mut unsupported_transactions = Vec::new();
    let mut sender_nonces: HashMap<Address, u64> = HashMap::new();
    let mut sender_costs: HashMap<Address, U256> = HashMap::new();

    // begin loop through sorted "best" transactions in pending pool
    // and execute them to build the block
    while let Some(pool_tx) = best_txs.next() {
        // ensure block has capacity (in gas) for this transaction
        if total_possible_gas + pool_tx.gas_limit() > gas_limit {
            // the tx could exceed max gas limit for the block
            // marking as invalid within the context of the `BestTransactions` pulled in this
            // current iteration  all dependents for this transaction are now considered invalid
            // before continuing loop
            best_txs.exceeds_gas_limit(&pool_tx, gas_limit);
            debug!(target: "worker::batch_builder", ?pool_tx, "marking tx invalid due to gas constraint");
            continue;
        }

        // convert tx to a signed transaction
        //
        // NOTE: `ValidPoolTransaction::size()` is private
        let tx = pool_tx.to_consensus();

        // ignore any transaction type outside the executable allowlist (the
        // predicate admits legacy, EIP-2930, EIP-1559, and EIP-7702): the batch
        // validator rejects non-allowlisted batches, so packing one would cost
        // this node a peer penalty on every vote request. The 4844 arm discards
        // blob transactions; the else arm is default-deny for any future
        // decodable type outside the allowlist (no such type exists today)
        if !tn_types::batch_allowlisted_tx_type(&tx) {
            if tx.is_eip4844() {
                best_txs.ignore_eip4844(&pool_tx);
                debug!(target: "worker::batch_builder", ?pool_tx, "marking eip4844 tx invalid");
                blob_transactions.push(*tx.hash());
            } else {
                best_txs.ignore_denylist_type(&pool_tx);
                debug!(target: "worker::batch_builder", ?pool_tx, "marking non-allowlisted tx type invalid");
                unsupported_transactions.push(*tx.hash());
            }
            continue;
        }

        // ensure block has capacity (in bytes) for this transaction
        if total_bytes_size + tx.size() > max_size {
            // the tx could exceed max gas limit for the block
            // marking as invalid within the context of the `BestTransactions` pulled in this
            // current iteration  all dependents for this transaction are now considered invalid
            // before continuing loop
            best_txs.max_batch_size(&pool_tx, tx.size(), max_size);
            debug!(target: "worker::batch_builder", ?pool_tx, "marking tx invalid due to bytes constraint");
            continue;
        }

        // txs are not executed, so use the gas_limit
        total_possible_gas += tx.gas_limit();
        total_bytes_size += tx.size();

        // append transaction to the list of executed transactions
        mined_transactions.push(*pool_tx.hash());
        transactions.push(tx.into_inner().encoded_2718());

        // track max nonce per sender for pool state updates
        let sender = pool_tx.sender();
        let nonce = pool_tx.nonce();
        sender_nonces.entry(sender).and_modify(|max| *max = (*max).max(nonce)).or_insert(nonce);

        // accumulate the cost of the transactions mined for this sender so the optimistic balance
        // update below can debit them (see the changed_accounts construction)
        let cost = *pool_tx.cost();
        sender_costs
            .entry(sender)
            .and_modify(|total| *total = total.saturating_add(cost))
            .or_insert(cost);
    }

    // batch
    let batch =
        Batch { transactions, epoch, beneficiary, base_fee_per_gas, worker_id, received_at: None };

    // remove any blob transactions that were submitted
    pool.remove_eip4844_txs(blob_transactions);

    // remove any non-allowlisted transaction types that were submitted
    pool.remove_unsupported_txs(unsupported_transactions);

    // construct changed_accounts for the optimistic pool update
    //
    // The nonce is advanced past the mined transactions so remaining transactions from the same
    // sender stay in `pending` rather than being demoted over a perceived nonce gap.
    //
    // The balance is the sender's real canonical balance minus the cost of the transactions just
    // mined for that sender, NOT `U256::MAX`. `U256::MAX` made the pool treat a sender's parked
    // (insufficient-funds) transactions as affordable and promote them into the next batch built
    // inside this optimistic window; those transactions are then quorum-certified and gossiped
    // only to be skipped for free at execution, a griefing/amplification vector (issue #1158).
    // Debiting the just-mined cost keeps a sender's parked transactions parked when the sender
    // cannot actually fund them, so the amplification the issue's PoC relies on (queue many
    // transactions, mine one, watch the rest promote) no longer occurs.
    //
    // Residual: the balance is re-derived from the last committed canonical balance on every
    // in-window rebuild and does not accumulate debits across successive rebuilds before the
    // engine's canonical update lands, so a drip-fed sender can still have on the order of one
    // transaction promoted per rebuild. Fully closing that requires tracking cumulative optimistic
    // spend across rebuilds (or a state-aware check on the follow-up batch path); both are larger
    // design choices left to the maintainers. See the crate README.
    let changed_accounts: Vec<ChangedAccount> = sender_nonces
        .into_iter()
        .map(|(address, max_nonce)| ChangedAccount {
            address,
            nonce: max_nonce + 1, // next expected nonce
            balance: pool
                .get_account_balance(address)
                .saturating_sub(sender_costs.get(&address).copied().unwrap_or(U256::ZERO)),
        })
        .collect();

    // return output
    BatchBuilderOutput { batch, mined_transactions, changed_accounts }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::TestPool;
    use std::sync::Arc;
    use tn_reth::{test_utils::TransactionFactory, RethChainSpec};
    use tn_types::{test_genesis, BatchBuilderArgs, Bytes, MIN_PROTOCOL_BASE_FEE, U256};

    /// The optimistic `changed_accounts` update must carry the sender's real balance, not an
    /// inflated `U256::MAX`. An inflated balance lets the pool promote a sender's parked
    /// insufficient-funds transactions into the next batch, which is a griefing/amplification
    /// vector (see the module docs and `crates/batch-builder/README.md`). The nonce must still be
    /// advanced past the mined transactions so legitimate sequential senders keep flowing.
    #[test]
    fn changed_accounts_carry_real_balance_and_advance_nonce() {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let mut tx_factory = TransactionFactory::new();
        let sender = tx_factory.address();

        // two sequential transactions (nonces 0 and 1) from the same sender
        let tx0 = tx_factory.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            None,
            U256::from(1),
            Bytes::new(),
        );
        let tx1 = tx_factory.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            None,
            U256::from(1),
            Bytes::new(),
        );

        // the pool reports a large, finite balance for the sender
        let real_balance = U256::from(1_000_000_000_000_000_000u128);
        let pool = TestPool::new(&[tx0, tx1]).with_balance(sender, real_balance);
        // both transactions are mined, so their cost is debited from the optimistic balance
        let mined_cost = pool.total_cost();
        assert!(mined_cost > U256::ZERO, "mined transactions must have non-zero cost");

        let args = BatchBuilderArgs { pool, beneficiary: Address::ZERO, epoch: 0 };
        let BatchBuilderOutput { changed_accounts, .. } =
            build_batch(args, 0, MIN_PROTOCOL_BASE_FEE);

        let changed = changed_accounts
            .iter()
            .find(|account| account.address == sender)
            .expect("sender must have a changed_accounts entry after mining");

        // the optimistic balance is the real balance minus the just-mined cost, never U256::MAX
        assert_eq!(changed.balance, real_balance - mined_cost);
        assert_ne!(changed.balance, U256::MAX);
        // the nonce is still advanced past the highest mined nonce (throughput fix preserved)
        assert_eq!(changed.nonce, 2);
    }
}
