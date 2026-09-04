//! Types for testing only.

use crate::{build_batch, BatchBuilderOutput};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};
use tn_reth::{
    new_pool_txn, BestTransactions, InvalidPoolTransactionError, PeerBatchTxs, PoolTxn, PoolTxnId,
    SenderId, SenderIdentifiers, TxPool,
};
use tn_types::{Address, Batch, BatchBuilderArgs, Recovered, TransactionTrait as _, TxHash, U256};

/// Attempt to update batch with accurate header information.
///
/// NOTE: this is loosely based on reth's auto-seal consensus
/// NOTE2: this assumes worker 0.
pub fn execute_test_batch(test_batch: &mut Batch) {
    let pool = TestPool::new(&test_batch.transactions);

    let args =
        BatchBuilderArgs { pool, beneficiary: test_batch.beneficiary, epoch: test_batch.epoch };
    let BatchBuilderOutput { batch, .. } = build_batch(args, 0, test_batch.base_fee_per_gas);
    test_batch.beneficiary = batch.beneficiary;
    // Don't reset base_fee_per_gas, some tests need that value to remain.
}

/// The deferral TTL every [`TestPool`] uses.
///
/// Long enough that a build started after [`TxPool::record_peer_batch`] always observes the
/// deferral, so builder tests never race the clock.
const TEST_PEER_BATCH_TTL: Duration = Duration::from_secs(3600);

/// A test pool that ensures every transaction is in the pending pool
#[derive(Clone, Debug)]
pub(crate) struct TestPool {
    transactions: Vec<Arc<PoolTxn>>,
    by_id: BTreeMap<PoolTxnId, Arc<PoolTxn>>,
    /// Per-sender balances returned by [`TxPool::get_account_balance`]. A sender that is absent
    /// here reports [`U256::MAX`], preserving the behavior of tests that do not exercise balance.
    balances: BTreeMap<Address, U256>,
    /// Transactions seen inside a validated peer batch, deferred by the builder (issue #1329).
    peer_batch_txs: PeerBatchTxs,
}

impl Default for TestPool {
    fn default() -> Self {
        Self {
            transactions: Vec::new(),
            by_id: BTreeMap::new(),
            balances: BTreeMap::new(),
            peer_batch_txs: PeerBatchTxs::new(TEST_PEER_BATCH_TTL),
        }
    }
}

impl TxPool for TestPool {
    fn best_transactions(&self) -> tn_reth::BestTxns {
        tn_reth::BestTxns::new_for_test(self.best_transactions_int())
    }
    fn remove_eip4844_txs(&mut self, _blobs: Vec<TxHash>) {
        // remove EIP-4844 transactions from the transactions vec and btreemap
        self.transactions.retain(|tx| !tx.is_eip4844());
        self.by_id.retain(|_, tx| !tx.is_eip4844());
    }
    fn remove_unsupported_txs(&mut self, _txs: Vec<TxHash>) {
        // remove non-allowlisted transaction types from the transactions vec and btreemap
        self.transactions.retain(|tx| tn_types::batch_allowlisted_tx_type(&tx.transaction));
        self.by_id.retain(|_, tx| tn_types::batch_allowlisted_tx_type(&tx.transaction));
    }
    fn get_account_balance(&self, address: Address) -> U256 {
        self.balances.get(&address).copied().unwrap_or(U256::MAX)
    }
    fn record_peer_batch(&self, hashes: &[TxHash]) {
        self.peer_batch_txs.record(hashes)
    }
    fn is_peer_deferred(&self, hash: &TxHash) -> bool {
        self.peer_batch_txs.is_deferred(hash)
    }
}

impl TestPool {
    /// Override the balance [`TxPool::get_account_balance`] reports for `address`.
    #[cfg(test)]
    pub(crate) fn with_balance(mut self, address: Address, balance: U256) -> Self {
        self.balances.insert(address, balance);
        self
    }

    /// Sum of the pool transactions' costs, used by tests to compute the expected optimistic
    /// balance debit.
    #[cfg(test)]
    pub(crate) fn total_cost(&self) -> U256 {
        self.transactions.iter().fold(U256::ZERO, |acc, tx| acc.saturating_add(*tx.cost()))
    }

    /// Create a new instance of Self.
    pub(crate) fn new(txs: &[Vec<u8>]) -> Self {
        let mut sender_ids = SenderIdentifiers::default();
        let mut by_id = Vec::with_capacity(txs.len());
        let transactions = txs
            .iter()
            .map(|tx| {
                let ecrecovered: Recovered<_> =
                    tn_reth::recover_raw_transaction(tx).expect("tx into ecrecovered");
                let nonce = ecrecovered.nonce();
                // add to sender ids
                let id = sender_ids.sender_id_or_create(ecrecovered.signer());
                let transaction =
                    tn_reth::recover_pooled_transaction(tx).expect("pooled tx from recovered");

                let transaction_id = PoolTxnId::new(id, nonce);

                let valid_tx = Arc::new(new_pool_txn(transaction, transaction_id));
                // add by id
                by_id.push((transaction_id, valid_tx.clone()));

                valid_tx
            })
            .collect();
        Self { transactions, by_id: by_id.into_iter().collect(), ..Default::default() }
    }

    fn best_transactions_int(&self) -> Box<dyn BestTransactions<Item = Arc<PoolTxn>>> {
        let mut independent = VecDeque::new();

        // see reth::transaction-pool::pool::pending::update_independents_and_highest_nonces()
        //
        // if there's __no__ ancestor, then this transaction is independent
        // guaranteed because the pool is gapless
        for tx in self.transactions.iter() {
            if tx.transaction_id.unchecked_ancestor().and_then(|id| self.by_id.get(&id)).is_none() {
                independent.push_back(tx.clone())
            }
        }

        Box::new(BestTestTransactions {
            all: self.by_id.clone(),
            independent,
            invalid: Default::default(),
            skip_blobs: true,
        })
    }
}

/// Type for pulling best transactions from the pool.
///
/// An iterator that returns transactions that can be executed on the current state (*best*
/// transactions).
///
/// The [`PendingPool`](crate::pool::pending::PendingPool) contains transactions that *could* all
/// be executed on the current state, but only yields transactions that are ready to be executed
/// now. While it contains all gapless transactions of a sender, it _always_ only returns the
/// transaction with the current on chain nonce.
struct BestTestTransactions {
    /// Contains a copy of _all_ transactions of the pending pool at the point in time this
    /// iterator was created.
    all: BTreeMap<PoolTxnId, Arc<PoolTxn>>,
    /// Transactions that can be executed right away: these have the expected nonce.
    ///
    /// Once an `independent` transaction with the nonce `N` is returned, it unlocks `N+1`, which
    /// then can be moved from the `all` set to the `independent` set.
    independent: VecDeque<Arc<PoolTxn>>,
    /// There might be the case where a yielded transactions is invalid, this will track it.
    ///
    /// Senders, not hashes, mirroring reth's `BestTransactions`: marking a transaction invalid
    /// must also skip its descendants (the sender's later nonces), which are already unlocked by
    /// the time the caller marks it. Tracking hashes alone would still yield the successor and
    /// pack a nonce-gapped batch.
    invalid: HashSet<SenderId>,
    /// Flag to control whether to skip blob transactions (EIP4844).
    skip_blobs: bool,
}

impl BestTestTransactions {
    /// Mark the transaction and it's descendants as invalid.
    fn mark_invalid(&mut self, tx: &Arc<PoolTxn>) {
        self.invalid.insert(tx.sender_id());
    }
}

impl BestTransactions for BestTestTransactions {
    fn mark_invalid(&mut self, tx: &Self::Item, _kind: &InvalidPoolTransactionError) {
        Self::mark_invalid(self, tx)
    }

    fn no_updates(&mut self) {
        unimplemented!()
    }

    fn skip_blobs(&mut self) {
        self.set_skip_blobs(true);
    }

    fn set_skip_blobs(&mut self, skip_blobs: bool) {
        self.skip_blobs = skip_blobs;
    }
}

impl Iterator for BestTestTransactions {
    type Item = Arc<PoolTxn>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // remove the next independent tx (created with `push_back`)
            let best = self.independent.pop_front()?.clone();
            let hash = best.transaction.transaction().hash();

            // skip transactions whose sender was marked invalid (this transaction or an ancestor)
            if self.invalid.contains(&best.sender_id()) {
                tracing::debug!(
                    target: "test-txpool",
                    "[{:?}] skipping invalid transaction",
                    hash
                );
                continue;
            }

            // Insert transactions that just got unlocked.
            if let Some(unlocked) = self.all.get(&best.transaction_id.descendant()) {
                self.independent.push_back(unlocked.clone());
            }

            if self.skip_blobs && best.is_eip4844() {
                // blobs should be skipped, marking the as invalid will ensure that no dependent
                // transactions are returned
                self.mark_invalid(&best)
            } else {
                return Some(best);
            }
        }
    }
}
