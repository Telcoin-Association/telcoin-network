//! Types for testing only.

use crate::{build_batch, BlockBuilderOutput};
use reth_primitives::{
    constants::MIN_PROTOCOL_BASE_FEE, Address, BlobTransactionSidecar, BlockBody,
    PooledTransactionsElement, SealedBlock, SealedHeader, TxHash,
};
use reth_transaction_pool::{
    identifier::{SenderIdentifiers, TransactionId},
    AllPoolTransactions, AllTransactionsEvents, BestTransactions, BestTransactionsAttributes,
    BlobStoreError, BlockInfo, EthPooledTransaction, GetPooledTransactionLimit, NewBlobSidecar,
    NewTransactionEvent, PoolResult, PoolSize, PropagatedTransactions, TransactionEvents,
    TransactionListenerKind, TransactionOrigin, TransactionPool, ValidPoolTransaction,
};
use std::{
    collections::{BTreeMap, HashSet, VecDeque},
    sync::Arc,
    time::Instant,
};
use tn_types::{
    Batch, BatchBuilderArgs, LastCanonicalUpdate, PendingBlockConfig, TransactionSigned,
};
use tokio::sync::mpsc::{self, Receiver};

/// Attempt to update batch with accurate header information.
///
/// NOTE: this is loosely based on reth's auto-seal consensus
pub fn execute_test_batch(test_batch: &mut Batch, parent: &SealedHeader) {
    let pool = TestPool::new(test_batch.transactions.clone());

    let parent_info = LastCanonicalUpdate {
        tip: SealedBlock::new(parent.clone(), BlockBody::default()),
        pending_block_base_fee: test_batch.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE),
        pending_block_blob_fee: None,
    };

    let batch_config = PendingBlockConfig::new(test_batch.beneficiary, parent_info);
    let args = BatchBuilderArgs { pool, batch_config };
    let BlockBuilderOutput { batch, .. } = build_batch(args);
    test_batch.parent_hash = batch.parent_hash;
    test_batch.beneficiary = batch.beneficiary;
    test_batch.timestamp = batch.timestamp;
    test_batch.base_fee_per_gas = batch.base_fee_per_gas;
}

/// A test pool that ensures every transaction is in the pending pool
#[derive(Default, Clone, Debug)]
struct TestPool {
    _sender_ids: Arc<SenderIdentifiers>,
    transactions: Vec<Arc<ValidPoolTransaction<EthPooledTransaction>>>,
    by_id: BTreeMap<TransactionId, Arc<ValidPoolTransaction<EthPooledTransaction>>>,
}

impl TestPool {
    /// Create a new instance of Self.
    fn new(txs: Vec<TransactionSigned>) -> Self {
        let mut sender_ids = SenderIdentifiers::default();
        let mut by_id = Vec::with_capacity(txs.len());
        let transactions = txs
            .into_iter()
            .map(|tx| {
                let ecrecovered = tx.try_into_ecrecovered().expect("tx into ecrecovered");
                let nonce = ecrecovered.nonce();
                // add to sender ids
                let id = sender_ids.sender_id_or_create(ecrecovered.signer());
                let transaction = EthPooledTransaction::try_from(ecrecovered)
                    .expect("ecrecovered into pooled tx");
                let transaction_id = TransactionId::new(id, nonce);

                let valid_tx = Arc::new(ValidPoolTransaction {
                    transaction,
                    transaction_id,
                    propagate: false,
                    timestamp: Instant::now(),
                    origin: TransactionOrigin::External,
                });
                // add by id
                by_id.push((transaction_id, valid_tx.clone()));

                valid_tx
            })
            .collect();
        let _sender_ids = Arc::new(sender_ids);
        Self { _sender_ids, transactions, by_id: by_id.into_iter().collect() }
    }
}

impl TransactionPool for TestPool {
    type Transaction = EthPooledTransaction;

    fn pool_size(&self) -> PoolSize {
        Default::default()
    }

    fn block_info(&self) -> BlockInfo {
        BlockInfo {
            last_seen_block_hash: Default::default(),
            last_seen_block_number: 0,
            pending_basefee: 0,
            pending_blob_fee: None,
        }
    }

    async fn add_transaction_and_subscribe(
        &self,
        _origin: TransactionOrigin,
        _transaction: Self::Transaction,
    ) -> PoolResult<TransactionEvents> {
        // let hash = *transaction.hash();
        // Err(PoolError::other(hash, Box::new(NoopInsertError::new(transaction))))
        unimplemented!()
    }

    async fn add_transaction(
        &self,
        _origin: TransactionOrigin,
        _transaction: Self::Transaction,
    ) -> PoolResult<TxHash> {
        // let hash = *transaction.hash();
        // Err(PoolError::other(hash, Box::new(NoopInsertError::new(transaction))))
        unimplemented!()
    }

    async fn add_transactions(
        &self,
        _origin: TransactionOrigin,
        _transactions: Vec<Self::Transaction>,
    ) -> Vec<PoolResult<TxHash>> {
        // transactions
        //     .into_iter()
        //     .map(|transaction| {
        //         let hash = *transaction.hash();
        //         Err(PoolError::other(hash, Box::new(NoopInsertError::new(transaction))))
        //     })
        //     .collect()
        unimplemented!()
    }

    fn transaction_event_listener(&self, _tx_hash: TxHash) -> Option<TransactionEvents> {
        None
    }

    fn all_transactions_event_listener(&self) -> AllTransactionsEvents<Self::Transaction> {
        // AllTransactionsEvents::new(mpsc::channel(1).1)
        unimplemented!()
    }

    fn pending_transactions_listener_for(
        &self,
        _kind: TransactionListenerKind,
    ) -> Receiver<TxHash> {
        mpsc::channel(1).1
    }

    fn new_transactions_listener(&self) -> Receiver<NewTransactionEvent<Self::Transaction>> {
        mpsc::channel(1).1
    }

    fn blob_transaction_sidecars_listener(&self) -> Receiver<NewBlobSidecar> {
        mpsc::channel(1).1
    }

    fn new_transactions_listener_for(
        &self,
        _kind: TransactionListenerKind,
    ) -> Receiver<NewTransactionEvent<Self::Transaction>> {
        mpsc::channel(1).1
    }

    fn pooled_transaction_hashes(&self) -> Vec<TxHash> {
        vec![]
    }

    fn pooled_transaction_hashes_max(&self, _max: usize) -> Vec<TxHash> {
        vec![]
    }

    fn pooled_transactions(&self) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        vec![]
    }

    fn pooled_transactions_max(
        &self,
        _max: usize,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        vec![]
    }

    fn get_pooled_transaction_elements(
        &self,
        _tx_hashes: Vec<TxHash>,
        _limit: GetPooledTransactionLimit,
    ) -> Vec<PooledTransactionsElement> {
        vec![]
    }

    fn get_pooled_transaction_element(
        &self,
        _tx_hash: TxHash,
    ) -> Option<PooledTransactionsElement> {
        None
    }

    fn best_transactions(
        &self,
    ) -> Box<dyn BestTransactions<Item = Arc<ValidPoolTransaction<Self::Transaction>>>> {
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

    fn best_transactions_with_base_fee(
        &self,
        _: u64,
    ) -> Box<dyn BestTransactions<Item = Arc<ValidPoolTransaction<Self::Transaction>>>> {
        Box::new(std::iter::empty())
    }

    fn best_transactions_with_attributes(
        &self,
        _: BestTransactionsAttributes,
    ) -> Box<dyn BestTransactions<Item = Arc<ValidPoolTransaction<Self::Transaction>>>> {
        Box::new(std::iter::empty())
    }

    fn pending_transactions(&self) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        vec![]
    }

    fn queued_transactions(&self) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        vec![]
    }

    fn all_transactions(&self) -> AllPoolTransactions<Self::Transaction> {
        AllPoolTransactions::default()
    }

    fn remove_transactions(
        &self,
        _hashes: Vec<TxHash>,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        vec![]
    }

    fn retain_unknown<A>(&self, _announcement: &mut A)
    //where
    // A: HandleMempoolData,
    {
    }

    fn get(&self, _tx_hash: &TxHash) -> Option<Arc<ValidPoolTransaction<Self::Transaction>>> {
        None
    }

    fn get_all(&self, _txs: Vec<TxHash>) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        vec![]
    }

    fn on_propagated(&self, _txs: PropagatedTransactions) {}

    fn get_transactions_by_sender(
        &self,
        _sender: Address,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        vec![]
    }

    fn get_transaction_by_sender_and_nonce(
        &self,
        _sender: Address,
        _nonce: u64,
    ) -> Option<Arc<ValidPoolTransaction<Self::Transaction>>> {
        None
    }

    fn get_transactions_by_origin(
        &self,
        _origin: TransactionOrigin,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        vec![]
    }

    fn unique_senders(&self) -> HashSet<Address> {
        Default::default()
    }

    fn get_blob(&self, _tx_hash: TxHash) -> Result<Option<BlobTransactionSidecar>, BlobStoreError> {
        Ok(None)
    }

    fn get_all_blobs(
        &self,
        _tx_hashes: Vec<TxHash>,
    ) -> Result<Vec<(TxHash, BlobTransactionSidecar)>, BlobStoreError> {
        Ok(vec![])
    }

    fn get_all_blobs_exact(
        &self,
        tx_hashes: Vec<TxHash>,
    ) -> Result<Vec<BlobTransactionSidecar>, BlobStoreError> {
        if tx_hashes.is_empty() {
            return Ok(vec![]);
        }
        Err(BlobStoreError::MissingSidecar(tx_hashes[0]))
    }

    fn get_pending_transactions_by_origin(
        &self,
        _origin: TransactionOrigin,
    ) -> Vec<Arc<ValidPoolTransaction<Self::Transaction>>> {
        vec![]
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
    all: BTreeMap<TransactionId, Arc<ValidPoolTransaction<EthPooledTransaction>>>,
    /// Transactions that can be executed right away: these have the expected nonce.
    ///
    /// Once an `independent` transaction with the nonce `N` is returned, it unlocks `N+1`, which
    /// then can be moved from the `all` set to the `independent` set.
    independent: VecDeque<Arc<ValidPoolTransaction<EthPooledTransaction>>>,
    /// There might be the case where a yielded transactions is invalid, this will track it.
    invalid: HashSet<TxHash>,
    /// Flag to control whether to skip blob transactions (EIP4844).
    skip_blobs: bool,
}

impl BestTestTransactions {
    /// Mark the transaction and it's descendants as invalid.
    fn mark_invalid(&mut self, tx: &Arc<ValidPoolTransaction<EthPooledTransaction>>) {
        self.invalid.insert(*tx.hash());
    }
}

impl BestTransactions for BestTestTransactions {
    fn mark_invalid(&mut self, tx: &Self::Item) {
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
    type Item = Arc<ValidPoolTransaction<EthPooledTransaction>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // remove the next independent tx (created with `push_back`)
            let best = self.independent.pop_front()?.clone();
            let hash = best.transaction.transaction().hash();

            // skip transactions that were marked as invalid
            if self.invalid.contains(&hash) {
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
