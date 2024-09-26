//! Logic for backing up the transaction pool in case of node crash.

use reth_errors::ProviderError;
use reth_execution_types::ChangedAccount;
use reth_fs_util::FsPathError;
use reth_primitives::{Address, BlockHash, IntoRecoveredTransaction, TransactionSigned, U256};
use reth_provider::{ExecutionOutcome, StateProviderFactory};
use reth_transaction_pool::{
    error::PoolError, PoolTransaction, TransactionOrigin, TransactionPool,
};
use std::{
    borrow::Borrow,
    collections::HashSet,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
};
use tracing::{debug, error, info, trace, warn};

/// Settings for local transaction backup task
#[derive(Debug, Clone, Default)]
pub struct LocalTransactionBackupConfig {
    /// Path to transactions backup file
    pub transactions_path: Option<PathBuf>,
}

impl LocalTransactionBackupConfig {
    /// Receive path to transactions backup and return initialized config
    pub const fn with_local_txs_backup(transactions_path: PathBuf) -> Self {
        Self { transactions_path: Some(transactions_path) }
    }
}

#[derive(Default)]
pub(super) struct LoadedAccounts {
    /// All accounts that were loaded
    pub(super) accounts: Vec<ChangedAccount>,
    /// All accounts that failed to load
    pub(super) failed_to_load: Vec<Address>,
}

/// A unique `ChangedAccount` identified by its address that can be used for deduplication
#[derive(Eq)]
struct ChangedAccountEntry(ChangedAccount);

impl PartialEq for ChangedAccountEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.address == other.0.address
    }
}

impl Hash for ChangedAccountEntry {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.address.hash(state);
    }
}

impl Borrow<Address> for ChangedAccountEntry {
    fn borrow(&self) -> &Address {
        &self.0.address
    }
}

/// Loads all accounts at the given state
///
/// Returns an error with all given addresses if the state is not available.
///
/// Note: this expects _unique_ addresses
pub(super) fn load_accounts<Provider, I>(
    provider: Provider,
    at: BlockHash,
    addresses: I,
) -> Result<LoadedAccounts, Box<(HashSet<Address>, ProviderError)>>
where
    I: IntoIterator<Item = Address>,

    Provider: StateProviderFactory,
{
    let addresses = addresses.into_iter();
    let mut res = LoadedAccounts::default();
    let state = match provider.history_by_block_hash(at) {
        Ok(state) => state,
        Err(err) => return Err(Box::new((addresses.collect(), err))),
    };
    for addr in addresses {
        if let Ok(maybe_acc) = state.basic_account(addr) {
            let acc = maybe_acc
                .map(|acc| ChangedAccount { address: addr, nonce: acc.nonce, balance: acc.balance })
                .unwrap_or_else(|| ChangedAccount { address: addr, nonce: 0, balance: U256::ZERO });
            res.accounts.push(acc)
        } else {
            // failed to load account.
            res.failed_to_load.push(addr);
        }
    }
    Ok(res)
}

/// Extracts all changed accounts from the `BundleState`
pub(crate) fn changed_accounts_iter(
    execution_outcome: &ExecutionOutcome,
) -> impl Iterator<Item = ChangedAccount> + '_ {
    execution_outcome
        .accounts_iter()
        .filter_map(|(addr, acc)| acc.map(|acc| (addr, acc)))
        .map(|(address, acc)| ChangedAccount { address, nonce: acc.nonce, balance: acc.balance })
}

/// Loads transactions from a file, decodes them from the RLP format, and inserts them
/// into the transaction pool on node boot up.
/// The file is removed after the transactions have been successfully processed.
async fn load_and_reinsert_transactions<P>(
    pool: P,
    file_path: &Path,
) -> Result<(), TransactionsBackupError>
where
    P: TransactionPool,
{
    if !file_path.exists() {
        return Ok(());
    }

    debug!(target: "txpool", txs_file =?file_path, "Check local persistent storage for saved transactions");
    let data = reth_fs_util::read(file_path)?;

    if data.is_empty() {
        return Ok(());
    }

    let txs_signed: Vec<TransactionSigned> = alloy_rlp::Decodable::decode(&mut data.as_slice())?;

    let pool_transactions = txs_signed
        .into_iter()
        .filter_map(|tx| tx.try_ecrecovered())
        .filter_map(|tx| {
            // Filter out errors
            <P::Transaction as PoolTransaction>::try_from_consensus(tx).ok()
        })
        .collect::<Vec<_>>();

    let outcome = pool.add_transactions(TransactionOrigin::Local, pool_transactions).await;

    info!(target: "txpool", txs_file =?file_path, num_txs=%outcome.len(), "Successfully reinserted local transactions from file");
    reth_fs_util::remove_file(file_path)?;
    Ok(())
}

fn save_local_txs_backup<P>(pool: P, file_path: &Path)
where
    P: TransactionPool,
{
    let local_transactions = pool.get_local_transactions();
    if local_transactions.is_empty() {
        trace!(target: "txpool", "no local transactions to save");
        return;
    }

    let local_transactions = local_transactions
        .into_iter()
        .map(|tx| tx.to_recovered_transaction().into_signed())
        .collect::<Vec<_>>();

    let num_txs = local_transactions.len();
    let mut buf = Vec::new();
    alloy_rlp::encode_list(&local_transactions, &mut buf);
    info!(target: "txpool", txs_file =?file_path, num_txs=%num_txs, "Saving current local transactions");
    let parent_dir = file_path.parent().map(std::fs::create_dir_all).transpose();

    match parent_dir.map(|_| reth_fs_util::write(file_path, buf)) {
        Ok(_) => {
            info!(target: "txpool", txs_file=?file_path, "Wrote local transactions to file");
        }
        Err(err) => {
            warn!(target: "txpool", %err, txs_file=?file_path, "Failed to write local transactions to file");
        }
    }
}

/// Errors possible during txs backup load and decode
#[derive(thiserror::Error, Debug)]
pub enum TransactionsBackupError {
    /// Error during RLP decoding of transactions
    #[error("failed to apply transactions backup. Encountered RLP decode error: {0}")]
    Decode(#[from] alloy_rlp::Error),
    /// Error during file upload
    #[error("failed to apply transactions backup. Encountered file error: {0}")]
    FsPath(#[from] FsPathError),
    /// Error adding transactions to the transaction pool
    #[error("failed to insert transactions to the transactions pool. Encountered pool error: {0}")]
    Pool(#[from] PoolError),
}

/// Task which manages saving local transactions to the persistent file in case of shutdown.
/// Reloads the transactions from the file on the boot up and inserts them into the pool.
pub async fn backup_local_transactions_task<P>(
    shutdown: reth_tasks::shutdown::GracefulShutdown,
    pool: P,
    config: LocalTransactionBackupConfig,
) where
    P: TransactionPool + Clone,
{
    let Some(transactions_path) = config.transactions_path else {
        // nothing to do
        return;
    };

    if let Err(err) = load_and_reinsert_transactions(pool.clone(), &transactions_path).await {
        error!(target: "txpool", "{}", err)
    }

    let graceful_guard = shutdown.await;

    // write transactions to disk
    save_local_txs_backup(pool, &transactions_path);

    drop(graceful_guard)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_chainspec::MAINNET;
    use reth_fs_util as fs;
    use reth_primitives::{hex, PooledTransactionsElement, U256};
    use reth_provider::test_utils::{ExtendedAccount, MockEthProvider};
    use reth_tasks::TaskManager;
    use reth_transaction_pool::{
        blobstore::InMemoryBlobStore, validate::EthTransactionValidatorBuilder,
        CoinbaseTipOrdering, EthPooledTransaction, Pool,
    };

    #[test]
    fn changed_acc_entry() {
        // empty changed account
        let changed_acc = ChangedAccountEntry(ChangedAccount {
            address: Address::random(),
            nonce: 0,
            balance: U256::ZERO,
        });
        let mut copy = changed_acc.0;
        copy.nonce = 10;
        assert!(changed_acc.eq(&ChangedAccountEntry(copy)));
    }

    const EXTENSION: &str = "rlp";
    const FILENAME: &str = "test_transactions_backup";

    #[tokio::test(flavor = "multi_thread")]
    async fn test_save_local_txs_backup() {
        let temp_dir = tempfile::tempdir().unwrap();
        let transactions_path = temp_dir.path().join(FILENAME).with_extension(EXTENSION);
        let tx_bytes = hex!("02f87201830655c2808505ef61f08482565f94388c818ca8b9251b393131c08a736a67ccb192978801049e39c4b5b1f580c001a01764ace353514e8abdfb92446de356b260e3c1225b73fc4c8876a6258d12a129a04f02294aa61ca7676061cd99f29275491218b4754b46a0248e5e42bc5091f507");
        let tx = PooledTransactionsElement::decode_enveloped(&mut &tx_bytes[..]).unwrap();
        let provider = MockEthProvider::default();
        let transaction = EthPooledTransaction::from_recovered_pooled_transaction(
            tx.try_into_ecrecovered().unwrap(),
        );
        let tx_to_cmp = transaction.clone();
        let sender = hex!("1f9090aaE28b8a3dCeaDf281B0F12828e676c326").into();
        provider.add_account(sender, ExtendedAccount::new(42, U256::MAX));
        let blob_store = InMemoryBlobStore::default();
        let validator = EthTransactionValidatorBuilder::new(MAINNET.clone())
            .build(provider, blob_store.clone());

        let txpool = Pool::new(
            validator.clone(),
            CoinbaseTipOrdering::default(),
            blob_store.clone(),
            Default::default(),
        );

        txpool.add_transaction(TransactionOrigin::Local, transaction.clone()).await.unwrap();

        let handle = tokio::runtime::Handle::current();
        let manager = TaskManager::new(handle);
        let config = LocalTransactionBackupConfig::with_local_txs_backup(transactions_path.clone());
        manager.executor().spawn_critical_with_graceful_shutdown_signal("test task", |shutdown| {
            backup_local_transactions_task(shutdown, txpool.clone(), config)
        });

        let mut txns = txpool.get_local_transactions();
        let tx_on_finish = txns.pop().expect("there should be 1 transaction");

        assert_eq!(*tx_to_cmp.hash(), *tx_on_finish.hash());

        // shutdown the executor
        manager.graceful_shutdown();

        let data = fs::read(transactions_path).unwrap();

        let txs: Vec<TransactionSigned> =
            alloy_rlp::Decodable::decode(&mut data.as_slice()).unwrap();
        assert_eq!(txs.len(), 1);

        temp_dir.close().unwrap();
    }
}
