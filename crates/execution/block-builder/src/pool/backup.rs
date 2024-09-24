//! Logic for backing up the transaction pool in case of node crash.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use reth_errors::ProviderError;
use reth_execution_types::ChangedAccount;
use reth_fs_util::FsPathError;
use reth_primitives::{Address, BlockHash, IntoRecoveredTransaction, TransactionSigned, U256};
use reth_provider::{ExecutionOutcome, StateProviderFactory};
use reth_transaction_pool::{
    error::PoolError, PoolTransaction, TransactionOrigin, TransactionPool,
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
