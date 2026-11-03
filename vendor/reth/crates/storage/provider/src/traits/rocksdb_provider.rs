use crate::{
    either_writer::{RawRocksDBBatch, RocksBatchArg, RocksTxRefArg},
    providers::RocksDBProvider,
};
use reth_storage_api::StorageSettingsCache;
use reth_storage_errors::provider::ProviderResult;

/// `RocksDB` provider factory.
///
/// This trait provides access to the `RocksDB` provider
pub trait RocksDBProviderFactory {
    /// Returns the `RocksDB` provider.
    fn rocksdb_provider(&self) -> RocksDBProvider;

    /// Executes a closure with a `RocksDB` transaction for reading.
    ///
    /// This helper encapsulates all the cfg-gated `RocksDB` transaction handling for reads.
    /// On legacy MDBX-only nodes (where `any_in_rocksdb()` is false), this skips creating
    /// the `RocksDB` transaction entirely, avoiding unnecessary overhead.
    fn with_rocksdb_tx<F, R>(&self, f: F) -> ProviderResult<R>
    where
        Self: StorageSettingsCache,
        F: FnOnce(RocksTxRefArg<'_>) -> ProviderResult<R>,
    {
        f(())
    }

    /// Executes a closure with a `RocksDB` batch, automatically registering it for commit.
    ///
    /// This helper encapsulates all the cfg-gated `RocksDB` batch handling.
    fn with_rocksdb_batch<F, R>(&self, f: F) -> ProviderResult<R>
    where
        F: FnOnce(RocksBatchArg<'_>) -> ProviderResult<(R, Option<RawRocksDBBatch>)>,
    {
        {
            let (result, _) = f(())?;
            Ok(result)
        }
    }

    /// Executes a closure with a `RocksDB` batch that auto-commits on threshold.
    ///
    /// Unlike [`Self::with_rocksdb_batch`], this uses a batch that automatically commits
    /// when it exceeds the size threshold, preventing OOM during large bulk writes.
    /// The consistency check on startup heals any crash between auto-commits.
    fn with_rocksdb_batch_auto_commit<F, R>(&self, f: F) -> ProviderResult<R>
    where
        F: FnOnce(RocksBatchArg<'_>) -> ProviderResult<(R, Option<RawRocksDBBatch>)>,
    {
        {
            let (result, _) = f(())?;
            Ok(result)
        }
    }
}
