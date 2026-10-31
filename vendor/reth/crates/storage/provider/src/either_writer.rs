//! Generic reader and writer abstractions for interacting with either database tables or static
//! files.

use std::{
    collections::BTreeSet,
    marker::PhantomData,
    ops::{Range, RangeInclusive},
};

use crate::{
    providers::{history_info, HistoryInfo, StaticFileProvider, StaticFileProviderRWRefMut},
    StaticFileProviderFactory,
};
use alloy_primitives::{map::HashMap, Address, BlockNumber, TxHash, TxNumber, B256};
use rayon::slice::ParallelSliceMut;
use reth_db::{
    cursor::{DbCursorRO, DbDupCursorRW},
    models::{AccountBeforeTx, StorageBeforeTx},
    static_file::TransactionSenderMask,
    table::Value,
    transaction::{CursorMutTy, CursorTy, DbTx, DbTxMut, DupCursorMutTy, DupCursorTy},
};
use reth_db_api::{
    cursor::DbCursorRW,
    models::{storage_sharded_key::StorageShardedKey, BlockNumberAddress, ShardedKey},
    tables,
    tables::BlockNumberList,
};
use reth_errors::ProviderError;
use reth_node_types::NodePrimitives;
use reth_primitives_traits::{ReceiptTy, StorageEntry};
use reth_static_file_types::StaticFileSegment;
use reth_storage_api::{ChangeSetReader, DBProvider, NodePrimitivesProvider, StorageSettingsCache};
use reth_storage_errors::provider::ProviderResult;
use strum::{Display, EnumIs};

/// Type alias for [`EitherReader`] constructors.
type EitherReaderTy<'a, P, T> =
    EitherReader<'a, CursorTy<<P as DBProvider>::Tx, T>, <P as NodePrimitivesProvider>::Primitives>;

/// Type alias for [`EitherReader`] constructors.
type DupEitherReaderTy<'a, P, T> = EitherReader<
    'a,
    DupCursorTy<<P as DBProvider>::Tx, T>,
    <P as NodePrimitivesProvider>::Primitives,
>;

/// Type alias for dup [`EitherWriter`] constructors.
type DupEitherWriterTy<'a, P, T> = EitherWriter<
    'a,
    DupCursorMutTy<<P as DBProvider>::Tx, T>,
    <P as NodePrimitivesProvider>::Primitives,
>;

/// Type alias for [`EitherWriter`] constructors.
type EitherWriterTy<'a, P, T> = EitherWriter<
    'a,
    CursorMutTy<<P as DBProvider>::Tx, T>,
    <P as NodePrimitivesProvider>::Primitives,
>;

/// Helper type for `RocksDB` batch argument in writer constructors.
///
/// When `rocksdb` feature is enabled, this is a real `RocksDB` batch.
/// Otherwise, it's `()` (unit type) to allow the same API without feature gates.
pub type RocksBatchArg<'a> = ();

/// The raw `RocksDB` batch type returned by [`EitherWriter::into_raw_rocksdb_batch`].
pub type RawRocksDBBatch = ();

/// Helper type for `RocksDB` transaction reference argument in reader constructors.
///
/// When `rocksdb` feature is disabled, it's `()` (unit type) to allow the same API without
/// feature gates.
pub type RocksTxRefArg<'a> = ();

/// Represents a destination for writing data, either to database, static files, or `RocksDB`.
#[derive(Debug, Display)]
pub enum EitherWriter<'a, CURSOR, N> {
    /// Write to database table via cursor
    Database(CURSOR),
    /// Write to static file
    StaticFile(StaticFileProviderRWRefMut<'a, N>),
}

impl<'a> EitherWriter<'a, (), ()> {
    /// Creates a new [`EitherWriter`] for receipts based on storage settings and prune modes.
    pub fn new_receipts<P>(
        provider: &'a P,
        block_number: BlockNumber,
    ) -> ProviderResult<EitherWriterTy<'a, P, tables::Receipts<ReceiptTy<P::Primitives>>>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache + StaticFileProviderFactory,
        P::Tx: DbTxMut,
        ReceiptTy<P::Primitives>: Value,
    {
        if Self::receipts_destination(provider).is_static_file() {
            Ok(EitherWriter::StaticFile(
                provider.get_static_file_writer(block_number, StaticFileSegment::Receipts)?,
            ))
        } else {
            Ok(EitherWriter::Database(
                provider.tx_ref().cursor_write::<tables::Receipts<ReceiptTy<P::Primitives>>>()?,
            ))
        }
    }

    /// Creates a new [`EitherWriter`] for senders based on storage settings.
    pub fn new_senders<P>(
        provider: &'a P,
        block_number: BlockNumber,
    ) -> ProviderResult<EitherWriterTy<'a, P, tables::TransactionSenders>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache + StaticFileProviderFactory,
        P::Tx: DbTxMut,
    {
        if EitherWriterDestination::senders(provider).is_static_file() {
            Ok(EitherWriter::StaticFile(
                provider
                    .get_static_file_writer(block_number, StaticFileSegment::TransactionSenders)?,
            ))
        } else {
            Ok(EitherWriter::Database(
                provider.tx_ref().cursor_write::<tables::TransactionSenders>()?,
            ))
        }
    }

    /// Creates a new [`EitherWriter`] for account changesets based on storage settings and prune
    /// modes.
    pub fn new_account_changesets<P>(
        provider: &'a P,
        block_number: BlockNumber,
    ) -> ProviderResult<DupEitherWriterTy<'a, P, tables::AccountChangeSets>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache + StaticFileProviderFactory,
        P::Tx: DbTxMut,
    {
        if provider.cached_storage_settings().storage_v2 {
            Ok(EitherWriter::StaticFile(
                provider
                    .get_static_file_writer(block_number, StaticFileSegment::AccountChangeSets)?,
            ))
        } else {
            Ok(EitherWriter::Database(
                provider.tx_ref().cursor_dup_write::<tables::AccountChangeSets>()?,
            ))
        }
    }

    /// Creates a new [`EitherWriter`] for storage changesets based on storage settings.
    pub fn new_storage_changesets<P>(
        provider: &'a P,
        block_number: BlockNumber,
    ) -> ProviderResult<DupEitherWriterTy<'a, P, tables::StorageChangeSets>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache + StaticFileProviderFactory,
        P::Tx: DbTxMut,
    {
        if provider.cached_storage_settings().storage_v2 {
            Ok(EitherWriter::StaticFile(
                provider
                    .get_static_file_writer(block_number, StaticFileSegment::StorageChangeSets)?,
            ))
        } else {
            Ok(EitherWriter::Database(
                provider.tx_ref().cursor_dup_write::<tables::StorageChangeSets>()?,
            ))
        }
    }

    /// Returns the destination for writing receipts.
    ///
    /// The rules are as follows:
    /// - If the node should not always write receipts to static files, and any receipt pruning is
    ///   enabled, write to the database.
    /// - If the node should always write receipts to static files, but receipt log filter pruning
    ///   is enabled, write to the database.
    /// - Otherwise, write to static files.
    pub fn receipts_destination<P: DBProvider + StorageSettingsCache>(
        provider: &P,
    ) -> EitherWriterDestination {
        let receipts_in_static_files = provider.cached_storage_settings().storage_v2;
        let prune_modes = provider.prune_modes_ref();

        if !receipts_in_static_files && prune_modes.has_receipts_pruning() ||
            // TODO: support writing receipts to static files with log filter pruning enabled
            receipts_in_static_files && !prune_modes.receipts_log_filter.is_empty()
        {
            EitherWriterDestination::Database
        } else {
            EitherWriterDestination::StaticFile
        }
    }

    /// Returns the destination for writing account changesets.
    ///
    /// This determines the destination based solely on storage settings.
    pub fn account_changesets_destination<P: DBProvider + StorageSettingsCache>(
        provider: &P,
    ) -> EitherWriterDestination {
        if provider.cached_storage_settings().storage_v2 {
            EitherWriterDestination::StaticFile
        } else {
            EitherWriterDestination::Database
        }
    }

    /// Returns the destination for writing storage changesets.
    ///
    /// This determines the destination based solely on storage settings.
    pub fn storage_changesets_destination<P: DBProvider + StorageSettingsCache>(
        provider: &P,
    ) -> EitherWriterDestination {
        if provider.cached_storage_settings().storage_v2 {
            EitherWriterDestination::StaticFile
        } else {
            EitherWriterDestination::Database
        }
    }

    /// Creates a new [`EitherWriter`] for storages history based on storage settings.
    pub fn new_storages_history<P>(
        provider: &P,
        _rocksdb_batch: RocksBatchArg<'a>,
    ) -> ProviderResult<EitherWriterTy<'a, P, tables::StoragesHistory>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache,
        P::Tx: DbTxMut,
    {
        Ok(EitherWriter::Database(provider.tx_ref().cursor_write::<tables::StoragesHistory>()?))
    }

    /// Creates a new [`EitherWriter`] for transaction hash numbers based on storage settings.
    pub fn new_transaction_hash_numbers<P>(
        provider: &P,
        _rocksdb_batch: RocksBatchArg<'a>,
    ) -> ProviderResult<EitherWriterTy<'a, P, tables::TransactionHashNumbers>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache,
        P::Tx: DbTxMut,
    {
        Ok(EitherWriter::Database(
            provider.tx_ref().cursor_write::<tables::TransactionHashNumbers>()?,
        ))
    }

    /// Creates a new [`EitherWriter`] for account history based on storage settings.
    pub fn new_accounts_history<P>(
        provider: &P,
        _rocksdb_batch: RocksBatchArg<'a>,
    ) -> ProviderResult<EitherWriterTy<'a, P, tables::AccountsHistory>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache,
        P::Tx: DbTxMut,
    {
        Ok(EitherWriter::Database(provider.tx_ref().cursor_write::<tables::AccountsHistory>()?))
    }
}

impl<'a, CURSOR, N: NodePrimitives> EitherWriter<'a, CURSOR, N> {
    /// Extracts the raw `RocksDB` write batch from this writer, if it contains one.
    ///
    /// Without the `rocksdb` feature, this always returns `None`.
    pub fn into_raw_rocksdb_batch(self) -> Option<RawRocksDBBatch> {
        match self {
            Self::Database(_) | Self::StaticFile(_) => None,
        }
    }

    /// Increment the block number.
    ///
    /// Relevant only for [`Self::StaticFile`]. It is a no-op for [`Self::Database`].
    pub fn increment_block(&mut self, expected_block_number: BlockNumber) -> ProviderResult<()> {
        match self {
            Self::Database(_) => Ok(()),
            Self::StaticFile(writer) => writer.increment_block(expected_block_number),
        }
    }

    /// Ensures that the writer is positioned at the specified block number.
    ///
    /// If the writer is positioned at a greater block number than the specified one, the writer
    /// will NOT be unwound and the error will be returned.
    ///
    /// Relevant only for [`Self::StaticFile`]. It is a no-op for [`Self::Database`].
    pub fn ensure_at_block(&mut self, block_number: BlockNumber) -> ProviderResult<()> {
        match self {
            Self::Database(_) => Ok(()),
            Self::StaticFile(writer) => writer.ensure_at_block(block_number),
        }
    }
}

impl<'a, CURSOR, N: NodePrimitives> EitherWriter<'a, CURSOR, N>
where
    N::Receipt: Value,
    CURSOR: DbCursorRW<tables::Receipts<N::Receipt>>,
{
    /// Append a transaction receipt.
    pub fn append_receipt(&mut self, tx_num: TxNumber, receipt: &N::Receipt) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => Ok(cursor.append(tx_num, receipt)?),
            Self::StaticFile(writer) => writer.append_receipt(tx_num, receipt),
        }
    }
}

impl<'a, CURSOR, N: NodePrimitives> EitherWriter<'a, CURSOR, N>
where
    CURSOR: DbCursorRW<tables::TransactionSenders>,
{
    /// Append a transaction sender to the destination
    pub fn append_sender(&mut self, tx_num: TxNumber, sender: &Address) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => Ok(cursor.append(tx_num, sender)?),
            Self::StaticFile(writer) => writer.append_transaction_sender(tx_num, sender),
        }
    }

    /// Append transaction senders to the destination
    pub fn append_senders<I>(&mut self, senders: I) -> ProviderResult<()>
    where
        I: Iterator<Item = (TxNumber, Address)>,
    {
        match self {
            Self::Database(cursor) => {
                for (tx_num, sender) in senders {
                    cursor.append(tx_num, &sender)?;
                }
                Ok(())
            }
            Self::StaticFile(writer) => writer.append_transaction_senders(senders),
        }
    }

    /// Removes all transaction senders above the given transaction number, and stops at the given
    /// block number.
    pub fn prune_senders(
        &mut self,
        unwind_tx_from: TxNumber,
        block: BlockNumber,
    ) -> ProviderResult<()>
    where
        CURSOR: DbCursorRO<tables::TransactionSenders>,
    {
        match self {
            Self::Database(cursor) => {
                let mut walker = cursor.walk_range(unwind_tx_from..)?;
                while walker.next().transpose()?.is_some() {
                    walker.delete_current()?;
                }
            }
            Self::StaticFile(writer) => {
                let static_file_transaction_sender_num = writer
                    .reader()
                    .get_highest_static_file_tx(StaticFileSegment::TransactionSenders);

                let to_delete = static_file_transaction_sender_num
                    .map(|static_num| (static_num + 1).saturating_sub(unwind_tx_from))
                    .unwrap_or_default();

                writer.prune_transaction_senders(to_delete, block)?;
            }
        }

        Ok(())
    }
}

impl<'a, CURSOR, N: NodePrimitives> EitherWriter<'a, CURSOR, N>
where
    CURSOR: DbCursorRW<tables::TransactionHashNumbers> + DbCursorRO<tables::TransactionHashNumbers>,
{
    /// Puts a transaction hash number mapping.
    ///
    /// When `append_only` is true, uses `cursor.append()` which is significantly faster
    /// but requires entries to be inserted in order and the table to be empty.
    /// When false, uses `cursor.upsert()` which handles arbitrary insertion order and duplicates.
    pub fn put_transaction_hash_number(
        &mut self,
        hash: TxHash,
        tx_num: TxNumber,
        append_only: bool,
    ) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => {
                if append_only {
                    Ok(cursor.append(hash, &tx_num)?)
                } else {
                    Ok(cursor.upsert(hash, &tx_num)?)
                }
            }
            Self::StaticFile(_) => Err(ProviderError::UnsupportedProvider),
        }
    }

    /// Puts multiple transaction hash number mappings in a batch.
    ///
    /// Accepts a vector of `(TxHash, TxNumber)` tuples and writes them all using the same cursor.
    /// This is more efficient than calling `put_transaction_hash_number` repeatedly.
    ///
    /// When `append_only` is true, uses `cursor.append()` which requires entries to be
    /// pre-sorted and the table to be empty or have only lower keys.
    /// When false, uses `cursor.upsert()` which handles arbitrary insertion order.
    pub fn put_transaction_hash_numbers_batch(
        &mut self,
        entries: Vec<(TxHash, TxNumber)>,
        append_only: bool,
    ) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => {
                for (hash, tx_num) in entries {
                    if append_only {
                        cursor.append(hash, &tx_num)?;
                    } else {
                        cursor.upsert(hash, &tx_num)?;
                    }
                }
                Ok(())
            }
            Self::StaticFile(_) => Err(ProviderError::UnsupportedProvider),
        }
    }

    /// Deletes a transaction hash number mapping.
    pub fn delete_transaction_hash_number(&mut self, hash: TxHash) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => {
                if cursor.seek_exact(hash)?.is_some() {
                    cursor.delete_current()?;
                }
                Ok(())
            }
            Self::StaticFile(_) => Err(ProviderError::UnsupportedProvider),
        }
    }
}

impl<'a, CURSOR, N: NodePrimitives> EitherWriter<'a, CURSOR, N>
where
    CURSOR: DbCursorRW<tables::StoragesHistory> + DbCursorRO<tables::StoragesHistory>,
{
    /// Puts a storage history entry.
    pub fn put_storage_history(
        &mut self,
        key: StorageShardedKey,
        value: &BlockNumberList,
    ) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => Ok(cursor.upsert(key, value)?),
            Self::StaticFile(_) => Err(ProviderError::UnsupportedProvider),
        }
    }

    /// Deletes a storage history entry.
    pub fn delete_storage_history(&mut self, key: StorageShardedKey) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => {
                if cursor.seek_exact(key)?.is_some() {
                    cursor.delete_current()?;
                }
                Ok(())
            }
            Self::StaticFile(_) => Err(ProviderError::UnsupportedProvider),
        }
    }

    /// Appends a storage history entry (for first sync - more efficient).
    pub fn append_storage_history(
        &mut self,
        key: StorageShardedKey,
        value: &BlockNumberList,
    ) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => Ok(cursor.append(key, value)?),
            Self::StaticFile(_) => Err(ProviderError::UnsupportedProvider),
        }
    }

    /// Upserts a storage history entry (for incremental sync).
    pub fn upsert_storage_history(
        &mut self,
        key: StorageShardedKey,
        value: &BlockNumberList,
    ) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => Ok(cursor.upsert(key, value)?),
            Self::StaticFile(_) => Err(ProviderError::UnsupportedProvider),
        }
    }

    /// Gets the last shard for an address and storage key (keyed with `u64::MAX`).
    pub fn get_last_storage_history_shard(
        &mut self,
        address: Address,
        storage_key: B256,
    ) -> ProviderResult<Option<BlockNumberList>> {
        let key = StorageShardedKey::last(address, storage_key);
        match self {
            Self::Database(cursor) => Ok(cursor.seek_exact(key)?.map(|(_, v)| v)),
            Self::StaticFile(_) => Err(ProviderError::UnsupportedProvider),
        }
    }
}

impl<'a, CURSOR, N: NodePrimitives> EitherWriter<'a, CURSOR, N>
where
    CURSOR: DbCursorRW<tables::AccountsHistory> + DbCursorRO<tables::AccountsHistory>,
{
    /// Appends an account history entry (for first sync - more efficient).
    pub fn append_account_history(
        &mut self,
        key: ShardedKey<Address>,
        value: &BlockNumberList,
    ) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => Ok(cursor.append(key, value)?),
            Self::StaticFile(_) => Err(ProviderError::UnsupportedProvider),
        }
    }

    /// Upserts an account history entry (for incremental sync).
    pub fn upsert_account_history(
        &mut self,
        key: ShardedKey<Address>,
        value: &BlockNumberList,
    ) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => Ok(cursor.upsert(key, value)?),
            Self::StaticFile(_) => Err(ProviderError::UnsupportedProvider),
        }
    }

    /// Gets the last shard for an address (keyed with `u64::MAX`).
    pub fn get_last_account_history_shard(
        &mut self,
        address: Address,
    ) -> ProviderResult<Option<BlockNumberList>> {
        match self {
            Self::Database(cursor) => {
                Ok(cursor.seek_exact(ShardedKey::last(address))?.map(|(_, v)| v))
            }
            Self::StaticFile(_) => Err(ProviderError::UnsupportedProvider),
        }
    }

    /// Deletes an account history entry.
    pub fn delete_account_history(&mut self, key: ShardedKey<Address>) -> ProviderResult<()> {
        match self {
            Self::Database(cursor) => {
                if cursor.seek_exact(key)?.is_some() {
                    cursor.delete_current()?;
                }
                Ok(())
            }
            Self::StaticFile(_) => Err(ProviderError::UnsupportedProvider),
        }
    }
}

impl<'a, CURSOR, N: NodePrimitives> EitherWriter<'a, CURSOR, N>
where
    CURSOR: DbDupCursorRW<tables::AccountChangeSets>,
{
    /// Append account changeset for a block.
    ///
    /// NOTE: This _sorts_ the changesets by address before appending
    pub fn append_account_changeset(
        &mut self,
        block_number: BlockNumber,
        mut changeset: Vec<AccountBeforeTx>,
    ) -> ProviderResult<()> {
        // First sort the changesets
        changeset.par_sort_by_key(|a| a.address);
        match self {
            Self::Database(cursor) => {
                for change in changeset {
                    cursor.append_dup(block_number, change)?;
                }
            }
            Self::StaticFile(writer) => {
                writer.append_account_changeset(changeset, block_number)?;
            }
        }

        Ok(())
    }
}

impl<'a, CURSOR, N: NodePrimitives> EitherWriter<'a, CURSOR, N>
where
    CURSOR: DbDupCursorRW<tables::StorageChangeSets>,
{
    /// Append storage changeset for a block.
    ///
    /// NOTE: This _sorts_ the changesets by address and storage key before appending.
    pub fn append_storage_changeset(
        &mut self,
        block_number: BlockNumber,
        mut changeset: Vec<StorageBeforeTx>,
    ) -> ProviderResult<()> {
        changeset.par_sort_by_key(|change| (change.address, change.key));

        match self {
            Self::Database(cursor) => {
                for change in changeset {
                    let storage_id = BlockNumberAddress((block_number, change.address));
                    cursor.append_dup(
                        storage_id,
                        StorageEntry { key: change.key, value: change.value },
                    )?;
                }
            }
            Self::StaticFile(writer) => {
                writer.append_storage_changeset(changeset, block_number)?;
            }
        }

        Ok(())
    }
}

/// Represents a source for reading data, either from database, static files, or `RocksDB`.
#[derive(Debug, Display)]
pub enum EitherReader<'a, CURSOR, N> {
    /// Read from database table via cursor
    Database(CURSOR, PhantomData<&'a ()>),
    /// Read from static file
    StaticFile(StaticFileProvider<N>, PhantomData<&'a ()>),
}

impl<'a> EitherReader<'a, (), ()> {
    /// Creates a new [`EitherReader`] for senders based on storage settings.
    pub fn new_senders<P>(
        provider: &P,
    ) -> ProviderResult<EitherReaderTy<'a, P, tables::TransactionSenders>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache + StaticFileProviderFactory,
        P::Tx: DbTx,
    {
        if EitherWriterDestination::senders(provider).is_static_file() {
            Ok(EitherReader::StaticFile(provider.static_file_provider(), PhantomData))
        } else {
            Ok(EitherReader::Database(
                provider.tx_ref().cursor_read::<tables::TransactionSenders>()?,
                PhantomData,
            ))
        }
    }

    /// Creates a new [`EitherReader`] for storages history based on storage settings.
    pub fn new_storages_history<P>(
        provider: &P,
        _rocksdb_tx: RocksTxRefArg<'a>,
    ) -> ProviderResult<EitherReaderTy<'a, P, tables::StoragesHistory>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache,
        P::Tx: DbTx,
    {
        Ok(EitherReader::Database(
            provider.tx_ref().cursor_read::<tables::StoragesHistory>()?,
            PhantomData,
        ))
    }

    /// Creates a new [`EitherReader`] for transaction hash numbers based on storage settings.
    pub fn new_transaction_hash_numbers<P>(
        provider: &P,
        _rocksdb_tx: RocksTxRefArg<'a>,
    ) -> ProviderResult<EitherReaderTy<'a, P, tables::TransactionHashNumbers>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache,
        P::Tx: DbTx,
    {
        Ok(EitherReader::Database(
            provider.tx_ref().cursor_read::<tables::TransactionHashNumbers>()?,
            PhantomData,
        ))
    }

    /// Creates a new [`EitherReader`] for account history based on storage settings.
    pub fn new_accounts_history<P>(
        provider: &P,
        _rocksdb_tx: RocksTxRefArg<'a>,
    ) -> ProviderResult<EitherReaderTy<'a, P, tables::AccountsHistory>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache,
        P::Tx: DbTx,
    {
        Ok(EitherReader::Database(
            provider.tx_ref().cursor_read::<tables::AccountsHistory>()?,
            PhantomData,
        ))
    }

    /// Creates a new [`EitherReader`] for account changesets based on storage settings.
    pub fn new_account_changesets<P>(
        provider: &P,
    ) -> ProviderResult<DupEitherReaderTy<'a, P, tables::AccountChangeSets>>
    where
        P: DBProvider + NodePrimitivesProvider + StorageSettingsCache + StaticFileProviderFactory,
        P::Tx: DbTx,
    {
        if EitherWriterDestination::account_changesets(provider).is_static_file() {
            Ok(EitherReader::StaticFile(provider.static_file_provider(), PhantomData))
        } else {
            Ok(EitherReader::Database(
                provider.tx_ref().cursor_dup_read::<tables::AccountChangeSets>()?,
                PhantomData,
            ))
        }
    }
}

impl<CURSOR, N: NodePrimitives> EitherReader<'_, CURSOR, N>
where
    CURSOR: DbCursorRO<tables::TransactionSenders>,
{
    /// Fetches the senders for a range of transactions.
    pub fn senders_by_tx_range(
        &mut self,
        range: Range<TxNumber>,
    ) -> ProviderResult<HashMap<TxNumber, Address>> {
        match self {
            Self::Database(cursor, _) => cursor
                .walk_range(range)?
                .map(|result| result.map_err(ProviderError::from))
                .collect::<ProviderResult<HashMap<_, _>>>(),
            Self::StaticFile(provider, _) => range
                .clone()
                .zip(provider.fetch_range_iter(
                    StaticFileSegment::TransactionSenders,
                    range,
                    |cursor, number| cursor.get_one::<TransactionSenderMask>(number.into()),
                )?)
                .filter_map(|(tx_num, sender)| {
                    let result = sender.transpose()?;
                    Some(result.map(|sender| (tx_num, sender)))
                })
                .collect::<ProviderResult<HashMap<_, _>>>(),
        }
    }
}

impl<CURSOR, N: NodePrimitives> EitherReader<'_, CURSOR, N>
where
    CURSOR: DbCursorRO<tables::TransactionHashNumbers>,
{
    /// Gets a transaction number by its hash.
    pub fn get_transaction_hash_number(
        &mut self,
        hash: TxHash,
    ) -> ProviderResult<Option<TxNumber>> {
        match self {
            Self::Database(cursor, _) => Ok(cursor.seek_exact(hash)?.map(|(_, v)| v)),
            Self::StaticFile(_, _) => Err(ProviderError::UnsupportedProvider),
        }
    }
}

impl<CURSOR, N: NodePrimitives> EitherReader<'_, CURSOR, N>
where
    CURSOR: DbCursorRO<tables::StoragesHistory>,
{
    /// Gets a storage history shard entry for the given [`StorageShardedKey`], if present.
    pub fn get_storage_history(
        &mut self,
        key: StorageShardedKey,
    ) -> ProviderResult<Option<BlockNumberList>> {
        match self {
            Self::Database(cursor, _) => Ok(cursor.seek_exact(key)?.map(|(_, v)| v)),
            Self::StaticFile(_, _) => Err(ProviderError::UnsupportedProvider),
        }
    }

    /// Lookup storage history and return [`HistoryInfo`].
    pub fn storage_history_info(
        &mut self,
        address: Address,
        storage_key: alloy_primitives::B256,
        block_number: BlockNumber,
        lowest_available_block_number: Option<BlockNumber>,
    ) -> ProviderResult<HistoryInfo> {
        match self {
            Self::Database(cursor, _) => {
                let key = StorageShardedKey::new(address, storage_key, block_number);
                history_info::<tables::StoragesHistory, _, _>(
                    cursor,
                    key,
                    block_number,
                    |k| k.address == address && k.sharded_key.key == storage_key,
                    lowest_available_block_number,
                )
            }
            Self::StaticFile(_, _) => Err(ProviderError::UnsupportedProvider),
        }
    }
}

impl<CURSOR, N: NodePrimitives> EitherReader<'_, CURSOR, N>
where
    CURSOR: DbCursorRO<tables::AccountsHistory>,
{
    /// Gets an account history shard entry for the given [`ShardedKey`], if present.
    pub fn get_account_history(
        &mut self,
        key: ShardedKey<Address>,
    ) -> ProviderResult<Option<BlockNumberList>> {
        match self {
            Self::Database(cursor, _) => Ok(cursor.seek_exact(key)?.map(|(_, v)| v)),
            Self::StaticFile(_, _) => Err(ProviderError::UnsupportedProvider),
        }
    }

    /// Lookup account history and return [`HistoryInfo`].
    pub fn account_history_info(
        &mut self,
        address: Address,
        block_number: BlockNumber,
        lowest_available_block_number: Option<BlockNumber>,
    ) -> ProviderResult<HistoryInfo> {
        match self {
            Self::Database(cursor, _) => {
                let key = ShardedKey::new(address, block_number);
                history_info::<tables::AccountsHistory, _, _>(
                    cursor,
                    key,
                    block_number,
                    |k| k.key == address,
                    lowest_available_block_number,
                )
            }
            Self::StaticFile(_, _) => Err(ProviderError::UnsupportedProvider),
        }
    }
}

impl<CURSOR, N: NodePrimitives> EitherReader<'_, CURSOR, N>
where
    CURSOR: DbCursorRO<tables::AccountChangeSets>,
{
    /// Iterate over account changesets and return all account address that were changed.
    pub fn changed_accounts_with_range(
        &mut self,
        range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<BTreeSet<Address>> {
        match self {
            Self::StaticFile(provider, _) => {
                let highest_static_block =
                    provider.get_highest_static_file_block(StaticFileSegment::AccountChangeSets);

                let Some(highest) = highest_static_block else {
                    return Err(ProviderError::MissingHighestStaticFileBlock(
                        StaticFileSegment::AccountChangeSets,
                    ))
                };

                let start = *range.start();
                let static_end = (*range.end()).min(highest);

                let mut changed_accounts = BTreeSet::default();
                if start <= static_end {
                    for block in start..=static_end {
                        let block_changesets = provider.account_block_changeset(block)?;
                        for changeset in block_changesets {
                            changed_accounts.insert(changeset.address);
                        }
                    }
                }

                Ok(changed_accounts)
            }
            Self::Database(provider, _) => provider
                .walk_range(range)?
                .map(|entry| {
                    entry.map(|(_, account_before)| account_before.address).map_err(Into::into)
                })
                .collect(),
        }
    }
}

/// Destination for writing data.
#[derive(Debug, EnumIs)]
pub enum EitherWriterDestination {
    /// Write to database table
    Database,
    /// Write to static file
    StaticFile,
    /// Write to `RocksDB`
    RocksDB,
}

impl EitherWriterDestination {
    /// Returns the destination for writing senders based on storage settings.
    pub fn senders<P>(provider: &P) -> Self
    where
        P: StorageSettingsCache,
    {
        // Write senders to static files only if they're explicitly enabled
        if provider.cached_storage_settings().storage_v2 {
            Self::StaticFile
        } else {
            Self::Database
        }
    }

    /// Returns the destination for writing account changesets based on storage settings.
    pub fn account_changesets<P>(provider: &P) -> Self
    where
        P: StorageSettingsCache,
    {
        // Write account changesets to static files only if they're explicitly enabled
        if provider.cached_storage_settings().storage_v2 {
            Self::StaticFile
        } else {
            Self::Database
        }
    }

    /// Returns the destination for writing storage changesets based on storage settings.
    pub fn storage_changesets<P>(provider: &P) -> Self
    where
        P: StorageSettingsCache,
    {
        // Write storage changesets to static files only if they're explicitly enabled
        if provider.cached_storage_settings().storage_v2 {
            Self::StaticFile
        } else {
            Self::Database
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{test_utils::create_test_provider_factory, StaticFileWriter};

    use super::*;
    use alloy_primitives::Address;
    use reth_db::models::AccountBeforeTx;
    use reth_static_file_types::StaticFileSegment;
    use reth_storage_api::{DatabaseProviderFactory, StorageSettings};

    /// Verifies that `changed_accounts_with_range` correctly caps the query range to the
    /// static file tip when the requested range extends beyond it.
    ///
    /// This test documents the fix for an off-by-one bug where the code computed
    /// `static_end = range.end().min(highest + 1)` instead of `min(highest)`.
    /// The bug allowed iteration to attempt reading block `highest + 1` which doesn't
    /// exist (silently returning empty results due to `MissingStaticFileBlock` handling).
    ///
    /// While the bug was masked by error handling, it caused:
    /// 1. Unnecessary iteration/lookup for non-existent blocks
    /// 2. Potential overflow when `highest == u64::MAX`
    #[test]
    fn test_changed_accounts_with_range_caps_at_static_file_tip() {
        let factory = create_test_provider_factory();
        let highest_block = 5u64;

        let addresses: Vec<Address> = (0..=highest_block)
            .map(|i| {
                let mut addr = Address::ZERO;
                addr.0[0] = i as u8;
                addr
            })
            .collect();

        {
            let sf_provider = factory.static_file_provider();
            let mut writer =
                sf_provider.latest_writer(StaticFileSegment::AccountChangeSets).unwrap();

            for block_num in 0..=highest_block {
                let changeset =
                    vec![AccountBeforeTx { address: addresses[block_num as usize], info: None }];
                writer.append_account_changeset(changeset, block_num).unwrap();
            }
            writer.commit().unwrap();
        }

        factory.set_storage_settings_cache(StorageSettings::v2());

        let provider = factory.database_provider_ro().unwrap();

        let sf_tip = provider
            .static_file_provider()
            .get_highest_static_file_block(StaticFileSegment::AccountChangeSets);
        assert_eq!(sf_tip, Some(highest_block));

        let mut reader = EitherReader::new_account_changesets(&provider).unwrap();
        assert!(matches!(reader, EitherReader::StaticFile(_, _)));

        // Query range 0..=10 when tip is 5 - should return only accounts from blocks 0-5
        let result = reader.changed_accounts_with_range(0..=10).unwrap();

        let expected: BTreeSet<Address> = addresses.into_iter().collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_reader_senders_by_tx_range() {
        let factory = create_test_provider_factory();

        // Insert senders only from 1 to 4, but we will query from 0 to 5.
        let senders = [
            (1, Address::random()),
            (2, Address::random()),
            (3, Address::random()),
            (4, Address::random()),
        ];

        for transaction_senders_in_static_files in [false, true] {
            factory.set_storage_settings_cache(if transaction_senders_in_static_files {
                StorageSettings::v2()
            } else {
                StorageSettings::v1()
            });

            let provider = factory.database_provider_rw().unwrap();
            let mut writer = EitherWriter::new_senders(&provider, 0).unwrap();
            if transaction_senders_in_static_files {
                assert!(matches!(writer, EitherWriter::StaticFile(_)));
            } else {
                assert!(matches!(writer, EitherWriter::Database(_)));
            }

            writer.increment_block(0).unwrap();
            writer.append_senders(senders.iter().copied()).unwrap();
            drop(writer);
            provider.commit().unwrap();

            let provider = factory.database_provider_ro().unwrap();
            let mut reader = EitherReader::new_senders(&provider).unwrap();
            if transaction_senders_in_static_files {
                assert!(matches!(reader, EitherReader::StaticFile(_, _)));
            } else {
                assert!(matches!(reader, EitherReader::Database(_, _)));
            }

            assert_eq!(
                reader.senders_by_tx_range(0..6).unwrap(),
                senders.iter().copied().collect::<HashMap<_, _>>(),
                "{reader}"
            );
        }
    }
}
