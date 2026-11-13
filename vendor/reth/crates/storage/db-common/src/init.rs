//! Reth genesis initialization utility functions.

use alloy_consensus::BlockHeader;
use alloy_genesis::GenesisAccount;
use alloy_primitives::{
    map::{AddressMap, B256Map, HashMap},
    Address, B256, U256,
};
use reth_chainspec::EthChainSpec;
use reth_codecs::Compact;
use reth_db_api::{
    models::{storage_sharded_key::StorageShardedKey, ShardedKey},
    tables,
    transaction::DbTxMut,
    BlockNumberList, DatabaseError,
};
use reth_execution_errors::StateRootError;
use reth_primitives_traits::{Account, Bytecode, GotExpected, NodePrimitives, StorageEntry};
use reth_provider::{
    errors::provider::ProviderResult, providers::StaticFileWriter, BlockHashReader, BlockNumReader,
    BundleStateInit, ChainSpecProvider, DBProvider, DatabaseProviderFactory, EitherWriter,
    ExecutionOutcome, HashingWriter, HeaderProvider, HistoryWriter, MetadataProvider,
    MetadataWriter, NodePrimitivesProvider, OriginalValuesKnown, ProviderError, RevertsInit,
    RocksDBProviderFactory, StageCheckpointReader, StageCheckpointWriter, StateWriteConfig,
    StateWriter, StaticFileProviderFactory, StorageSettings, StorageSettingsCache, TrieWriter,
};
use reth_stages_types::{StageCheckpoint, StageId};
use reth_static_file_types::StaticFileSegment;
use reth_trie::{
    prefix_set::TriePrefixSets, IntermediateStateRootState, StateRoot as StateRootComputer,
    StateRootProgress,
};
use reth_trie_db::DatabaseStateRoot;
use tracing::{debug, error, info, trace, warn};

/// Soft limit for the number of flushed updates after which to log progress summary.
const SOFT_LIMIT_COUNT_FLUSHED_UPDATES: usize = 1_000_000;

/// Storage initialization error type.
#[derive(Debug, thiserror::Error, Clone)]
pub enum InitStorageError {
    /// Genesis header found on static files but the database is empty.
    #[error(
        "static files found, but the database is uninitialized. If attempting to re-syncing, delete both."
    )]
    UninitializedDatabase,
    /// An existing genesis block was found in the database, and its hash did not match the hash of
    /// the chainspec.
    #[error(
        "genesis hash in the storage does not match the specified chainspec: chainspec is {chainspec_hash}, database is {storage_hash}"
    )]
    GenesisHashMismatch {
        /// Expected genesis hash.
        chainspec_hash: B256,
        /// Actual genesis hash.
        storage_hash: B256,
    },
    /// Provider error.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// State root error while computing the state root
    #[error(transparent)]
    StateRootError(#[from] StateRootError),
    /// State root doesn't match the expected one.
    #[error("state root mismatch: {_0}")]
    StateRootMismatch(GotExpected<B256>),
    /// TN(TN-0116): a storage v2 genesis allocates storage to an account without code.
    #[error(
        "genesis account {_0} has storage but no code; storage v2 refuses it, because it could \
         not record those slots if a later block wiped the account"
    )]
    GenesisStorageWithoutCode(Address),
}

impl From<DatabaseError> for InitStorageError {
    fn from(error: DatabaseError) -> Self {
        Self::Provider(ProviderError::Database(error))
    }
}

/// Write the genesis block if it has not already been written
pub fn init_genesis<PF>(factory: &PF) -> Result<B256, InitStorageError>
where
    PF: DatabaseProviderFactory
        + StaticFileProviderFactory<Primitives: NodePrimitives<BlockHeader: Compact>>
        + ChainSpecProvider
        + StageCheckpointReader
        + BlockNumReader
        + MetadataProvider
        + StorageSettingsCache,
    PF::ProviderRW: StaticFileProviderFactory<Primitives = PF::Primitives>
        + StageCheckpointWriter
        + HistoryWriter
        + HeaderProvider
        + HashingWriter
        + StateWriter
        + TrieWriter
        + MetadataWriter
        + ChainSpecProvider
        + StorageSettingsCache
        + RocksDBProviderFactory
        + NodePrimitivesProvider
        + AsRef<PF::ProviderRW>,
    PF::ChainSpec: EthChainSpec<Header = <PF::Primitives as NodePrimitives>::BlockHeader>,
{
    init_genesis_with_settings(factory, StorageSettings::base())
}

/// Write the genesis block if it has not already been written with [`StorageSettings`].
pub fn init_genesis_with_settings<PF>(
    factory: &PF,
    genesis_storage_settings: StorageSettings,
) -> Result<B256, InitStorageError>
where
    PF: DatabaseProviderFactory
        + StaticFileProviderFactory<Primitives: NodePrimitives<BlockHeader: Compact>>
        + ChainSpecProvider
        + StageCheckpointReader
        + BlockNumReader
        + MetadataProvider
        + StorageSettingsCache,
    PF::ProviderRW: StaticFileProviderFactory<Primitives = PF::Primitives>
        + StageCheckpointWriter
        + HistoryWriter
        + HeaderProvider
        + HashingWriter
        + StateWriter
        + TrieWriter
        + MetadataWriter
        + ChainSpecProvider
        + StorageSettingsCache
        + RocksDBProviderFactory
        + NodePrimitivesProvider
        + AsRef<PF::ProviderRW>,
    PF::ChainSpec: EthChainSpec<Header = <PF::Primitives as NodePrimitives>::BlockHeader>,
{
    let chain = factory.chain_spec();

    let genesis = chain.genesis();
    let hash = chain.genesis_hash();

    // Get the genesis block number from the chain spec
    let genesis_block_number = chain.genesis_header().number();

    // Check if we already have the genesis header or if we have the wrong one.
    match factory.block_hash(genesis_block_number) {
        Ok(None) | Err(ProviderError::MissingStaticFileBlock(StaticFileSegment::Headers, _)) => {}
        Ok(Some(block_hash)) => {
            if block_hash == hash {
                // Some users will at times attempt to re-sync from scratch by just deleting the
                // database. Since `factory.block_hash` will only query the static files, we need to
                // make sure that our database has been written to, and throw error if it's empty.
                if factory.get_stage_checkpoint(StageId::Headers)?.is_none() {
                    // TN(TN-0083): the commit below finalizes the static files before the MDBX
                    // transaction, so a first launch that crashed between the two leaves the
                    // genesis header in static files and nothing in the database, and every
                    // restart failed here until the operator deleted the datadir.
                    // With no stage checkpoint at all the database is empty, so the launch
                    // resumes: `insert_genesis_header` keeps the header already there. Storage v1
                    // only: under v2 genesis also committed block 0 of the changeset segments,
                    // which the resumed `insert_genesis_state` fails to append again. TN's
                    // `layout::settle` removes such a genesis, or refuses the datadir, first.
                    if database_is_empty(factory)? {
                        warn!(target: "reth::storage", "Genesis header found on static files with an empty database; resuming an interrupted first launch.");
                    } else {
                        error!(target: "reth::storage", "Genesis header found on static files, but database is uninitialized.");
                        return Err(InitStorageError::UninitializedDatabase)
                    }
                } else {
                    let stored = factory.storage_settings()?.unwrap_or_else(StorageSettings::v1);
                    if stored != genesis_storage_settings {
                        warn!(
                            target: "reth::storage",
                            ?stored,
                            requested = ?genesis_storage_settings,
                            "Storage settings mismatch detected"
                        );
                    }

                    debug!("Genesis already written, skipping.");
                    return Ok(hash)
                }
            } else {
                return Err(InitStorageError::GenesisHashMismatch {
                    chainspec_hash: hash,
                    storage_hash: block_hash,
                })
            }
        }
        Err(e) => {
            debug!(?e);
            return Err(e.into());
        }
    }

    debug!("Writing genesis block.");

    let alloc = &genesis.alloc;

    // TN(TN-0116): a genesis account with storage but no code (and a zero nonce) is the only
    // account a later block can wipe with its storage in place (EIP-161's empty-account deletion,
    // or a `SELFDESTRUCT` after a create over it), which v2 cannot record and refuses at that block
    // (`DatabaseProvider::ensure_wiped_accounts_had_no_storage`). Refuse it before anything is
    // written, at genesis, instead of halting the chain later.
    if genesis_storage_settings.storage_v2 &&
        let Some((address, _)) = alloc.iter().find(|(_, account)| {
            account.storage.as_ref().is_some_and(|storage| !storage.is_empty()) &&
                account.code.as_ref().is_none_or(|code| code.is_empty())
        })
    {
        return Err(InitStorageError::GenesisStorageWithoutCode(*address))
    }

    // Make sure to set storage settings before anything writes
    factory.set_storage_settings_cache(genesis_storage_settings);

    // use transaction to insert genesis header
    let provider_rw = factory.database_provider_rw()?;

    // Behaviour reserved only for new nodes should be set in the storage settings.
    provider_rw.write_storage_settings(genesis_storage_settings)?;

    // For non-zero genesis blocks, set expected_block_start BEFORE insert_genesis_state.
    // When block_range is None, next_block_number() uses expected_block_start. By default,
    // expected_block_start comes from find_fixed_range which returns the file range start (0),
    // not the genesis block number. This would cause increment_block(N) to fail.
    let static_file_provider = provider_rw.static_file_provider();
    if genesis_block_number > 0 {
        if genesis_storage_settings.storage_v2 {
            static_file_provider
                .get_writer(genesis_block_number, StaticFileSegment::AccountChangeSets)?
                .user_header_mut()
                .set_expected_block_start(genesis_block_number);
        }
        if genesis_storage_settings.storage_v2 {
            static_file_provider
                .get_writer(genesis_block_number, StaticFileSegment::StorageChangeSets)?
                .user_header_mut()
                .set_expected_block_start(genesis_block_number);
        }
    }

    insert_genesis_hashes(&provider_rw, alloc.iter())?;
    insert_genesis_history(&provider_rw, alloc.iter())?;

    // Insert header
    insert_genesis_header(&provider_rw, &chain)?;

    insert_genesis_state(&provider_rw, alloc.iter())?;

    // compute state root to populate trie tables
    compute_state_root(&provider_rw, None)?;

    // set stage checkpoint to genesis block number for all stages
    let checkpoint = StageCheckpoint::new(genesis_block_number);
    for stage in StageId::ALL {
        provider_rw.save_stage_checkpoint(stage, checkpoint)?;
    }

    // Static file segments start empty, so we need to initialize the block range.
    // For genesis blocks with non-zero block numbers, we use get_writer() instead of
    // latest_writer() and set_block_range() to ensure static files start at the correct block.
    let static_file_provider = provider_rw.static_file_provider();

    static_file_provider
        .get_writer(genesis_block_number, StaticFileSegment::Receipts)?
        .user_header_mut()
        .set_block_range(genesis_block_number, genesis_block_number);
    static_file_provider
        .get_writer(genesis_block_number, StaticFileSegment::Transactions)?
        .user_header_mut()
        .set_block_range(genesis_block_number, genesis_block_number);

    if genesis_storage_settings.storage_v2 {
        static_file_provider
            .get_writer(genesis_block_number, StaticFileSegment::TransactionSenders)?
            .user_header_mut()
            .set_block_range(genesis_block_number, genesis_block_number);
    }

    // `commit_unwind`` will first commit the DB and then the static file provider, which is
    // necessary on `init_genesis`.
    provider_rw.commit()?;

    Ok(hash)
}

/// TN(TN-0083): whether no stage has a checkpoint, i.e. no genesis write ever committed to the
/// database.
fn database_is_empty<PF: StageCheckpointReader>(factory: &PF) -> Result<bool, InitStorageError> {
    for stage in StageId::ALL {
        if factory.get_stage_checkpoint(stage)?.is_some() {
            return Ok(false)
        }
    }
    Ok(true)
}

/// Inserts the genesis state into the database.
pub fn insert_genesis_state<'a, 'b, Provider>(
    provider: &Provider,
    alloc: impl Iterator<Item = (&'a Address, &'b GenesisAccount)>,
) -> ProviderResult<()>
where
    Provider: StaticFileProviderFactory
        + DBProvider<Tx: DbTxMut>
        + HeaderProvider
        + StateWriter
        + ChainSpecProvider
        + AsRef<Provider>,
{
    let genesis_block_number = provider.chain_spec().genesis_header().number();
    insert_state(provider, alloc, genesis_block_number)
}

/// Inserts state at given block into database.
pub fn insert_state<'a, 'b, Provider>(
    provider: &Provider,
    alloc: impl Iterator<Item = (&'a Address, &'b GenesisAccount)>,
    block: u64,
) -> ProviderResult<()>
where
    Provider: StaticFileProviderFactory
        + DBProvider<Tx: DbTxMut>
        + HeaderProvider
        + StateWriter
        + AsRef<Provider>,
{
    let capacity = alloc.size_hint().1.unwrap_or(0);
    let mut state_init: BundleStateInit =
        AddressMap::with_capacity_and_hasher(capacity, Default::default());
    let mut reverts_init: AddressMap<_> =
        AddressMap::with_capacity_and_hasher(capacity, Default::default());
    let mut contracts: B256Map<Bytecode> =
        B256Map::with_capacity_and_hasher(capacity, Default::default());

    for (address, account) in alloc {
        let bytecode_hash = if let Some(code) = &account.code {
            match Bytecode::new_raw_checked(code.clone()) {
                Ok(bytecode) => {
                    let hash = bytecode.hash_slow();
                    contracts.insert(hash, bytecode);
                    Some(hash)
                }
                Err(err) => {
                    error!(%address, %err, "Failed to decode genesis bytecode.");
                    return Err(DatabaseError::Other(err.to_string()).into());
                }
            }
        } else {
            None
        };

        // get state
        let storage = account
            .storage
            .as_ref()
            .map(|m| {
                m.iter()
                    .map(|(key, value)| {
                        let value = U256::from_be_bytes(value.0);
                        (*key, (U256::ZERO, value))
                    })
                    .collect::<B256Map<_>>()
            })
            .unwrap_or_default();

        reverts_init.insert(
            *address,
            (Some(None), storage.keys().map(|k| StorageEntry::new(*k, U256::ZERO)).collect()),
        );

        state_init.insert(
            *address,
            (
                None,
                Some(Account {
                    nonce: account.nonce.unwrap_or_default(),
                    balance: account.balance,
                    bytecode_hash,
                }),
                storage,
            ),
        );
    }
    let all_reverts_init: RevertsInit = HashMap::from_iter([(block, reverts_init)]);

    let execution_outcome = ExecutionOutcome::new_init(
        state_init,
        all_reverts_init,
        contracts,
        Vec::default(),
        block,
        Vec::new(),
    );

    provider.write_state(
        &execution_outcome,
        OriginalValuesKnown::Yes,
        StateWriteConfig::default(),
    )?;

    trace!(target: "reth::cli", "Inserted state");

    Ok(())
}

/// Inserts hashes for the genesis state.
pub fn insert_genesis_hashes<'a, 'b, Provider>(
    provider: &Provider,
    alloc: impl Iterator<Item = (&'a Address, &'b GenesisAccount)> + Clone,
) -> ProviderResult<()>
where
    Provider: DBProvider<Tx: DbTxMut> + HashingWriter,
{
    // insert and hash accounts to hashing table
    let alloc_accounts = alloc.clone().map(|(addr, account)| (*addr, Some(Account::from(account))));
    provider.insert_account_for_hashing(alloc_accounts)?;

    trace!(target: "reth::cli", "Inserted account hashes");

    let alloc_storage = alloc.filter_map(|(addr, account)| {
        // only return Some if there is storage
        account.storage.as_ref().map(|storage| {
            (*addr, storage.iter().map(|(&key, &value)| StorageEntry { key, value: value.into() }))
        })
    });
    provider.insert_storage_for_hashing(alloc_storage)?;

    trace!(target: "reth::cli", "Inserted storage hashes");

    Ok(())
}

/// Inserts history indices for genesis accounts and storage.
///
/// Writes to either MDBX or `RocksDB` based on storage settings configuration,
/// using [`EitherWriter`] to abstract over the storage backend.
pub fn insert_genesis_history<'a, 'b, Provider>(
    provider: &Provider,
    alloc: impl Iterator<Item = (&'a Address, &'b GenesisAccount)> + Clone,
) -> ProviderResult<()>
where
    Provider: DBProvider<Tx: DbTxMut>
        + HistoryWriter
        + ChainSpecProvider
        + StorageSettingsCache
        + RocksDBProviderFactory
        + NodePrimitivesProvider,
{
    let genesis_block_number = provider.chain_spec().genesis_header().number();
    insert_history(provider, alloc, genesis_block_number)
}

/// Inserts account history indices for genesis accounts.
pub fn insert_genesis_account_history<'a, 'b, Provider>(
    provider: &Provider,
    alloc: impl Iterator<Item = (&'a Address, &'b GenesisAccount)>,
) -> ProviderResult<()>
where
    Provider: DBProvider<Tx: DbTxMut>
        + HistoryWriter
        + ChainSpecProvider
        + StorageSettingsCache
        + RocksDBProviderFactory
        + NodePrimitivesProvider,
{
    let genesis_block_number = provider.chain_spec().genesis_header().number();
    insert_account_history(provider, alloc, genesis_block_number)
}

/// Inserts storage history indices for genesis accounts.
pub fn insert_genesis_storage_history<'a, 'b, Provider>(
    provider: &Provider,
    alloc: impl Iterator<Item = (&'a Address, &'b GenesisAccount)>,
) -> ProviderResult<()>
where
    Provider: DBProvider<Tx: DbTxMut>
        + HistoryWriter
        + ChainSpecProvider
        + StorageSettingsCache
        + RocksDBProviderFactory
        + NodePrimitivesProvider,
{
    let genesis_block_number = provider.chain_spec().genesis_header().number();
    insert_storage_history(provider, alloc, genesis_block_number)
}

/// Inserts history indices for genesis accounts and storage.
///
/// Writes to either MDBX or `RocksDB` based on storage settings configuration,
/// using [`EitherWriter`] to abstract over the storage backend.
pub fn insert_history<'a, 'b, Provider>(
    provider: &Provider,
    alloc: impl Iterator<Item = (&'a Address, &'b GenesisAccount)> + Clone,
    block: u64,
) -> ProviderResult<()>
where
    Provider: DBProvider<Tx: DbTxMut>
        + HistoryWriter
        + StorageSettingsCache
        + RocksDBProviderFactory
        + NodePrimitivesProvider,
{
    insert_account_history(provider, alloc.clone(), block)?;
    insert_storage_history(provider, alloc, block)?;
    Ok(())
}

/// Inserts account history indices at the given block.
pub fn insert_account_history<'a, 'b, Provider>(
    provider: &Provider,
    alloc: impl Iterator<Item = (&'a Address, &'b GenesisAccount)>,
    block: u64,
) -> ProviderResult<()>
where
    Provider: DBProvider<Tx: DbTxMut>
        + HistoryWriter
        + StorageSettingsCache
        + RocksDBProviderFactory
        + NodePrimitivesProvider,
{
    provider.with_rocksdb_batch(|batch| {
        let mut writer = EitherWriter::new_accounts_history(provider, batch)?;
        let list = BlockNumberList::new([block]).expect("single block always fits");
        for (addr, _) in alloc {
            writer.upsert_account_history(ShardedKey::last(*addr), &list)?;
        }
        trace!(target: "reth::cli", "Inserted account history");
        Ok(((), writer.into_raw_rocksdb_batch()))
    })?;

    Ok(())
}

/// Inserts storage history indices at the given block.
pub fn insert_storage_history<'a, 'b, Provider>(
    provider: &Provider,
    alloc: impl Iterator<Item = (&'a Address, &'b GenesisAccount)>,
    block: u64,
) -> ProviderResult<()>
where
    Provider: DBProvider<Tx: DbTxMut>
        + HistoryWriter
        + StorageSettingsCache
        + RocksDBProviderFactory
        + NodePrimitivesProvider,
{
    provider.with_rocksdb_batch(|batch| {
        let mut writer = EitherWriter::new_storages_history(provider, batch)?;
        let list = BlockNumberList::new([block]).expect("single block always fits");
        for (addr, account) in alloc {
            if let Some(storage) = &account.storage {
                for key in storage.keys() {
                    writer.upsert_storage_history(StorageShardedKey::last(*addr, *key), &list)?;
                }
            }
        }
        trace!(target: "reth::cli", "Inserted storage history");
        Ok(((), writer.into_raw_rocksdb_batch()))
    })?;

    Ok(())
}

/// Inserts header for the genesis state.
pub fn insert_genesis_header<Provider, Spec>(
    provider: &Provider,
    chain: &Spec,
) -> ProviderResult<()>
where
    Provider: StaticFileProviderFactory<Primitives: NodePrimitives<BlockHeader: Compact>>
        + DBProvider<Tx: DbTxMut>,
    Spec: EthChainSpec<Header = <Provider::Primitives as NodePrimitives>::BlockHeader>,
{
    let (header, block_hash) = (chain.genesis_header(), chain.genesis_hash());
    let static_file_provider = provider.static_file_provider();

    // Get the actual genesis block number from the header
    let genesis_block_number = header.number();

    match static_file_provider.block_hash(genesis_block_number) {
        Ok(None) | Err(ProviderError::MissingStaticFileBlock(StaticFileSegment::Headers, _)) => {
            let difficulty = header.difficulty();

            // For genesis blocks with non-zero block numbers, we need to ensure they are stored
            // in the correct static file range. We use get_writer() with the genesis block number
            // to ensure the genesis block is stored in the correct static file range.
            let mut writer = static_file_provider
                .get_writer(genesis_block_number, StaticFileSegment::Headers)?;

            // For non-zero genesis blocks, we need to set block range to genesis_block_number and
            // append header without increment block
            if genesis_block_number > 0 {
                writer
                    .user_header_mut()
                    .set_block_range(genesis_block_number, genesis_block_number);
                writer.append_header_direct(header, difficulty, &block_hash)?;
            } else {
                // For zero genesis blocks, use normal append_header
                writer.append_header(header, &block_hash)?;
            }
        }
        Ok(Some(_)) => {}
        Err(e) => return Err(e),
    }

    provider.tx_ref().put::<tables::HeaderNumbers>(block_hash, genesis_block_number)?;
    provider.tx_ref().put::<tables::BlockBodyIndices>(genesis_block_number, Default::default())?;

    Ok(())
}

// TN(TN-0028): upstream's `init_from_state_dump` (JSONL state import through ETL) and its
// parsers are removed; TN initializes from its genesis only.

/// Computes the state root (from scratch) based on the accounts and storages present in the
/// database.
fn compute_state_root<Provider>(
    provider: &Provider,
    prefix_sets: Option<TriePrefixSets>,
) -> Result<B256, InitStorageError>
where
    Provider: DBProvider<Tx: DbTxMut> + TrieWriter,
{
    trace!(target: "reth::cli", "Computing state root");

    let tx = provider.tx_ref();
    let mut intermediate_state: Option<IntermediateStateRootState> = None;
    let mut total_flushed_updates = 0;

    loop {
        let mut state_root =
            StateRootComputer::from_tx(tx).with_intermediate_state(intermediate_state);

        if let Some(sets) = prefix_sets.clone() {
            state_root = state_root.with_prefix_sets(sets);
        }

        match state_root.root_with_progress()? {
            StateRootProgress::Progress(state, _, updates) => {
                let updated_len = provider.write_trie_updates(updates)?;
                total_flushed_updates += updated_len;

                trace!(target: "reth::cli",
                    last_account_key = %state.account_root_state.last_hashed_key,
                    updated_len,
                    total_flushed_updates,
                    "Flushing trie updates"
                );

                intermediate_state = Some(*state);

                if total_flushed_updates.is_multiple_of(SOFT_LIMIT_COUNT_FLUSHED_UPDATES) {
                    info!(target: "reth::cli",
                        total_flushed_updates,
                        "Flushing trie updates"
                    );
                }
            }
            StateRootProgress::Complete(root, _, updates) => {
                let updated_len = provider.write_trie_updates(updates)?;
                total_flushed_updates += updated_len;

                trace!(target: "reth::cli",
                    %root,
                    updated_len,
                    total_flushed_updates,
                    "State root has been computed"
                );

                return Ok(root)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::constants::{
        HOLESKY_GENESIS_HASH, MAINNET_GENESIS_HASH, SEPOLIA_GENESIS_HASH,
    };
    use alloy_genesis::Genesis;
    use alloy_primitives::Bytes;
    use reth_chainspec::{Chain, ChainSpec, HOLESKY, MAINNET, SEPOLIA};
    use reth_db::DatabaseEnv;
    use reth_db_api::{
        cursor::DbCursorRO,
        models::{storage_sharded_key::StorageShardedKey, IntegerList, ShardedKey},
        table::{Table, TableRow},
        transaction::DbTx,
        Database,
    };
    use reth_provider::{
        test_utils::{create_test_provider_factory_with_chain_spec, MockNodeTypesWithDB},
        ProviderFactory, RocksDBProviderFactory,
    };
    use std::{collections::BTreeMap, sync::Arc};

    fn collect_table_entries<DB, T>(
        tx: &<DB as Database>::TX,
    ) -> Result<Vec<TableRow<T>>, InitStorageError>
    where
        DB: Database,
        T: Table,
    {
        Ok(tx.cursor_read::<T>()?.walk_range(..)?.collect::<Result<Vec<_>, _>>()?)
    }

    #[test]
    fn success_init_genesis_mainnet() {
        let genesis_hash =
            init_genesis(&create_test_provider_factory_with_chain_spec(MAINNET.clone())).unwrap();

        // actual, expected
        assert_eq!(genesis_hash, MAINNET_GENESIS_HASH);
    }

    #[test]
    fn success_init_genesis_sepolia() {
        let genesis_hash =
            init_genesis(&create_test_provider_factory_with_chain_spec(SEPOLIA.clone())).unwrap();

        // actual, expected
        assert_eq!(genesis_hash, SEPOLIA_GENESIS_HASH);
    }

    #[test]
    fn success_init_genesis_holesky() {
        let genesis_hash =
            init_genesis(&create_test_provider_factory_with_chain_spec(HOLESKY.clone())).unwrap();

        // actual, expected
        assert_eq!(genesis_hash, HOLESKY_GENESIS_HASH);
    }

    /// TN(TN-0083): a storage-v1 first launch that committed the genesis header to static files
    /// but not to the database resumes instead of failing on every restart.
    #[test]
    fn init_genesis_resumes_after_a_torn_first_launch() {
        let factory = create_test_provider_factory_with_chain_spec(SEPOLIA.clone());
        {
            let provider_rw = factory.database_provider_rw().unwrap();
            insert_genesis_header(&provider_rw, &factory.chain_spec()).unwrap();
            provider_rw.static_file_provider().commit().unwrap();
            // the MDBX transaction is dropped uncommitted: the crash between the two commits
        }
        assert_eq!(factory.block_hash(0).unwrap(), Some(SEPOLIA_GENESIS_HASH));
        assert!(factory.get_stage_checkpoint(StageId::Headers).unwrap().is_none());

        assert_eq!(init_genesis(&factory).unwrap(), SEPOLIA_GENESIS_HASH);
        assert!(factory.get_stage_checkpoint(StageId::Headers).unwrap().is_some());
        assert_eq!(factory.block_hash(0).unwrap(), Some(SEPOLIA_GENESIS_HASH));
        // the next start is the ordinary "already written" path
        assert_eq!(init_genesis(&factory).unwrap(), SEPOLIA_GENESIS_HASH);
    }

    /// TN(TN-0083): a database that holds any checkpoint without the Headers one is still
    /// refused; only an empty database is a torn first launch.
    #[test]
    fn init_genesis_refuses_a_partly_written_database() {
        let factory = create_test_provider_factory_with_chain_spec(SEPOLIA.clone());
        let provider_rw = factory.database_provider_rw().unwrap();
        insert_genesis_header(&provider_rw, &factory.chain_spec()).unwrap();
        provider_rw.save_stage_checkpoint(StageId::Finish, StageCheckpoint::new(0)).unwrap();
        provider_rw.commit().unwrap();

        assert!(matches!(
            init_genesis(&factory).unwrap_err(),
            InitStorageError::UninitializedDatabase
        ));
    }

    #[test]
    fn fail_init_inconsistent_db() {
        let factory = create_test_provider_factory_with_chain_spec(SEPOLIA.clone());
        let static_file_provider = factory.static_file_provider();
        let rocksdb_provider = factory.rocksdb_provider();
        init_genesis(&factory).unwrap();

        // Try to init db with a different genesis block
        let genesis_hash = init_genesis(
            &ProviderFactory::<MockNodeTypesWithDB>::new(
                factory.into_db(),
                MAINNET.clone(),
                static_file_provider,
                rocksdb_provider,
                reth_tasks::Runtime::test(),
            )
            .unwrap(),
        );

        assert!(matches!(
            genesis_hash.unwrap_err(),
            InitStorageError::GenesisHashMismatch {
                chainspec_hash: MAINNET_GENESIS_HASH,
                storage_hash: SEPOLIA_GENESIS_HASH
            }
        ))
    }

    #[test]
    fn init_genesis_history() {
        let address_with_balance = Address::with_last_byte(1);
        let address_with_storage = Address::with_last_byte(2);
        let storage_key = B256::with_last_byte(1);
        let chain_spec = Arc::new(ChainSpec {
            chain: Chain::from_id(1),
            genesis: Genesis {
                alloc: BTreeMap::from([
                    (
                        address_with_balance,
                        GenesisAccount { balance: U256::from(1), ..Default::default() },
                    ),
                    (
                        address_with_storage,
                        GenesisAccount {
                            storage: Some(BTreeMap::from([(storage_key, B256::random())])),
                            ..Default::default()
                        },
                    ),
                ]),
                ..Default::default()
            },
            hardforks: Default::default(),
            paris_block_and_final_difficulty: None,
            deposit_contract: None,
            ..Default::default()
        });

        let factory = create_test_provider_factory_with_chain_spec(chain_spec);
        init_genesis(&factory).unwrap();

        let expected_accounts = vec![
            (ShardedKey::new(address_with_balance, u64::MAX), IntegerList::new([0]).unwrap()),
            (ShardedKey::new(address_with_storage, u64::MAX), IntegerList::new([0]).unwrap()),
        ];
        let expected_storages = vec![(
            StorageShardedKey::new(address_with_storage, storage_key, u64::MAX),
            IntegerList::new([0]).unwrap(),
        )];

        let collect_from_mdbx = |factory: &ProviderFactory<MockNodeTypesWithDB>| {
            let provider = factory.provider().unwrap();
            let tx = provider.tx_ref();
            (
                collect_table_entries::<DatabaseEnv, tables::AccountsHistory>(tx).unwrap(),
                collect_table_entries::<DatabaseEnv, tables::StoragesHistory>(tx).unwrap(),
            )
        };

        {
            let (accounts, storages) = collect_from_mdbx(&factory);
            assert_eq!(accounts, expected_accounts);
            assert_eq!(storages, expected_storages);
        }
    }

    /// TN(TN-0112): a genesis-allocated slot reads its genesis value at a block after genesis
    /// under storage v2, as under v1. Genesis writes `StoragesHistory` with the plain slot; while
    /// the vendored v2 read history by `keccak256(slot)`, such a slot read as never written (zero).
    #[test]
    fn v2_genesis_storage_reads_after_genesis() {
        use reth_provider::{HistoricalStateProviderRef, StateProvider, StorageChangeSetReader};

        let address = Address::with_last_byte(2);
        let slot = B256::with_last_byte(1);
        let value = B256::with_last_byte(0x2a);
        let chain_spec = Arc::new(ChainSpec {
            chain: Chain::from_id(1),
            genesis: Genesis {
                alloc: BTreeMap::from([(
                    address,
                    GenesisAccount {
                        balance: U256::from(1),
                        // TN(TN-0116): v2 refuses genesis storage without code.
                        code: Some(Bytes::from_static(&[0x00])),
                        storage: Some(BTreeMap::from([(slot, value)])),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            },
            hardforks: Default::default(),
            paris_block_and_final_difficulty: None,
            deposit_contract: None,
            ..Default::default()
        });

        let mut layouts = Vec::new();
        for settings in [StorageSettings::v1(), StorageSettings::v2()] {
            let factory = create_test_provider_factory_with_chain_spec(chain_spec.clone());
            init_genesis_with_settings(&factory, settings).unwrap();
            let provider = factory.provider().unwrap();

            // The state at the start of block 1 is genesis'; at block 0 the slot is unset.
            let after_genesis = HistoricalStateProviderRef::new(&provider, 1);
            assert_eq!(
                after_genesis.storage(address, slot).unwrap(),
                Some(U256::from_be_bytes(value.0)),
                "{settings:?}"
            );
            let before_genesis = HistoricalStateProviderRef::new(&provider, 0);
            assert_eq!(
                before_genesis.storage(address, slot).unwrap().unwrap_or_default(),
                U256::ZERO,
                "{settings:?}"
            );

            let changeset = provider.storage_changeset(0).unwrap();
            assert_eq!(changeset.len(), 1, "{settings:?}");
            assert_eq!(changeset[0].1.key.as_b256(), slot, "{settings:?}: plain changeset slot");

            let tx = provider.tx_ref();
            layouts.push((
                collect_table_entries::<DatabaseEnv, tables::AccountsHistory>(tx).unwrap(),
                collect_table_entries::<DatabaseEnv, tables::StoragesHistory>(tx).unwrap(),
            ));
        }
        assert_eq!(layouts[0], layouts[1], "genesis history differs between v1 and v2");
    }

    /// TN(TN-0116): a v2 genesis that allocates storage to an account without code is refused
    /// before anything is written; v1, which can record a wipe of it, writes it.
    #[test]
    fn v2_genesis_refuses_storage_without_code() {
        let address = Address::with_last_byte(3);
        for code in [None, Some(Bytes::new())] {
            let chain_spec = Arc::new(ChainSpec {
                chain: Chain::from_id(1),
                genesis: Genesis {
                    alloc: BTreeMap::from([(
                        address,
                        GenesisAccount {
                            balance: U256::from(1),
                            code: code.clone(),
                            storage: Some(BTreeMap::from([(B256::ZERO, B256::with_last_byte(1))])),
                            ..Default::default()
                        },
                    )]),
                    ..Default::default()
                },
                hardforks: Default::default(),
                paris_block_and_final_difficulty: None,
                deposit_contract: None,
                ..Default::default()
            });

            let factory = create_test_provider_factory_with_chain_spec(chain_spec.clone());
            let err = init_genesis_with_settings(&factory, StorageSettings::v2()).unwrap_err();
            assert!(
                matches!(err, InitStorageError::GenesisStorageWithoutCode(a) if a == address),
                "{code:?}: {err}"
            );
            let static_files = factory.static_file_provider();
            assert_eq!(
                static_files.get_highest_static_file_block(StaticFileSegment::Headers),
                None
            );
            assert_eq!(factory.storage_settings().unwrap(), None, "{code:?}: settings written");

            let factory = create_test_provider_factory_with_chain_spec(chain_spec);
            init_genesis_with_settings(&factory, StorageSettings::v1()).unwrap();
        }
    }

    #[test]
    fn warn_storage_settings_mismatch() {
        let factory = create_test_provider_factory_with_chain_spec(MAINNET.clone());
        init_genesis_with_settings(&factory, StorageSettings::v1()).unwrap();

        // Request different settings - should warn but succeed
        let result = init_genesis_with_settings(&factory, StorageSettings::v2());

        // Should succeed (warning is logged, not an error)
        assert!(result.is_ok());
    }

    #[test]
    fn allow_same_storage_settings() {
        let factory = create_test_provider_factory_with_chain_spec(MAINNET.clone());
        let settings = StorageSettings::v2();
        init_genesis_with_settings(&factory, settings).unwrap();

        let result = init_genesis_with_settings(&factory, settings);

        assert!(result.is_ok());
    }
}
