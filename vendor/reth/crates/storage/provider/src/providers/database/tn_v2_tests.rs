//! TN(TN-0045): storage v2 without `RocksDB`, checked against v1 on the same generated chain.
//!
//! Each test generates one chain of executed blocks with transactions, account changes, slots
//! that change in several blocks, and an account a block destroys and re-creates. It saves the
//! chain with `save_blocks` into a v1 and a v2 factory, in the same batches, and compares what the
//! two layouts answer. Every historical answer is also checked against the state the generator
//! tracked, so a fault both layouts share fails too.

use crate::{
    test_utils::{create_test_provider_factory, MockNodeTypesWithDB},
    BlockExecutionWriter, ChangeSetReader, HistoricalStateProviderRef, HistoryWriter,
    ProviderFactory, SaveBlocksMode, StorageChangeSetReader, TransactionsProvider,
};
use alloy_consensus::constants::KECCAK_EMPTY;
use alloy_primitives::{
    keccak256, map::HashMap, Address, BlockNumber, Bytes, TxHash, TxNumber, B256, U256,
};
use reth_chain_state::{ComputedTrieData, ExecutedBlock};
use reth_db_api::{
    cursor::DbCursorRO,
    models::{storage_sharded_key::StorageShardedKey, ShardedKey, StorageSettings},
    table::{Table, TableRow},
    tables,
    transaction::DbTx,
};
use reth_ethereum_primitives::{Block, Receipt, TransactionSigned};
use reth_execution_types::{BlockExecutionOutput, BlockExecutionResult};
use reth_primitives_traits::{Account, BlockBody as _, SealedBlock, SealedHeader};
use reth_storage_api::{AccountReader, DBProvider, StateProvider, StorageSettingsCache};
use reth_testing_utils::generators::{self, random_block, BlockParams};
use reth_trie::{HashedPostState, KeccakKeyHasher};
use revm_database::{AccountStatus, BundleState, RevertToSlot};
use revm_state::{AccountInfo, Bytecode};
use std::{collections::BTreeMap, fmt::Debug, ops::RangeInclusive, sync::Arc};

type Factory = ProviderFactory<MockNodeTypesWithDB>;

/// One account's historical answers at one block: the account, then each slot's value.
type HistoricalRead = (BlockNumber, Address, Option<Account>, Vec<Option<U256>>);

/// Accounts the generator changes, by last address byte.
const ACCOUNTS: u8 = 6;
/// Slots per account the generator changes.
const SLOTS: u64 = 5;
/// The last generated block.
const TIP: BlockNumber = 12;
/// The `save_blocks` calls, so the chain has single and multi-block saves.
const BATCHES: [RangeInclusive<BlockNumber>; 4] = [1..=3, 4..=4, 5..=9, 10..=12];
/// An account, by last address byte, that block 2 creates without storage and block
/// [`RECREATED_AT`] destroys and re-creates with [`RECREATED_SLOT`] set to 7: a storage wipe
/// with a slot revert of `Destroyed`. `address(ACCOUNTS)` is the account nothing touches.
const RECREATED: u8 = ACCOUNTS + 1;
/// The block that destroys and re-creates [`RECREATED`], inside the batch `5..=9`.
const RECREATED_AT: BlockNumber = 7;
/// The re-created account's slot. The reads cover it, but the generator never writes it, so the
/// wipe block is its only history entry.
const RECREATED_SLOT: U256 = U256::from_limbs([SLOTS + 1, 0, 0, 0]);

fn address(i: u8) -> Address {
    Address::with_last_byte(0x40 + i)
}

/// The state after one block.
#[derive(Clone, Debug, Default)]
struct Snapshot {
    accounts: BTreeMap<Address, AccountInfo>,
    storage: BTreeMap<(Address, U256), U256>,
}

impl Snapshot {
    fn account(&self, address: Address) -> Option<Account> {
        self.accounts.get(&address).map(|info| Account {
            nonce: info.nonce,
            balance: info.balance,
            bytecode_hash: None,
        })
    }

    fn slot(&self, address: Address, slot: U256) -> U256 {
        self.storage.get(&(address, slot)).copied().unwrap_or_default()
    }
}

/// A generated chain: blocks 0 (empty genesis) to [`TIP`], the state after each block, and each
/// transaction with its number.
struct Chain {
    blocks: Vec<ExecutedBlock>,
    states: Vec<Snapshot>,
    txs: Vec<(TxHash, TxNumber, TransactionSigned)>,
}

impl Chain {
    fn generate() -> Self {
        let mut rng = generators::rng_with_seed(b"tn-0045 storage v2 differential");
        let mut state = Snapshot::default();
        let mut states = vec![state.clone()];
        let mut txs = Vec::new();
        let mut next_tx: TxNumber = 0;

        let genesis = SealedBlock::<Block>::from_sealed_parts(
            SealedHeader::new(
                alloy_consensus::Header { number: 0, ..Default::default() },
                B256::ZERO,
            ),
            Default::default(),
        );
        let mut parent = genesis.hash();
        let mut blocks = vec![executed(genesis, BundleState::default())];

        for number in 1..=TIP {
            let block = random_block(
                &mut rng,
                number,
                BlockParams {
                    parent: Some(parent),
                    tx_count: Some(1 + (number % 3) as u8),
                    ommers_count: Some(0),
                    ..Default::default()
                },
            );
            parent = block.hash();
            for tx in block.body().transactions() {
                txs.push((*tx.tx_hash(), next_tx, tx.clone()));
                next_tx += 1;
            }

            let mut builder = BundleState::builder(number..=number);
            if number == 1 {
                // The empty bytecode is in a bundle's contracts too; `to_plain_state` drops it.
                let code = Bytecode::new_raw(Bytes::from_static(&[0x60, 0x00, 0x60, 0x00, 0xf3]));
                builder = builder
                    .contract(KECCAK_EMPTY, Bytecode::new())
                    .contract(code.hash_slow(), code);
            }
            for i in 0..ACCOUNTS {
                let address = address(i);
                let before = state.accounts.get(&address).cloned();
                // A deterministic mix: some accounts change their info, some only their
                // storage (an info revert of `DoNothing`), some both, some nothing.
                let pick = (number as u8).wrapping_mul(7).wrapping_add(i * 3) % 5;
                let change_info = before.is_none() || pick == 0 || pick == 3;
                let change_storage =
                    pick == 1 || pick == 3 || (before.is_none() && i.is_multiple_of(2));
                if !change_info && !change_storage {
                    continue
                }

                let mut info = before.clone().unwrap_or_default();
                if change_info {
                    info.nonce += 1;
                    info.balance = U256::from(number * 1000 + i as u64);
                    builder = builder.revert_account_info(number, address, Some(before.clone()));
                }
                if let Some(before) = &before {
                    builder = builder.state_original_account_info(address, before.clone());
                }
                builder = builder.state_present_account_info(address, info.clone());
                state.accounts.insert(address, info);

                if change_storage {
                    let mut present = HashMap::default();
                    let mut reverts = Vec::new();
                    for s in 0..SLOTS {
                        if !(number + s + i as u64).is_multiple_of(3) {
                            continue
                        }
                        let slot = U256::from(s + 1);
                        let old = state.slot(address, slot);
                        // Every fourth write clears the slot, so deletions are covered.
                        let new = if (number + s).is_multiple_of(4) {
                            U256::ZERO
                        } else {
                            U256::from(number * 100 + s)
                        };
                        present.insert(slot, (old, new));
                        reverts.push((slot, old));
                        if new.is_zero() {
                            state.storage.remove(&(address, slot));
                        } else {
                            state.storage.insert((address, slot), new);
                        }
                    }
                    if !present.is_empty() {
                        builder = builder
                            .state_storage(address, present)
                            .revert_storage(number, address, reverts);
                    }
                }
            }
            let recreated = address(RECREATED);
            if number == 2 {
                let info = AccountInfo { nonce: 1, balance: U256::from(2), ..Default::default() };
                builder = builder
                    .state_present_account_info(recreated, info.clone())
                    .revert_account_info(number, recreated, Some(None));
                state.accounts.insert(recreated, info);
            }
            if number == RECREATED_AT {
                let before = state.accounts[&recreated].clone();
                let info = AccountInfo { nonce: 1, balance: U256::from(7), ..Default::default() };
                builder = builder
                    .state_original_account_info(recreated, before.clone())
                    .state_present_account_info(recreated, info.clone())
                    .state_storage(
                        recreated,
                        HashMap::from_iter([(RECREATED_SLOT, (U256::ZERO, U256::from(7)))]),
                    )
                    .revert_account_info(number, recreated, Some(Some(before)))
                    .revert_storage(number, recreated, vec![(RECREATED_SLOT, U256::ZERO)]);
                state.accounts.insert(recreated, info);
                state.storage.insert((recreated, RECREATED_SLOT), U256::from(7));
            }
            let mut bundle = builder.build();
            if number == RECREATED_AT {
                // TN(TN-0043): what revm leaves for an account destroyed and re-created in the
                // block, which the builder cannot express.
                bundle.state.get_mut(&recreated).unwrap().status = AccountStatus::DestroyedChanged;
                let revert = bundle.reverts[0]
                    .iter_mut()
                    .find_map(|(address, revert)| (*address == recreated).then_some(revert))
                    .unwrap();
                revert.wipe_storage = true;
                revert.storage.insert(RECREATED_SLOT, RevertToSlot::Destroyed);
            }
            blocks.push(executed(block, bundle));
            states.push(state.clone());
        }

        Self { blocks, states, txs }
    }

    /// The blocks of one batch.
    fn batch(&self, range: &RangeInclusive<BlockNumber>) -> Vec<ExecutedBlock> {
        self.blocks[*range.start() as usize..=*range.end() as usize].to_vec()
    }

    /// A factory with genesis and the batches that end at or below `tip`.
    fn save(&self, settings: StorageSettings, tip: BlockNumber) -> Factory {
        let factory = create_test_provider_factory();
        factory.set_storage_settings_cache(settings);
        self.save_batches(&factory, 0..=0);
        for range in BATCHES.iter().filter(|range| *range.end() <= tip) {
            self.save_batches(&factory, range.clone());
        }
        factory
    }

    fn save_batches(&self, factory: &Factory, range: RangeInclusive<BlockNumber>) {
        let provider_rw = factory.provider_rw().unwrap();
        provider_rw.save_blocks(self.batch(&range), SaveBlocksMode::Full).unwrap();
        provider_rw.commit().unwrap();
    }
}

/// An executed block with one receipt per transaction and the bundle's hashed state.
pub(super) fn executed(block: SealedBlock<Block>, bundle: BundleState) -> ExecutedBlock {
    let receipts = vec![Receipt::default(); block.body().transaction_count()];
    let hashed_state =
        HashedPostState::from_bundle_state::<KeccakKeyHasher>(bundle.state()).into_sorted();
    ExecutedBlock::new(
        Arc::new(block.try_recover().unwrap()),
        Arc::new(BlockExecutionOutput {
            result: BlockExecutionResult {
                receipts,
                requests: Default::default(),
                gas_used: 0,
                blob_gas_used: 0,
            },
            state: bundle,
        }),
        ComputedTrieData { hashed_state: Arc::new(hashed_state), ..Default::default() },
    )
}

fn rows<T: Table>(factory: &Factory) -> Vec<TableRow<T>> {
    let provider = factory.provider().unwrap();
    provider.tx_ref().cursor_read::<T>().unwrap().walk(None).unwrap().map(Result::unwrap).collect()
}

/// The three tables `RocksDB` held under upstream's v2.
fn index_tables(factory: &Factory) -> impl PartialEq + Debug {
    (
        rows::<tables::AccountsHistory>(factory),
        rows::<tables::StoragesHistory>(factory),
        rows::<tables::TransactionHashNumbers>(factory),
    )
}

/// Every historical account and storage read at the start of each block `1..=tip + 1`,
/// checked against the generator's state. Returns the raw answers for a v1/v2 comparison.
fn historical_reads(factory: &Factory, chain: &Chain, tip: BlockNumber) -> Vec<HistoricalRead> {
    let provider = factory.provider().unwrap();
    let mut answers = Vec::new();
    for block in 1..=tip + 1 {
        let state = HistoricalStateProviderRef::new(&provider, block);
        let expected = &chain.states[block as usize - 1];
        for i in 0..=RECREATED {
            let address = address(i);
            let account = state.basic_account(&address).unwrap();
            assert_eq!(account, expected.account(address), "account {address} at block {block}");
            let mut slots = Vec::new();
            for s in 0..SLOTS + 1 {
                let slot = U256::from(s + 1);
                let value = state.storage(address, B256::from(slot)).unwrap();
                assert_eq!(
                    value.unwrap_or_default(),
                    expected.slot(address, slot),
                    "slot {slot} of {address} at block {block}"
                );
                slots.push(value);
            }
            answers.push((block, address, account, slots));
        }
    }
    answers
}

/// Every account and slot of the state at the tip has a history shard. A key without one reads
/// as never written at every block (`HistoryInfo::NotYetWritten`), not as an error, so a missing
/// history entry fails no read on its own. The keys come from the tip state, not from the
/// generator, whose preimages only name them.
fn assert_history_covers_tip_state(factory: &Factory) {
    let provider = factory.provider().unwrap();
    let tx = provider.tx_ref();
    let addresses: HashMap<B256, Address> =
        (0..=RECREATED).map(address).map(|address| (keccak256(address), address)).collect();
    let slots: HashMap<B256, B256> = (1..=SLOTS + 1)
        .map(|s| B256::from(U256::from(s)))
        .map(|slot| (keccak256(slot), slot))
        .collect();

    let mut history = tx.cursor_read::<tables::AccountsHistory>().unwrap();
    let mut accounts = 0;
    for entry in tx.cursor_read::<tables::HashedAccounts>().unwrap().walk(None).unwrap() {
        let address = addresses[&entry.unwrap().0];
        let shard = history.seek(ShardedKey::new(address, 0)).unwrap();
        assert!(shard.is_some_and(|(key, _)| key.key == address), "{address}: no history");
        accounts += 1;
    }

    let mut history = tx.cursor_read::<tables::StoragesHistory>().unwrap();
    let mut storage = 0;
    for entry in tx.cursor_dup_read::<tables::HashedStorages>().unwrap().walk(None).unwrap() {
        let (hashed_address, entry) = entry.unwrap();
        let (address, slot) = (addresses[&hashed_address], slots[&entry.key]);
        let shard = history.seek(StorageShardedKey::new(address, slot, 0)).unwrap();
        assert!(
            shard.is_some_and(|(key, _)| key.address == address && key.sharded_key.key == slot),
            "slot {slot} of {address}: no history"
        );
        storage += 1;
    }
    assert!(accounts > 0 && storage > 0, "{accounts} accounts, {storage} slots at the tip");
}

/// Each block's changesets, sorted, for a v1/v2 comparison.
fn changesets(factory: &Factory, tip: BlockNumber) -> impl PartialEq + Debug {
    let provider = factory.provider().unwrap();
    (0..=tip)
        .map(|block| {
            let mut accounts = provider.account_block_changeset(block).unwrap();
            accounts.sort_by_key(|change| change.address);
            let mut storage = provider.storage_changeset(block).unwrap();
            storage.sort();
            (accounts, storage)
        })
        .collect::<Vec<_>>()
}

/// Every transaction of blocks `..=tip` is found by hash; the later ones are not.
fn assert_tx_lookups(factory: &Factory, chain: &Chain, tip: BlockNumber) {
    let provider = factory.provider().unwrap();
    let kept: usize = chain.blocks[..=tip as usize]
        .iter()
        .map(|block| block.recovered_block().body().transaction_count())
        .sum();
    let (kept, removed) = chain.txs.split_at(kept);
    for (hash, number, tx) in kept {
        assert_eq!(provider.transaction_id(*hash).unwrap(), Some(*number), "tx {hash}");
        assert_eq!(provider.transaction_by_hash(*hash).unwrap().as_ref(), Some(tx), "tx {hash}");
    }
    for (hash, ..) in removed {
        assert_eq!(provider.transaction_id(*hash).unwrap(), None, "removed tx {hash}");
    }
}

/// The three ex-`RocksDB` tables are written under v2, identical to v1's, and every historical
/// read, changeset and transaction lookup agrees with v1 and with the generated state, including
/// those of an account whose storage a block wiped.
#[test]
fn v2_matches_v1_on_a_generated_chain() {
    let chain = Chain::generate();
    let v1 = chain.save(StorageSettings::v1(), TIP);
    let v2 = chain.save(StorageSettings::v2(), TIP);

    assert!(!rows::<tables::AccountsHistory>(&v2).is_empty(), "v2 AccountsHistory is empty");
    assert!(!rows::<tables::StoragesHistory>(&v2).is_empty(), "v2 StoragesHistory is empty");
    assert!(
        !rows::<tables::TransactionHashNumbers>(&v2).is_empty(),
        "v2 TransactionHashNumbers is empty"
    );
    assert_eq!(index_tables(&v2), index_tables(&v1), "index tables differ between v1 and v2");
    // TN(TN-0041): v2's bytecode-only `write_state` writes the rows v1's conversion writes.
    let bytecodes = rows::<tables::Bytecodes>(&v2);
    assert_eq!(bytecodes.len(), 1, "v2 Bytecodes: {bytecodes:?}");
    assert_eq!(bytecodes, rows::<tables::Bytecodes>(&v1), "Bytecodes differ between v1 and v2");
    assert_eq!(changesets(&v2, TIP), changesets(&v1, TIP), "changesets differ between v1 and v2");
    assert_eq!(historical_reads(&v2, &chain, TIP), historical_reads(&v1, &chain, TIP));
    assert_tx_lookups(&v1, &chain, TIP);
    assert_tx_lookups(&v2, &chain, TIP);

    // TN(TN-0043): the wipe block is the re-created slot's only history entry, so a read after
    // it finds the value only if the wiped account's slot revert made it into the history.
    for factory in [&v1, &v2] {
        let provider = factory.provider().unwrap();
        let state = HistoricalStateProviderRef::new(&provider, RECREATED_AT + 1);
        let slot = state.storage(address(RECREATED), B256::from(RECREATED_SLOT)).unwrap();
        assert_eq!(slot, Some(U256::from(7)), "the re-created slot after the wipe");
    }
}

/// Removing blocks with `remove_block_and_execution_above` leaves the tables a save of the
/// shorter chain writes, reads answer for the shorter chain, and saving the removed blocks
/// again rebuilds the full chain's tables, in both layouts.
#[test]
fn v2_history_unwind_round_trip() {
    let chain = Chain::generate();
    let keep = 4;
    for settings in [StorageSettings::v1(), StorageSettings::v2()] {
        let full = chain.save(settings, TIP);
        let short = chain.save(settings, keep);

        let provider_rw = full.unwind_provider_rw().unwrap();
        provider_rw.remove_block_and_execution_above(keep).unwrap();
        provider_rw.commit().unwrap();

        assert_eq!(index_tables(&full), index_tables(&short), "{settings:?}: unwound tables");
        assert_eq!(changesets(&full, keep), changesets(&short, keep), "{settings:?}");
        historical_reads(&full, &chain, keep);
        assert_tx_lookups(&full, &chain, keep);

        for range in BATCHES.iter().filter(|range| *range.start() > keep) {
            chain.save_batches(&full, range.clone());
        }
        let rebuilt = chain.save(settings, TIP);
        assert_eq!(index_tables(&full), index_tables(&rebuilt), "{settings:?}: re-saved tables");
        historical_reads(&full, &chain, TIP);
        assert_tx_lookups(&full, &chain, TIP);
    }
}

/// TN(TN-0043): in both layouts every key of the tip state has history: after a full save, after
/// an unwind to just before the wipe block, and after a re-save of the rest in one batch.
#[test]
fn v2_history_covers_the_tip_state() {
    let chain = Chain::generate();
    let keep = RECREATED_AT - 1;
    for settings in [StorageSettings::v1(), StorageSettings::v2()] {
        let factory = chain.save(settings, TIP);
        assert_history_covers_tip_state(&factory);

        let provider_rw = factory.unwind_provider_rw().unwrap();
        provider_rw.remove_block_and_execution_above(keep).unwrap();
        provider_rw.commit().unwrap();
        assert_history_covers_tip_state(&factory);

        chain.save_batches(&factory, keep + 1..=TIP);
        assert_history_covers_tip_state(&factory);
    }
}

/// Saves a block whose revert wipes `address`'s storage.
fn save_wipe(
    factory: &Factory,
    number: BlockNumber,
    parent: B256,
    address: Address,
) -> Result<(), String> {
    let mut rng = generators::rng_with_seed(b"tn-0043 wipe");
    let block = random_block(
        &mut rng,
        number,
        BlockParams {
            parent: Some(parent),
            tx_count: Some(0),
            ommers_count: Some(0),
            ..Default::default()
        },
    );
    let info = AccountInfo { nonce: 1, ..Default::default() };
    let mut bundle = BundleState::builder(number..=number)
        .state_original_account_info(address, info.clone())
        .state_present_account_info(address, info.clone())
        .revert_account_info(number, address, Some(Some(info)))
        .build();
    bundle.reverts[0].iter_mut().for_each(|(_, revert)| revert.wipe_storage = true);
    let provider_rw = factory.provider_rw().unwrap();
    provider_rw
        .save_blocks(vec![executed(block, bundle)], SaveBlocksMode::Full)
        .map_err(|err| err.to_string())?;
    provider_rw.commit().unwrap();
    Ok(())
}

/// TN(TN-0043): under v2 a block that wipes an account with storage fails instead of leaving
/// the removed slots out of the changesets and the history. A wipe of an account without
/// storage still saves.
#[test]
fn v2_wipe_of_existing_storage_fails_closed() {
    let chain = Chain::generate();
    let v2 = chain.save(StorageSettings::v2(), 3);
    let parent = chain.blocks[3].recovered_block().hash();
    let with_storage = *chain.states[3].storage.keys().next().map(|(a, _)| a).unwrap();
    let without_storage = Address::with_last_byte(0xee);

    let err = save_wipe(&v2, 4, parent, with_storage).unwrap_err();
    assert!(err.contains("wiped the storage"), "{err}");

    // A fresh factory: the failed save above left uncommitted static-file rows behind.
    save_wipe(&chain.save(StorageSettings::v2(), 3), 4, parent, without_storage).unwrap();
}

/// TN(TN-0043): a wipe of storage written by an earlier block of the same batch fails too: the
/// database does not hold that storage yet.
#[test]
fn v2_wipe_of_storage_written_earlier_in_the_batch_fails_closed() {
    let chain = Chain::generate();
    let v2 = chain.save(StorageSettings::v2(), 0);
    let mut blocks = chain.batch(&(1..=1));
    let written: Vec<_> = chain.states[1].storage.keys().map(|(address, _)| *address).collect();
    let address = written[0];

    let mut rng = generators::rng_with_seed(b"tn-0043 wipe in batch");
    let block = random_block(
        &mut rng,
        2,
        BlockParams {
            parent: Some(blocks[0].recovered_block().hash()),
            tx_count: Some(0),
            ommers_count: Some(0),
            ..Default::default()
        },
    );
    let info = chain.states[1].accounts[&address].clone();
    let mut bundle = BundleState::builder(2..=2)
        .state_original_account_info(address, info.clone())
        .state_present_account_info(address, info.clone())
        .revert_account_info(2, address, Some(Some(info)))
        .build();
    bundle.reverts[0].iter_mut().for_each(|(_, revert)| revert.wipe_storage = true);
    blocks.push(executed(block, bundle));

    let provider_rw = v2.provider_rw().unwrap();
    let err = provider_rw.save_blocks(blocks, SaveBlocksMode::Full).unwrap_err().to_string();
    assert!(err.contains("wiped the storage"), "{err}");
}

/// TN(TN-0043): under v2 `update_history_indices` refuses blocks whose changesets the static
/// files have not committed, the trap `save_blocks` avoids by indexing from the reverts.
#[test]
fn v2_update_history_indices_refuses_uncommitted_changesets() {
    let chain = Chain::generate();

    // Committed blocks re-index to what `save_blocks` wrote.
    let v2 = chain.save(StorageSettings::v2(), TIP);
    let before = index_tables(&v2);
    let provider_rw = v2.provider_rw().unwrap();
    provider_rw.unwind_account_history_indices_range(5..=TIP).unwrap();
    provider_rw.unwind_storage_history_indices_range(5..=TIP).unwrap();
    provider_rw.update_history_indices(5..=TIP).unwrap();
    provider_rw.commit().unwrap();
    assert_eq!(index_tables(&v2), before);

    // Blocks saved in this transaction are not in the committed static files yet.
    let v2 = chain.save(StorageSettings::v2(), 9);
    let provider_rw = v2.provider_rw().unwrap();
    provider_rw.save_blocks(chain.batch(&(10..=12)), SaveBlocksMode::Full).unwrap();
    let err = provider_rw.update_history_indices(10..=12).unwrap_err().to_string();
    assert!(err.contains("cannot index history"), "{err}");
}
