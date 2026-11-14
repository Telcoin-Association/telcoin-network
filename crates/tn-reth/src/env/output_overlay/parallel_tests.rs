//! Parallel storage roots give the serial root and trie updates, and every failure falls back to
//! the serial computation.
//!
//! The fixture persists a base state (a hot contract, many small contracts, EOAs) so the database
//! holds real account and storage trie nodes, then drives multi-block outputs through the output
//! overlay, comparing each block's parallel root with the serial one over the same transaction.

use super::{
    block_is_checked, checked, storage_root_targets, DatabaseError, ParallelRoot, ParallelRoots,
    ProviderError, StorageRootSource as _, StorageRootTasks, StorageSplits, TaskError,
    CHECK_ONE_BLOCK_IN, FAIL_NEXT_WALK, INJECTED_WALK_FAILURE, TASKS_STARTED, TASK_ROOTS_TAKEN,
};
use crate::{
    env::output_overlay::{AccountWalk, StorageWalk, Walks},
    OutputTrieOverlay, RethChainSpec, RethEnv,
};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use reth_chain_state::ComputedTrieData;
use reth_primitives_traits::Account;
use reth_provider::{
    ChainStateBlockWriter as _, DBProvider as _, DatabaseProviderFactory as _, StateWriter as _,
    TrieWriter as _,
};
use reth_trie::{updates::TrieUpdates, HashedPostState, HashedStorage};
use std::sync::Arc;
use tempfile::TempDir;
use tn_types::{test_genesis, TaskManager, B256, U256};

/// Slots of the hot contract in the base state.
const HOT_SLOTS: u64 = 2_000;
/// Small contracts in the base state, with one to three slots each.
const SMALL_CONTRACTS: u64 = 300;
/// EOAs in the base state.
const EOAS: u64 = 2_000;

/// A deterministic key from a domain tag and an index.
fn key(domain: u8, index: u64) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&index.to_be_bytes());
    bytes[8] = domain;
    tn_types::keccak256(bytes)
}

/// The hot contract's hashed address.
fn hot() -> B256 {
    key(1, 0)
}

/// Small contract `i`'s hashed address.
fn small(i: u64) -> B256 {
    key(2, i)
}

/// EOA `i`'s hashed address.
fn eoa(i: u64) -> B256 {
    key(3, i)
}

/// Slot `i`'s hashed key.
fn slot(i: u64) -> B256 {
    key(4, i)
}

/// A contract account with nonce `nonce`.
fn contract(nonce: u64) -> Option<Account> {
    Some(Account { nonce, balance: U256::from(nonce), bytecode_hash: Some(key(5, 0)) })
}

/// An EOA with nonce `nonce`.
fn account(nonce: u64) -> Option<Account> {
    Some(Account { nonce, balance: U256::from(nonce * 7 + 1), bytecode_hash: None })
}

/// A temp chain with the base state persisted (finalized block 1).
struct Fixture {
    env: RethEnv,
    _tmp: TempDir,
    _tasks: TaskManager,
    /// The finalized block number the next persist writes.
    next_tip: u64,
}

impl Fixture {
    fn new() -> eyre::Result<Self> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let tmp = TempDir::new()?;
        let tasks = TaskManager::new("parallel storage roots");
        let env = RethEnv::new_for_temp_chain(chain, tmp.path(), &tasks, None)?;
        let mut fixture = Self { env, _tmp: tmp, _tasks: tasks, next_tip: 1 };

        let mut base =
            HashedPostState::default().with_accounts([(hot(), contract(1))]).with_storages([(
                hot(),
                HashedStorage::from_iter(
                    false,
                    (0..HOT_SLOTS).map(|i| (slot(i), U256::from(i + 1))),
                ),
            )]);
        for i in 0..SMALL_CONTRACTS {
            base.accounts.insert(small(i), contract(i + 2));
            base.storages.insert(
                small(i),
                HashedStorage::from_iter(
                    false,
                    (0..1 + i % 3).map(|s| (slot(s), U256::from(i + s + 1))),
                ),
            );
        }
        for i in 0..EOAS {
            base.accounts.insert(eoa(i), account(i));
        }
        fixture.persist(&[base])?;
        Ok(fixture)
    }

    /// Persist `blocks` as one output: each block's state and the trie updates of its serial root
    /// over the output's earlier blocks, then advance the finalized block number, as a persist
    /// does.
    fn persist(&mut self, blocks: &[HashedPostState]) -> eyre::Result<()> {
        let factory = self.env.blockchain_provider();
        let provider = factory.database_provider_ro()?;
        let mut overlay = OutputTrieOverlay::new();
        let mut sealed = Vec::new();
        for block in blocks {
            let (_, updates) =
                overlay.layered_root_with_updates(provider.tx_ref(), block.clone())?;
            let state = Arc::new(block.clone().into_sorted());
            let nodes = Arc::new(updates.into_sorted());
            overlay.extend_from_block(&ComputedTrieData::without_trie_input(
                Arc::clone(&state),
                Arc::clone(&nodes),
            ));
            sealed.push((state, nodes));
        }
        drop(provider);
        let rw = factory.database_provider_rw()?;
        for (state, nodes) in &sealed {
            rw.write_hashed_state(state)?;
            rw.write_trie_updates_sorted(nodes)?;
        }
        rw.save_finalized_block_number(self.next_tip)?;
        rw.commit()?;
        self.next_tip += 1;
        Ok(())
    }

    /// Run `blocks` as one output: each block's root serially, with the tasks unchecked (each walk
    /// on one thread or task, then split) and in shadow mode, over one transaction; all must
    /// agree. Returns the task results the walks took.
    fn check_output(&self, blocks: &[HashedPostState]) -> eyre::Result<usize> {
        let factory = self.env.blockchain_provider();
        let provider = factory.database_provider_ro()?;
        let tx = provider.tx_ref();
        let mut overlay = OutputTrieOverlay::new();
        let taken_before = TASK_ROOTS_TAKEN.with(|n| n.get());
        for (i, block) in blocks.iter().enumerate() {
            let serial = overlay.layered_root_with_updates(tx, block.clone())?;
            for account in [AccountWalk::OneThread, AccountWalk::Split] {
                for storage in [StorageWalk::OneTask, StorageWalk::Split] {
                    let walk = Walks::new(account, storage);
                    let parallel = overlay.layered_root_with_updates_parallel(
                        factory,
                        tx,
                        block.clone(),
                        walk,
                    )?;
                    assert_eq!(parallel.0, serial.0, "state root differs at block {i} ({walk:?})");
                    assert_eq!(parallel.1, serial.1, "trie updates differ at block {i} ({walk:?})");
                }
            }
            let shadow = overlay.block_root_with_updates(
                factory,
                tx,
                block.clone(),
                ParallelRoots::Shadow(Walks::SPLIT),
                1,
            )?;
            assert_eq!(shadow, serial, "shadow result differs at block {i}");
            overlay.extend_from_block(&ComputedTrieData::without_trie_input(
                Arc::new(block.clone().into_sorted()),
                Arc::new(serial.1.into_sorted()),
            ));
        }
        Ok(TASK_ROOTS_TAKEN.with(|n| n.get()) - taken_before)
    }
}

/// A random block over the fixture's keys plus new ones: account updates, creations and
/// destructions, slot writes and deletions, storage wipes with and without new slots, destroyed
/// contracts re-created with other storage, storage-only changes, and keys earlier blocks of the
/// output changed.
fn random_block(rng: &mut StdRng, block: u64) -> HashedPostState {
    let mut state = HashedPostState::default();
    let value = |rng: &mut StdRng| U256::from(rng.random_range(1..u64::MAX));
    for _ in 0..rng.random_range(0..40) {
        let i = rng.random_range(0..EOAS + 50);
        let info = if rng.random_ratio(1, 8) { None } else { account(block * 1_000 + i) };
        state.accounts.insert(eoa(i), info);
    }
    // the hot contract: many slots most blocks, sometimes untouched
    if rng.random_ratio(4, 5) {
        let slots = (0..rng.random_range(1..300)).map(|_| {
            let s = rng.random_range(0..HOT_SLOTS + 200);
            (slot(s), if rng.random_ratio(1, 6) { U256::ZERO } else { value(rng) })
        });
        let storage = HashedStorage::from_iter(false, slots.collect::<Vec<_>>());
        state.storages.insert(hot(), storage);
        if rng.random_bool(0.5) {
            state.accounts.insert(hot(), contract(block));
        }
    }
    for _ in 0..rng.random_range(0..30) {
        let i = rng.random_range(0..SMALL_CONTRACTS + 20);
        let roll = rng.random_range(0..10);
        let (info, storage) = match roll {
            // destroyed: account removed, storage wiped
            0 => (Some(None), HashedStorage::from_iter(true, [])),
            // wiped and re-created in this block with other slots
            1 => (Some(contract(block)), HashedStorage::from_iter(true, [(slot(9), value(rng))])),
            // a slot deleted
            2 => (None, HashedStorage::from_iter(false, [(slot(0), U256::ZERO)])),
            // storage-only change: the account leaf comes from earlier blocks or the database
            3..=5 => (
                None,
                HashedStorage::from_iter(false, [(slot(rng.random_range(0..5)), value(rng))]),
            ),
            _ => (
                Some(contract(block + i)),
                HashedStorage::from_iter(false, [(slot(1), value(rng))]),
            ),
        };
        if let Some(info) = info {
            state.accounts.insert(small(i), info);
        }
        state.storages.insert(small(i), storage);
    }
    state
}

/// Random multi-block outputs, persisted between outputs so later outputs read their tries from
/// the database: the parallel root equals the serial one on every block.
#[tokio::test]
async fn random_outputs_parallel_equals_serial() -> eyre::Result<()> {
    let mut fixture = Fixture::new()?;
    let mut taken = 0;
    for seed in 0..4u64 {
        let mut rng = StdRng::seed_from_u64(0x7a5c_0000 + seed);
        let blocks: Vec<_> = (0..10).map(|b| random_block(&mut rng, seed * 100 + b)).collect();
        taken += fixture.check_output(&blocks)?;
        fixture.persist(&blocks)?;
    }
    assert!(taken > 0, "no block took a task's storage root: the tasks were never exercised");
    Ok(())
}

/// One hot contract: a single task with many slots, overlapped with the account walk.
#[tokio::test]
async fn one_hot_contract_parallel_equals_serial() -> eyre::Result<()> {
    let fixture = Fixture::new()?;
    let blocks: Vec<_> = (0..4u64)
        .map(|b| {
            HashedPostState::default()
                .with_accounts((0..50).map(|i| (eoa(i * 13 + b), account(b + i))))
                .with_storages([(
                    hot(),
                    HashedStorage::from_iter(
                        false,
                        (0..400).map(|s| (slot(s * 5 + b), U256::from(b * 1_000 + s))),
                    ),
                )])
        })
        .collect();
    fixture.check_output(&blocks)?;
    Ok(())
}

/// Many small contracts: one task each, one or two slots.
#[tokio::test]
async fn many_small_contracts_parallel_equals_serial() -> eyre::Result<()> {
    let fixture = Fixture::new()?;
    let blocks: Vec<_> = (0..3u64)
        .map(|b| {
            HashedPostState::default().with_storages((0..SMALL_CONTRACTS).map(|i| {
                (small(i), HashedStorage::from_iter(false, [(slot(b), U256::from(b + i + 1))]))
            }))
        })
        .collect();
    let taken = fixture.check_output(&blocks)?;
    assert!(taken > 0, "no block took a task's storage root: the tasks were never exercised");
    Ok(())
}

/// No storage changes: no targets, the serial path, no task.
#[tokio::test]
async fn no_storage_changes_runs_serially() -> eyre::Result<()> {
    let fixture = Fixture::new()?;
    let blocks: Vec<_> = (0..3u64)
        .map(|b| {
            HashedPostState::default().with_accounts(
                (0..100)
                    .map(|i| (eoa(i + b), account(b * 10 + i)))
                    .chain([(hot(), contract(b + 9))]),
            )
        })
        .collect();
    let block = blocks[0].clone();
    assert!(storage_root_targets(&block.construct_prefix_sets().freeze()).is_empty());
    assert_eq!(fixture.check_output(&blocks)?, 0, "a block without storage changes ran tasks");
    Ok(())
}

/// A commit between the walk's transaction and the tasks' (it changes a slot of every target and
/// advances the finalized block number, as a persist does): every task sees another tip and
/// leaves its account to the walk, so the root is the serial one over the walk's transaction.
/// Without the tip check any task result the walk took would carry the commit's slot.
#[tokio::test]
async fn moved_tip_falls_back_to_serial() -> eyre::Result<()> {
    let mut fixture = Fixture::new()?;
    // the hot contract and 60 small ones (two or three slots each), one slot written in each
    let targets = || std::iter::once(hot()).chain((0..60).map(|i| small(i * 5 + 1)));
    let block = HashedPostState::default().with_storages(
        targets().map(|a| (a, HashedStorage::from_iter(false, [(slot(0), U256::from(77))]))),
    );
    let factory = fixture.env.blockchain_provider();
    let walk_provider = factory.database_provider_ro()?;
    let walk_tx = walk_provider.tx_ref();
    let overlay = OutputTrieOverlay::new();
    let serial = overlay.layered_root_with_updates(walk_tx, block.clone())?;

    // the commit: another value under a slot of each target the block does not write
    let moved = HashedPostState::default().with_storages(
        targets().map(|a| (a, HashedStorage::from_iter(false, [(slot(1), U256::from(999))]))),
    );
    fixture.persist(&[moved])?;
    let factory = fixture.env.blockchain_provider();
    let after = factory.database_provider_ro()?;
    let moved_serial = overlay.layered_root_with_updates(after.tx_ref(), block.clone())?;
    assert_ne!(moved_serial.0, serial.0, "the commit must change the block's root");
    drop(after);

    let taken_before = TASK_ROOTS_TAKEN.with(|n| n.get());
    let parallel =
        overlay.layered_root_with_updates_parallel(factory, walk_tx, block, Walks::SPLIT)?;
    assert_eq!(parallel, serial, "the root must be the walk transaction's serial root");
    assert_eq!(TASK_ROOTS_TAKEN.with(|n| n.get()), taken_before, "a task's root was used");
    Ok(())
}

/// A commit between two roots of one output (it advances the finalized block number, as a persist
/// does): the overlay holds the first block, built over the database before the commit, so a later
/// root over the moved database would lay that block over a state it was not built on. The root
/// fails in every mode, before it computes anything; the first root of a fresh overlay over the
/// moved database is fine.
#[tokio::test]
async fn root_over_a_moved_anchor_fails() -> eyre::Result<()> {
    let mut fixture = Fixture::new()?;
    let first = HashedPostState::default().with_accounts((0..10).map(|i| (eoa(i), account(i + 3))));
    let second = HashedPostState::default().with_accounts((10..20).map(|i| (eoa(i), account(i))));
    let mut overlay = OutputTrieOverlay::new();
    {
        let factory = fixture.env.blockchain_provider();
        let provider = factory.database_provider_ro()?;
        let (_, updates) = overlay.block_root_with_updates(
            factory,
            provider.tx_ref(),
            first.clone(),
            ParallelRoots::Off,
            2,
        )?;
        overlay.extend_from_block(&ComputedTrieData::without_trie_input(
            Arc::new(first.clone().into_sorted()),
            Arc::new(updates.into_sorted()),
        ));
    }
    fixture.persist(&[first])?;

    let factory = fixture.env.blockchain_provider();
    let provider = factory.database_provider_ro()?;
    let modes = [ParallelRoots::Off, ParallelRoots::Sampled(Walks::SPLIT)];
    for mode in modes.into_iter().chain([ParallelRoots::Shadow(Walks::SPLIT)]) {
        let error = overlay
            .block_root_with_updates(factory, provider.tx_ref(), second.clone(), mode, 3)
            .expect_err("a root over a moved anchor must fail");
        assert!(error.to_string().contains("moved during the output"), "{mode:?}: {error}");
    }
    OutputTrieOverlay::new().block_root_with_updates(
        factory,
        provider.tx_ref(),
        second,
        ParallelRoots::Off,
        3,
    )?;
    Ok(())
}

/// A walk that fails (as on a database read error) still claims the tasks that have not started,
/// so they return at once instead of computing their storage roots before the error comes back,
/// and the error is the walk's own. Driven from the worker of a one-thread pool, nothing can start
/// a task before the walk ends, so every task is unstarted when the walk fails.
#[tokio::test]
async fn failed_walk_starts_no_task() -> eyre::Result<()> {
    let fixture = Fixture::new()?;
    let block = HashedPostState::default().with_storages(
        (0..SMALL_CONTRACTS)
            .map(|i| (small(i), HashedStorage::from_iter(false, [(slot(7), U256::from(i + 1))]))),
    );
    let targets = storage_root_targets(&block.construct_prefix_sets().freeze()).len();
    assert_eq!(targets as u64, SMALL_CONTRACTS, "one task per small contract");
    let factory = fixture.env.blockchain_provider();
    let overlay = OutputTrieOverlay::new();
    let walks = Walks::new(AccountWalk::OneThread, StorageWalk::OneTask);
    let pool = rayon::ThreadPoolBuilder::new().num_threads(1).build()?;
    let (result, started) = pool.install(|| -> eyre::Result<_> {
        let provider = factory.database_provider_ro()?;
        let before = TASKS_STARTED.with(|n| n.get());
        FAIL_NEXT_WALK.with(|f| f.set(true));
        let result =
            overlay.layered_root_with_updates_parallel(factory, provider.tx_ref(), block, walks);
        assert!(!FAIL_NEXT_WALK.with(|f| f.get()), "the walk did not run");
        Ok((result, TASKS_STARTED.with(|n| n.get()) - before))
    })?;
    let injected = ProviderError::from(DatabaseError::Other(INJECTED_WALK_FAILURE.into()));
    let error = result.expect_err("the walk was made to fail");
    assert_eq!(error.to_string(), injected.to_string(), "the walk's error must come back as is");
    assert_eq!(started, 0, "{started} of {targets} tasks ran after the walk failed");
    Ok(())
}

/// Every node checks the same blocks, one in [`CHECK_ONE_BLOCK_IN`] by number, whatever it has
/// seen since it started: two nodes in one process, one from genesis and one that joined later,
/// asked in turn as their engines would be, pick the same blocks where they overlap.
#[test]
fn every_node_checks_the_same_blocks() {
    let end = 4 * CHECK_ONE_BLOCK_IN;
    let joined = CHECK_ONE_BLOCK_IN + 77;
    let mut checked = [Vec::new(), Vec::new()];
    for number in 0..end {
        if block_is_checked(number) {
            checked[0].push(number);
        }
        if number >= joined && block_is_checked(number) {
            checked[1].push(number);
        }
    }
    let expected: Vec<_> = (0..4).map(|k| k * CHECK_ONE_BLOCK_IN).collect();
    assert_eq!(checked[0], expected, "the node from genesis");
    assert_eq!(checked[1], expected[2..], "the node that joined at block {joined}");
}

/// TN-0140's shape (upstream's `inserted_leaf_before_deeper_stored_branch_is_included`) through
/// TN's state-root path: a block inserts a slot between a storage extension's prefix and the
/// stored branch below it. The block's root, serial, on every parallel walk and in shadow mode,
/// equals a rebuild from the hashed state with no stored trie node, both when the earlier block's
/// trie nodes are persisted and when they are in the output's overlay. With the trie node
/// iterator's old skip condition (`key.starts_with(&seek_prefix)`) the inserted slot is never
/// visited and both serial roots differ from the rebuild.
#[tokio::test]
async fn inserted_slot_before_a_deeper_stored_branch_matches_a_rebuild() -> eyre::Result<()> {
    use reth_trie::{
        hashed_cursor::HashedPostStateCursorFactory, trie_cursor::noop::NoopTrieCursorFactory,
        Nibbles, StateRoot,
    };
    use reth_trie_db::DatabaseHashedCursorFactory;

    /// A hashed slot key starting with `prefix`, zeros after.
    fn slot_at(prefix: &[u8]) -> B256 {
        let mut bytes = [0; 32];
        bytes[..prefix.len()].copy_from_slice(prefix);
        B256::from(bytes)
    }
    /// The root of the database's hashed state under `blocks`, from no stored trie node.
    fn rebuilt(fixture: &Fixture, blocks: &[&HashedPostState]) -> eyre::Result<B256> {
        let provider = fixture.env.blockchain_provider().database_provider_ro()?;
        let mut state = HashedPostState::default();
        blocks.iter().for_each(|block| state.extend_ref(block));
        let state = state.into_sorted();
        let hashed = DatabaseHashedCursorFactory::new(provider.tx_ref());
        Ok(StateRoot::new(
            NoopTrieCursorFactory::default(),
            HashedPostStateCursorFactory::new(hashed, &state),
        )
        .root()?)
    }

    let address = B256::repeat_byte(0xaa);
    let block = |slots: &[B256]| {
        HashedPostState::default().with_accounts([(address, contract(1))]).with_storages([(
            address,
            HashedStorage::from_iter(false, slots.iter().map(|slot| (*slot, U256::from(1)))),
        )])
    };
    // Under nibble 1 every old slot starts 1, 0, a: an extension from [1] to the branch at
    // [1, 0, a]. The inserted slot (1, 0, 0, ..) sits between them.
    let first = block(&[
        slot_at(&[0x00]),
        slot_at(&[0x0f, 0x00]),
        slot_at(&[0x0f, 0x10]),
        slot_at(&[0x10, 0xa0, 0x00]),
        slot_at(&[0x10, 0xa0, 0x01]),
        slot_at(&[0x10, 0xa0, 0x10]),
        slot_at(&[0x10, 0xa1]),
    ]);
    let second = block(&[slot_at(&[0x10, 0x00])]);

    // The first block's root and the stored nodes it leaves: the branch at [1, 0, a] and no node
    // above it under nibble 1.
    let first_root = |fixture: &Fixture| -> eyre::Result<B256> {
        let provider = fixture.env.blockchain_provider().database_provider_ro()?;
        let (root, updates) =
            OutputTrieOverlay::new().layered_root_with_updates(provider.tx_ref(), first.clone())?;
        let nodes = &updates.storage_tries[&address].storage_nodes;
        assert!(nodes.contains_key(&Nibbles::from_nibbles([1, 0, 0xa])), "the deeper branch");
        for path in [&[1][..], &[1, 0]] {
            assert!(!nodes.contains_key(&Nibbles::from_nibbles(path)), "no node at {path:?}");
        }
        assert_eq!(root, rebuilt(fixture, &[&first])?, "the rebuild agrees on the first block");
        Ok(root)
    };

    // The first block's nodes in the output's overlay.
    let fixture = Fixture::new()?;
    first_root(&fixture)?;
    {
        let provider = fixture.env.blockchain_provider().database_provider_ro()?;
        let tx = provider.tx_ref();
        let mut overlay = OutputTrieOverlay::new();
        let (_, updates) = overlay.layered_root_with_updates(tx, first.clone())?;
        overlay.extend_from_block(&ComputedTrieData::without_trie_input(
            Arc::new(first.clone().into_sorted()),
            Arc::new(updates.into_sorted()),
        ));
        let (root, _) = overlay.layered_root_with_updates(tx, second.clone())?;
        assert_eq!(root, rebuilt(&fixture, &[&first, &second])?, "overlay: serial root");
    }
    fixture.check_output(&[first.clone(), second.clone()])?;

    // The first block's nodes persisted.
    let mut fixture = Fixture::new()?;
    let root = first_root(&fixture)?;
    fixture.persist(std::slice::from_ref(&first))?;
    assert_eq!(rebuilt(&fixture, &[])?, root, "the rebuild agrees on the persisted block");
    let provider = fixture.env.blockchain_provider().database_provider_ro()?;
    let (root, _) =
        OutputTrieOverlay::new().layered_root_with_updates(provider.tx_ref(), second.clone())?;
    drop(provider);
    assert_eq!(root, rebuilt(&fixture, &[&second])?, "persisted: serial root");
    fixture.check_output(&[second])?;
    Ok(())
}

/// A task that fails or panics leaves its account to the walk, and a task the walk claimed
/// before it started does nothing.
#[test]
fn failed_panicked_and_claimed_tasks_leave_the_root_to_the_walk() {
    let mut targets = vec![(key(9, 1), Default::default()), (key(9, 2), Default::default())];
    targets.sort_unstable_by_key(|(address, _)| *address);
    let tasks = StorageRootTasks::new(targets, StorageSplits::default());
    let [first, second] = [&tasks.slots[0], &tasks.slots[1]];

    tasks.run(first, || panic!("a task panics"));
    assert!(tasks.storage_root(first.hashed_address).is_none(), "a panicked task's root");

    assert!(tasks.storage_root(second.hashed_address).is_none(), "the walk claims a pending task");
    tasks.run(second, || unreachable!("a claimed task must not run"));

    let failed =
        StorageRootTasks::new(vec![(key(9, 3), Default::default())], StorageSplits::default());
    failed.run(&failed.slots[0], || Err(TaskError::TipMoved { walk: Some(1), task: Some(2) }));
    assert!(failed.storage_root(key(9, 3)).is_none(), "a failed task's root");
    let done =
        StorageRootTasks::new(vec![(key(9, 4), Default::default())], StorageSplits::default());
    done.run(&done.slots[0], || Ok((key(9, 5), 3, Default::default())));
    assert_eq!(done.storage_root(key(9, 4)).map(|r| r.0), Some(key(9, 5)), "a finished task");
    assert!(done.storage_root(key(9, 6)).is_none(), "an account without a task");
    assert_eq!(tasks.failures.lock().0, 1, "the panic is counted as a failure");
}

/// Every failed task counts once in `tn_reth.parallel_state_root_task_failures_total` under its
/// reason; a task that finishes or is claimed counts nothing.
#[test]
fn failed_tasks_are_counted_by_reason() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
        let mut targets: Vec<_> = (1..=6).map(|i| (key(9, i), Default::default())).collect();
        targets.sort_unstable_by_key(|(address, _)| *address);
        let tasks = StorageRootTasks::new(targets, StorageSplits::default());
        let slots = &tasks.slots;
        tasks.run(&slots[0], || panic!("a task panics"));
        tasks.run(&slots[1], || Err(TaskError::TipMoved { walk: Some(1), task: Some(2) }));
        tasks.run(&slots[2], || Err(TaskError::TipMoved { walk: Some(1), task: None }));
        tasks.run(&slots[3], || Err(DatabaseError::Other("a read fails".into()).into()));
        tasks.run(&slots[4], || Ok((key(9, 7), 1, Default::default())));
        assert!(tasks.storage_root(slots[5].hashed_address).is_none(), "the walk claims it");
        tasks.run(&slots[5], || unreachable!("a claimed task must not run"));
        assert_eq!(tasks.failures.lock().0, 4, "the block's log line counts the same failures");
    });
    let snapshot = snapshotter.snapshot().into_vec();
    let counted = |reason: &str| {
        snapshot.iter().find_map(|(k, _, _, value)| {
            (k.key().name() == "tn_reth.parallel_state_root_task_failures_total"
                && k.key().labels().any(|l| l.key() == "reason" && l.value() == reason))
            .then_some(value)
        })
    };
    for (reason, expected) in [("panicked", 1), ("tip_moved", 2), ("database", 1)] {
        let value = counted(reason);
        assert!(
            matches!(value, Some(DebugValue::Counter(n)) if *n == expected),
            "{reason}: {value:?}"
        );
    }
    assert!(counted("provider").is_none(), "no provider failure was recorded");
}

/// A parallel result, from the walk on one thread (`composed: false`) or the split.
fn parallel(result: (B256, TrieUpdates), composed: bool) -> ParallelRoot {
    ParallelRoot { result, composed, storage_composed: 0 }
}

/// A sampled comparison that finds a difference (in the root, or only in the trie updates)
/// returns the serial result and counts it in `tn_reth.parallel_state_root_mismatches_total`; a
/// difference in a root the split composed also counts as the `mismatch` account-walk fallback.
/// The counter binds to the recorder current when `RETH_METRICS` is first forced, so run it under
/// nextest (one process per test).
#[test]
fn sampled_difference_uses_the_serial_result_and_counts_it() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let serial = (key(7, 1), TrieUpdates::default());
    let mut other_updates = TrieUpdates::default();
    other_updates.removed_nodes.insert(reth_trie::Nibbles::from_nibbles([1, 2]));
    let other_root = (key(7, 2), TrieUpdates::default());
    metrics::with_local_recorder(&recorder, || {
        assert_eq!(checked(serial.clone(), parallel(serial.clone(), false), false), serial);
        assert_eq!(checked(serial.clone(), parallel(serial.clone(), true), false), serial);
        assert_eq!(checked(serial.clone(), parallel(other_root.clone(), false), false), serial);
        assert_eq!(
            checked(serial.clone(), parallel((key(7, 1), other_updates), false), false),
            serial
        );
        assert_eq!(checked(serial.clone(), parallel(other_root, true), false), serial);
    });
    let snapshot = snapshotter.snapshot().into_vec();
    let counted = |name: &str, reason: Option<&str>| {
        snapshot.iter().find_map(|(k, _, _, value)| {
            let labels_match = k.key().labels().any(|l| Some(l.value()) == reason)
                || (reason.is_none() && k.key().labels().next().is_none());
            (k.key().name() == name && labels_match).then_some(value)
        })
    };
    let mismatches = counted("tn_reth.parallel_state_root_mismatches_total", None);
    assert!(matches!(mismatches, Some(DebugValue::Counter(3))), "counted: {mismatches:?}");
    let split = counted("tn_reth.account_walk_fallbacks_total", Some("mismatch"));
    assert!(matches!(split, Some(DebugValue::Counter(1))), "split mismatches: {split:?}");
}

/// In shadow mode a difference panics.
#[test]
#[should_panic(expected = "differs from the serial root")]
fn shadow_difference_panics() {
    checked(
        (key(7, 1), TrieUpdates::default()),
        parallel((key(7, 2), TrieUpdates::default()), false),
        true,
    );
}
