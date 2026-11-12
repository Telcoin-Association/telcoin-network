//! The split account walk gives the serial root and trie updates, and every shape it does not
//! compose falls back to the walk on one thread.
//!
//! The fixture persists a base state large enough that every first nibble's node has only branch
//! children (thousands of accounts), so the database holds a real account trie, then drives
//! multi-block outputs through the output overlay, comparing each block's root with the serial
//! one over the same transaction and counting the blocks the split composed. Every test chooses
//! the walks explicitly ([`Walks`]), so they pass whatever `TN_PARALLEL_ACCOUNT_WALK` and
//! `TN_PARALLEL_STORAGE_WALK` say.

use super::{
    compose, worth_splitting, AccountWalk, Subtries, CORRUPT_NEXT_COMPOSED_ROOT, SPLITS_COMPOSED,
    SPLIT_MIN_ACCOUNTS,
};
use crate::{
    env::output_overlay::{
        layered_factories,
        parallel::{ParallelRoots, Walks, CHECK_NEXT_SAMPLED_BLOCK},
        storage_split::StorageWalk,
    },
    OutputTrieOverlay, RethChainSpec, RethEnv,
};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use reth_chain_state::ComputedTrieData;
use reth_db::{cursor::DbCursorRO as _, tables, transaction::DbTx as _};
use reth_primitives_traits::Account;
use reth_provider::{
    ChainStateBlockWriter as _, DBProvider as _, DatabaseProviderFactory as _, StateWriter as _,
    TrieWriter as _,
};
use reth_trie::{
    trie_cursor::TrieCursorFactory,
    updates::{StorageTrieUpdates, TrieUpdates},
    HashedPostState, HashedStorage, StorageRootSource, EMPTY_ROOT_HASH,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tempfile::TempDir;
use tn_types::{test_genesis, TaskManager, B256, U256};

/// The account walk on one thread, the storage split on.
const ONE_THREAD: Walks = Walks::new(AccountWalk::OneThread, StorageWalk::Split);

/// EOAs in the large base state: about 23 per second-level node, so no first nibble's node has a
/// leaf child.
pub(in crate::env::output_overlay) const EOAS: u64 = 6_000;
/// Slots of the hot contract.
const HOT_SLOTS: u64 = 1_000;
/// Small contracts, one or two slots each.
const SMALL_CONTRACTS: u64 = 200;

/// A deterministic key from a domain tag and an index.
pub(in crate::env::output_overlay) fn key(domain: u8, index: u64) -> B256 {
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
pub(in crate::env::output_overlay) fn eoa(i: u64) -> B256 {
    key(3, i)
}

/// Slot `i`'s hashed key.
pub(in crate::env::output_overlay) fn slot(i: u64) -> B256 {
    key(4, i)
}

/// The first nibble of `address`.
fn nibble_of(address: &B256) -> u8 {
    address[0] >> 4
}

/// A contract account with nonce `nonce`.
pub(in crate::env::output_overlay) fn contract(nonce: u64) -> Option<Account> {
    Some(Account { nonce, balance: U256::from(nonce), bytecode_hash: Some(key(5, 0)) })
}

/// An EOA with nonce `nonce`.
pub(in crate::env::output_overlay) fn account(nonce: u64) -> Option<Account> {
    Some(Account { nonce, balance: U256::from(nonce * 7 + 1), bytecode_hash: None })
}

/// The large base state: the hot contract, small contracts, `EOAS` EOAs.
fn large_base() -> HashedPostState {
    let mut base =
        HashedPostState::default().with_accounts([(hot(), contract(1))]).with_storages([(
            hot(),
            HashedStorage::from_iter(false, (0..HOT_SLOTS).map(|i| (slot(i), U256::from(i + 1)))),
        )]);
    for i in 0..SMALL_CONTRACTS {
        base.accounts.insert(small(i), contract(i + 2));
        base.storages.insert(
            small(i),
            HashedStorage::from_iter(false, (0..1 + i % 2).map(|s| (slot(s), U256::from(i + s)))),
        );
    }
    for i in 0..EOAS {
        base.accounts.insert(eoa(i), account(i));
    }
    base
}

/// A temp chain with a base state persisted (finalized block 1).
pub(in crate::env::output_overlay) struct Fixture {
    pub(in crate::env::output_overlay) env: RethEnv,
    _tmp: TempDir,
    _tasks: TaskManager,
    /// The finalized block number the next persist writes.
    next_tip: u64,
}

impl Fixture {
    pub(in crate::env::output_overlay) fn new(base: HashedPostState) -> eyre::Result<Self> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let tmp = TempDir::new()?;
        let tasks = TaskManager::new("account split");
        let env = RethEnv::new_for_temp_chain(chain, tmp.path(), &tasks, None)?;
        let mut fixture = Self { env, _tmp: tmp, _tasks: tasks, next_tip: 1 };
        fixture.persist(&[base])?;
        Ok(fixture)
    }

    /// Persist `blocks` as one output with their serial roots' trie updates, then advance the
    /// finalized block number, as a persist does.
    pub(in crate::env::output_overlay) fn persist(
        &mut self,
        blocks: &[HashedPostState],
    ) -> eyre::Result<()> {
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

    /// Run `blocks` as one output: each block's root serially, with the tasks unchecked and in
    /// shadow mode (the walk split), over one transaction; all three must agree. Returns the roots
    /// the split composed.
    fn check_output(&self, blocks: &[HashedPostState]) -> eyre::Result<usize> {
        let factory = self.env.blockchain_provider();
        let provider = factory.database_provider_ro()?;
        let tx = provider.tx_ref();
        let mut overlay = OutputTrieOverlay::new();
        let composed_before = SPLITS_COMPOSED.with(|n| n.get());
        for (i, block) in blocks.iter().enumerate() {
            let serial = overlay.layered_root_with_updates(tx, block.clone())?;
            let parallel = overlay.layered_root_with_updates_parallel(
                factory,
                tx,
                block.clone(),
                Walks::SPLIT,
            )?;
            assert_eq!(parallel.0, serial.0, "state root differs at block {i}");
            assert_eq!(parallel.1, serial.1, "trie updates differ at block {i}");
            let shadow = overlay.block_root_with_updates(
                factory,
                tx,
                block.clone(),
                ParallelRoots::Shadow(Walks::SPLIT),
            )?;
            assert_eq!(shadow, serial, "shadow result differs at block {i}");
            overlay.extend_from_block(&ComputedTrieData::without_trie_input(
                Arc::new(block.clone().into_sorted()),
                Arc::new(serial.1.into_sorted()),
            ));
        }
        Ok(SPLITS_COMPOSED.with(|n| n.get()) - composed_before)
    }
}

/// `f`'s result and the account-walk fallbacks this thread counted while it ran, by reason.
fn counting_fallbacks<T>(f: impl FnOnce() -> T) -> (T, HashMap<String, u64>) {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let result = metrics::with_local_recorder(&recorder, f);
    let counts = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| key.key().name() == "tn_reth.account_walk_fallbacks_total")
        .filter_map(|(key, _, _, value)| {
            let reason = key.key().labels().find(|l| l.key() == "reason")?.value().to_string();
            match value {
                DebugValue::Counter(n) => Some((reason, n)),
                _ => None,
            }
        })
        .collect();
    (result, counts)
}

/// `count` EOAs (existing ones and new ones) whose hashed address starts with `nibble`, updated.
fn eoas_under(nibble: u8, count: usize, nonce: u64) -> Vec<(B256, Option<Account>)> {
    (0..)
        .map(eoa)
        .filter(|address| nibble_of(address) == nibble)
        .take(count)
        .map(|address| (address, account(nonce)))
        .collect()
}

/// Every changed account under one nibble: that subtrie is walked, the other fifteen are re-hashed
/// from their stored nodes, and the root is the serial one.
#[tokio::test]
async fn one_nibble_changed() -> eyre::Result<()> {
    let fixture = Fixture::new(large_base())?;
    let blocks: Vec<_> = (0..3u64)
        .map(|b| HashedPostState::default().with_accounts(eoas_under(5, 100, b + 10)))
        .collect();
    assert_eq!(fixture.check_output(&blocks)?, 3 * 2, "every block's root is composed");
    Ok(())
}

/// Changes in all sixteen nibbles, with the hot contract's storage root on a task and small
/// contracts' storage changes, over a multi-block output.
#[tokio::test]
async fn all_nibbles_changed() -> eyre::Result<()> {
    let fixture = Fixture::new(large_base())?;
    let blocks: Vec<_> = (0..4u64)
        .map(|b| {
            let mut block = HashedPostState::default()
                .with_accounts((0..400).map(|i| (eoa(i * 13 + b), account(b * 100 + i))))
                .with_storages([(
                    hot(),
                    HashedStorage::from_iter(
                        false,
                        (0..200).map(|s| (slot(s * 3 + b), U256::from(b * 1_000 + s))),
                    ),
                )]);
            for i in 0..40 {
                block.storages.insert(
                    small(i * 3 + b),
                    HashedStorage::from_iter(false, [(slot(1), U256::from(b + i + 5))]),
                );
            }
            block
        })
        .collect();
    let nibbles: std::collections::HashSet<_> = blocks[0].accounts.keys().map(nibble_of).collect();
    assert_eq!(nibbles.len(), 16, "the block changes every nibble");
    assert_eq!(fixture.check_output(&blocks)?, 4 * 2, "every block's root is composed");
    Ok(())
}

/// A trie whose accounts all start with one nibble: its root is an extension over that nibble's
/// node, not a branch, so the split does not compose it and the walk runs on one thread.
#[tokio::test]
async fn single_nibble_trie_falls_back() -> eyre::Result<()> {
    let base = HashedPostState::default().with_accounts(eoas_under(9, 3_000, 1));
    let fixture = Fixture::new(base)?;
    let block = HashedPostState::default().with_accounts(eoas_under(9, 200, 2));
    let (composed, fallbacks) = counting_fallbacks(|| fixture.check_output(&[block]));
    assert_eq!(composed?, 0, "a single-nibble trie's root was composed");
    assert_eq!(fallbacks.get("shape"), Some(&2), "fallbacks: {fallbacks:?}");
    Ok(())
}

/// A small trie (a few hundred accounts): some first nibbles' nodes have leaf children, which the
/// split does not rebuild, so it falls back.
#[tokio::test]
async fn small_trie_falls_back() -> eyre::Result<()> {
    let base = HashedPostState::default().with_accounts((0..300).map(|i| (eoa(i), account(i))));
    let fixture = Fixture::new(base)?;
    let block =
        HashedPostState::default().with_accounts((0..150).map(|i| (eoa(i * 2), account(i + 9))));
    let (composed, fallbacks) = counting_fallbacks(|| fixture.check_output(&[block]));
    assert_eq!(composed?, 0, "a small trie's root was composed");
    assert_eq!(fallbacks.get("shape"), Some(&2), "fallbacks: {fallbacks:?}");
    Ok(())
}

/// Destroyed accounts with wiped storage, wiped and re-created contracts, and a whole nibble's
/// accounts destroyed (its subtrie becomes empty and the root loses that child).
#[tokio::test]
async fn wiped_storage_and_destroyed_accounts() -> eyre::Result<()> {
    let fixture = Fixture::new(large_base())?;
    let mut block = HashedPostState::default();
    for i in 0..SMALL_CONTRACTS {
        let (info, storage) = match i % 4 {
            // destroyed: account removed, storage wiped
            0 => (None, HashedStorage::from_iter(true, [])),
            // wiped and re-created with other slots
            1 => (contract(i + 50), HashedStorage::from_iter(true, [(slot(7), U256::from(i))])),
            // a slot deleted
            2 => (contract(i + 60), HashedStorage::from_iter(false, [(slot(0), U256::ZERO)])),
            _ => continue,
        };
        block.accounts.insert(small(i), info);
        block.storages.insert(small(i), storage);
    }
    // the hot contract destroyed and re-created with a few slots
    block.accounts.insert(hot(), contract(99));
    block.storages.insert(
        hot(),
        HashedStorage::from_iter(true, (0..5).map(|s| (slot(s), U256::from(s + 1)))),
    );
    // every account under one nibble (the genesis allocations' too) destroyed
    let emptied = 0xa;
    let destroyed = {
        let provider = fixture.env.blockchain_provider().database_provider_ro()?;
        let mut cursor = provider.tx_ref().cursor_read::<tables::HashedAccounts>()?;
        let mut start = B256::ZERO;
        start[0] = emptied << 4;
        cursor
            .walk(Some(start))?
            .map_while(|entry| entry.ok().filter(|(address, _)| nibble_of(address) == emptied))
            .map(|(address, _)| address)
            .collect::<Vec<_>>()
    };
    assert!(destroyed.len() > 300, "the nibble holds {} accounts", destroyed.len());
    for address in &destroyed {
        block.accounts.insert(*address, None);
        block.storages.insert(*address, HashedStorage::from_iter(true, []));
    }
    let next = HashedPostState::default().with_accounts(
        (0..300).map(eoa).filter(|a| nibble_of(a) != emptied).map(|a| (a, account(3))),
    );
    assert_eq!(fixture.check_output(&[block, next])?, 2 * 2, "both roots are composed");
    Ok(())
}

/// Few changes stay on one thread (no subtrie tasks); many changes split.
#[tokio::test]
async fn few_and_many_changes() -> eyre::Result<()> {
    let fixture = Fixture::new(large_base())?;
    let few = HashedPostState::default()
        .with_accounts((0..SPLIT_MIN_ACCOUNTS as u64 - 1).map(|i| (eoa(i), account(i + 1))));
    assert!(!worth_splitting(&few.construct_prefix_sets().freeze()));
    let (composed, fallbacks) = counting_fallbacks(|| fixture.check_output(&[few]));
    assert_eq!(composed?, 0, "a block with few changes was split");
    assert_eq!(fallbacks.get("few_changes"), Some(&2), "fallbacks: {fallbacks:?}");
    let many =
        HashedPostState::default().with_accounts((0..EOAS + 500).map(|i| (eoa(i), account(i + 2))));
    let (composed, fallbacks) = counting_fallbacks(|| fixture.check_output(&[many]));
    assert_eq!(composed?, 2, "a block with many changes was not split");
    assert!(fallbacks.values().all(|n| *n == 0), "fallbacks: {fallbacks:?}");
    Ok(())
}

/// Random multi-block outputs over the large base, persisted between outputs so later outputs read
/// their tries from the database.
#[tokio::test]
async fn random_outputs() -> eyre::Result<()> {
    let mut fixture = Fixture::new(large_base())?;
    let mut composed = 0;
    for seed in 0..3u64 {
        let mut rng = StdRng::seed_from_u64(0x5b17_0000 + seed);
        let blocks: Vec<_> = (0..6)
            .map(|b| {
                let block = seed * 100 + b;
                let mut state = HashedPostState::default();
                for _ in 0..rng.random_range(40..300) {
                    let i = rng.random_range(0..EOAS + 300);
                    let info = if rng.random_ratio(1, 10) { None } else { account(block * 7 + i) };
                    state.accounts.insert(eoa(i), info);
                }
                if rng.random_ratio(3, 4) {
                    let slots = (0..rng.random_range(1..200)).map(|_| {
                        let s = rng.random_range(0..HOT_SLOTS + 100);
                        (slot(s), U256::from(rng.random_range(0..4u64)))
                    });
                    state.storages.insert(hot(), HashedStorage::from_iter(false, slots));
                }
                state
            })
            .collect();
        composed += fixture.check_output(&blocks)?;
        fixture.persist(&blocks)?;
    }
    assert!(composed > 0, "no root was composed: the split was never exercised");
    Ok(())
}

/// Fewer than two non-empty subtries: no branch root to compose.
#[test]
fn compose_needs_two_children() {
    let empty = || (EMPTY_ROOT_HASH, TrieUpdates::default());
    assert!(compose((0..16).map(|_| empty()).collect()).is_none());
    // a non-empty subtrie without its node at [n]: a leaf or an extension child
    let mut subtries: Vec<_> = (0..16).map(|_| empty()).collect();
    subtries[3] = (key(8, 1), TrieUpdates::default());
    subtries[4] = (key(8, 2), TrieUpdates::default());
    assert!(compose(subtries).is_none());
}

/// The environment value selects the walk: unset or `1` splits, `0` and anything else run it on
/// one thread.
#[test]
fn env_value_selects_the_walk() {
    assert_eq!(AccountWalk::from_env_value(None), AccountWalk::Split);
    assert_eq!(AccountWalk::from_env_value(Some("1")), AccountWalk::Split);
    assert_eq!(AccountWalk::from_env_value(Some("0")), AccountWalk::OneThread);
    assert_eq!(AccountWalk::from_env_value(Some("yes")), AccountWalk::OneThread);
    assert_eq!(AccountWalk::from_env_value(Some("")), AccountWalk::OneThread);
}

/// A block the split would take, with the walk on one thread: the serial root, nothing composed
/// and no fallback counted (the off switch is not a fallback).
#[tokio::test]
async fn one_thread_walk_does_not_split() -> eyre::Result<()> {
    let fixture = Fixture::new(large_base())?;
    let block = HashedPostState::default().with_accounts((0..400).map(|i| (eoa(i), account(i))));
    let factory = fixture.env.blockchain_provider();
    let provider = factory.database_provider_ro()?;
    let tx = provider.tx_ref();
    let overlay = OutputTrieOverlay::new();
    let serial = overlay.layered_root_with_updates(tx, block.clone())?;
    let composed_before = SPLITS_COMPOSED.with(|n| n.get());
    let (results, fallbacks) = counting_fallbacks(|| -> eyre::Result<_> {
        Ok([
            overlay.layered_root_with_updates_parallel(factory, tx, block.clone(), ONE_THREAD)?,
            overlay.block_root_with_updates(
                factory,
                tx,
                block.clone(),
                ParallelRoots::Shadow(ONE_THREAD),
            )?,
        ])
    });
    for result in results? {
        assert_eq!(result, serial);
    }
    assert_eq!(SPLITS_COMPOSED.with(|n| n.get()), composed_before, "a one-thread walk composed");
    assert!(fallbacks.values().all(|n| *n == 0), "fallbacks: {fallbacks:?}");
    Ok(())
}

/// No storage-root tasks: the subtrie walks compute every storage root themselves.
#[derive(Debug)]
struct NoTasks;

impl StorageRootSource for NoTasks {
    fn storage_root(&self, _: B256) -> Option<(B256, usize, StorageTrieUpdates)> {
        None
    }
}

/// An account-trie cursor factory that fails (or panics) on every account-trie cursor.
#[derive(Debug)]
struct Broken<F> {
    inner: F,
    panics: bool,
}

impl<F: TrieCursorFactory> TrieCursorFactory for Broken<F> {
    type AccountTrieCursor<'a>
        = F::AccountTrieCursor<'a>
    where
        Self: 'a;
    type StorageTrieCursor<'a>
        = F::StorageTrieCursor<'a>
    where
        Self: 'a;

    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor<'_>, reth_db::DatabaseError> {
        assert!(!self.panics, "a subtrie task panics");
        Err(reth_db::DatabaseError::Other("a subtrie task fails".into()))
    }

    fn storage_trie_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageTrieCursor<'_>, reth_db::DatabaseError> {
        self.inner.storage_trie_cursor(hashed_address)
    }
}

/// Subtrie tasks that fail or panic (a helper's database error, a panic) leave their nibbles to
/// the calling thread, which recomputes them and still composes the serial root.
#[tokio::test]
async fn failed_and_panicked_subtries_are_recomputed() -> eyre::Result<()> {
    let fixture = Fixture::new(large_base())?;
    let block = HashedPostState::default()
        .with_accounts((0..400).map(|i| (eoa(i * 7), account(i + 3))))
        .with_storages([(
            hot(),
            HashedStorage::from_iter(false, (0..50).map(|s| (slot(s), U256::from(s + 9)))),
        )]);
    let factory = fixture.env.blockchain_provider();
    let provider = factory.database_provider_ro()?;
    let tx = provider.tx_ref();
    let overlay = OutputTrieOverlay::new();
    let serial = overlay.layered_root_with_updates(tx, block.clone())?;
    let prefix_sets = block.construct_prefix_sets().freeze();
    let current = block.into_sorted();
    let (trie, hashed) = layered_factories(tx, &overlay.runs, &current);
    let source: Arc<dyn StorageRootSource> = Arc::new(NoTasks);
    for panics in [false, true] {
        let subtries = Subtries::new(&prefix_sets);
        let failed = AtomicUsize::new(0);
        let broken = Broken { inner: &trie, panics };
        subtries.work(&broken, &hashed, &source, &|_| {
            failed.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(failed.load(Ordering::Relaxed), 16, "every nibble failed (panics: {panics})");
        let composed = subtries.finish(&trie, &hashed, &source)?;
        assert_eq!(composed, Some(serial.clone()), "recomputed root (panics: {panics})");
    }
    Ok(())
}

/// A commit between the walk's transaction and the helpers' (accounts under every nibble change
/// and the finalized block number advances, as a persist does): every helper sees another tip and
/// claims nothing, so the calling thread walks all sixteen subtries over its own transaction and
/// the root is that transaction's serial root.
#[tokio::test]
async fn moved_tip_leaves_the_subtries_to_the_walk() -> eyre::Result<()> {
    let mut fixture = Fixture::new(large_base())?;
    let block = HashedPostState::default().with_accounts((0..400).map(|i| (eoa(i), account(i))));
    let factory = fixture.env.blockchain_provider();
    let walk_provider = factory.database_provider_ro()?;
    let walk_tx = walk_provider.tx_ref();
    let overlay = OutputTrieOverlay::new();
    let serial = overlay.layered_root_with_updates(walk_tx, block.clone())?;

    let moved = HashedPostState::default()
        .with_accounts((1_000..1_400).map(|i| (eoa(i), account(i * 3 + 1))));
    fixture.persist(&[moved])?;
    let factory = fixture.env.blockchain_provider();
    let after = factory.database_provider_ro()?;
    let moved_serial = overlay.layered_root_with_updates(after.tx_ref(), block.clone())?;
    assert_ne!(moved_serial.0, serial.0, "the commit must change the block's root");
    drop(after);

    let composed_before = SPLITS_COMPOSED.with(|n| n.get());
    let parallel =
        overlay.layered_root_with_updates_parallel(factory, walk_tx, block, Walks::SPLIT)?;
    assert_eq!(parallel, serial, "the root must be the walk transaction's serial root");
    assert_eq!(SPLITS_COMPOSED.with(|n| n.get()), composed_before + 1, "the split composed");
    Ok(())
}

/// A wrong root from the split (made wrong after its own checks) is used unchecked, but a
/// sampled block's serial check replaces it with the serial result and counts the `mismatch`
/// fallback: the production check covers the split.
#[tokio::test]
async fn sampled_check_replaces_a_wrong_split_root() -> eyre::Result<()> {
    let fixture = Fixture::new(large_base())?;
    let block = HashedPostState::default().with_accounts((0..400).map(|i| (eoa(i), account(i))));
    let factory = fixture.env.blockchain_provider();
    let provider = factory.database_provider_ro()?;
    let tx = provider.tx_ref();
    let overlay = OutputTrieOverlay::new();
    let serial = overlay.layered_root_with_updates(tx, block.clone())?;

    CORRUPT_NEXT_COMPOSED_ROOT.with(|c| c.set(true));
    let wrong =
        overlay.layered_root_with_updates_parallel(factory, tx, block.clone(), Walks::SPLIT)?;
    assert_ne!(wrong.0, serial.0, "the corrupted split root must reach the result unchecked");

    CORRUPT_NEXT_COMPOSED_ROOT.with(|c| c.set(true));
    CHECK_NEXT_SAMPLED_BLOCK.with(|c| c.set(true));
    let (checked, fallbacks) = counting_fallbacks(|| {
        overlay.block_root_with_updates(
            factory,
            tx,
            block.clone(),
            ParallelRoots::Sampled(Walks::SPLIT),
        )
    });
    assert_eq!(checked?, serial, "a sampled block must use the serial result");
    assert_eq!(fallbacks.get("mismatch"), Some(&1), "fallbacks: {fallbacks:?}");
    assert!(!CORRUPT_NEXT_COMPOSED_ROOT.with(|c| c.get()), "the split did not compose");
    Ok(())
}

/// In shadow mode a wrong root from the split panics.
#[tokio::test]
#[should_panic(expected = "differs from the serial root")]
async fn shadow_panics_on_a_wrong_split_root() {
    let fixture = Fixture::new(large_base()).unwrap();
    let block = HashedPostState::default().with_accounts((0..400).map(|i| (eoa(i), account(i))));
    let factory = fixture.env.blockchain_provider();
    let provider = factory.database_provider_ro().unwrap();
    let overlay = OutputTrieOverlay::new();
    CORRUPT_NEXT_COMPOSED_ROOT.with(|c| c.set(true));
    let _ = overlay.block_root_with_updates(
        factory,
        provider.tx_ref(),
        block,
        ParallelRoots::Shadow(Walks::SPLIT),
    );
}
