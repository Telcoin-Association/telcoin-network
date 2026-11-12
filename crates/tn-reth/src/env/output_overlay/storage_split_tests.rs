//! The split storage roots of hot contracts give the serial root and trie updates, and every shape
//! the split does not compose leaves the storage root to one task or to the walk.
//!
//! The fixture persists two hot contracts with tries big enough that every first nibble's node
//! has only branch children (thousands of slots each), a medium contract whose trie fails the
//! pre-check, and enough EOAs for the account split, then drives outputs through the output
//! overlay, comparing each block's root with the serial one over the same transaction (with the
//! account walk split and on one thread, and in shadow mode) and counting the storage roots the
//! split composed. Every test chooses the walks explicitly ([`Walks`]).

use super::{
    compose, splittable, Splittable, StorageSplits, StorageWalk, CORRUPT_NEXT_STORAGE_SPLIT,
    SPLIT_MIN_SLOTS,
};
use crate::{
    env::output_overlay::{
        account_split::{
            tests::{account, contract, eoa, key, slot, Fixture, EOAS},
            AccountWalk,
        },
        layered_factories,
        parallel::{
            ParallelRoots, Walks, CHECK_NEXT_SAMPLED_BLOCK, CHECK_ONE_BLOCK_IN,
            STORAGE_SPLITS_COMPOSED,
        },
    },
    OutputTrieOverlay,
};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use rand::{rngs::StdRng, Rng as _, SeedableRng as _};
use reth_chain_state::ComputedTrieData;
use reth_db::DatabaseError;
use reth_provider::DatabaseProviderFactory as _;
use reth_trie::{
    trie_cursor::{TrieCursor, TrieCursorFactory, TrieStorageCursor},
    updates::StorageTrieUpdates,
    BranchNodeCompact, HashedPostState, HashedStorage, Nibbles, StorageRoot, TrieMask,
    EMPTY_ROOT_HASH,
};
use reth_trie_db::DatabaseStorageRoot as _;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tn_types::{B256, U256};

/// Slots of each hot contract: about 30 per second-level node, so no first nibble's node has a
/// leaf or extension child.
const HOT_SLOTS: u64 = 8_000;
/// Slots of the medium contract: some first nibbles' nodes have leaf children.
const MEDIUM_SLOTS: u64 = 1_000;

/// The walks the tests compare with serial: both split, and the storage split with the account
/// walk on one thread.
const SPLIT_WALKS: [Walks; 2] =
    [Walks::SPLIT, Walks::new(AccountWalk::OneThread, StorageWalk::Split)];

/// Hot contract `i`'s hashed address (`i` < 2).
fn hot(i: u64) -> B256 {
    key(11, i)
}

/// The medium contract's hashed address.
fn medium() -> B256 {
    key(12, 0)
}

/// The base state: two hot contracts, the medium one, `EOAS` EOAs.
fn base() -> HashedPostState {
    let mut base = HashedPostState::default();
    for (address, slots) in [(hot(0), HOT_SLOTS), (hot(1), HOT_SLOTS), (medium(), MEDIUM_SLOTS)] {
        base.accounts.insert(address, contract(slots));
        base.storages.insert(
            address,
            HashedStorage::from_iter(false, (0..slots).map(|s| (slot(s), U256::from(s + 1)))),
        );
    }
    for i in 0..EOAS {
        base.accounts.insert(eoa(i), account(i));
    }
    base
}

/// `count` slots of a contract changed to values derived from `block`.
fn changed(count: u64, step: u64, block: u64) -> HashedStorage {
    HashedStorage::from_iter(
        false,
        (0..count).map(|s| (slot(s * step + block), U256::from(block * 10_000 + s + 1))),
    )
}

/// A block changing `slots` slots of both hot contracts and 200 EOAs.
fn hot_block(slots: u64, block: u64) -> HashedPostState {
    HashedPostState::default()
        .with_accounts((0..200).map(|i| (eoa(i * 11 + block), account(block * 100 + i))))
        .with_storages([(hot(0), changed(slots, 7, block)), (hot(1), changed(slots, 5, block))])
}

/// Run `blocks` as one output over `fixture`: each block's root serially, with the tasks unchecked
/// under each of `SPLIT_WALKS`, and in shadow mode, over one transaction; all must agree. Returns
/// the storage roots the split composed.
fn check_output(fixture: &Fixture, blocks: &[HashedPostState]) -> eyre::Result<usize> {
    let factory = fixture.env.blockchain_provider();
    let provider = factory.database_provider_ro()?;
    let tx = provider.tx_ref();
    let mut overlay = OutputTrieOverlay::new();
    let composed_before = STORAGE_SPLITS_COMPOSED.with(|n| n.get());
    for (i, block) in blocks.iter().enumerate() {
        let serial = overlay.layered_root_with_updates(tx, block.clone())?;
        for walks in SPLIT_WALKS {
            let parallel =
                overlay.layered_root_with_updates_parallel(factory, tx, block.clone(), walks)?;
            assert_eq!(parallel.0, serial.0, "state root differs at block {i} ({walks:?})");
            assert_eq!(parallel.1, serial.1, "trie updates differ at block {i} ({walks:?})");
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
    Ok(STORAGE_SPLITS_COMPOSED.with(|n| n.get()) - composed_before)
}

/// `f`'s result and the storage-walk fallbacks this thread counted while it ran, by reason.
fn counting_fallbacks<T>(f: impl FnOnce() -> T) -> (T, HashMap<String, u64>) {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let result = metrics::with_local_recorder(&recorder, f);
    let counts = snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, ..)| key.key().name() == "tn_reth.storage_walk_fallbacks_total")
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

/// Both hot contracts' tries split over a multi-block output: every storage root composed, the
/// block's root and trie updates the serial ones.
#[tokio::test]
async fn hot_tries_split() -> eyre::Result<()> {
    let fixture = Fixture::new(base())?;
    let blocks: Vec<_> = (0..3).map(|b| hot_block(300, b)).collect();
    let (composed, fallbacks) = counting_fallbacks(|| check_output(&fixture, &blocks));
    // per block: two tries, under two unchecked walks and the shadow run
    assert_eq!(composed?, 3 * 2 * 3, "every hot storage root is composed");
    assert!(fallbacks.values().all(|n| *n == 0), "fallbacks: {fallbacks:?}");
    Ok(())
}

/// A trie that fails the pre-check (the medium contract) and a hot trie with too few changed
/// slots both stay one task; the first counts `small_trie`, the second nothing.
#[tokio::test]
async fn small_tries_and_few_slots_stay_one_task() -> eyre::Result<()> {
    let fixture = Fixture::new(base())?;
    let block = HashedPostState::default()
        .with_accounts((0..200).map(|i| (eoa(i), account(i + 5))))
        .with_storages([
            (medium(), changed(200, 3, 0)),
            (hot(0), changed(SPLIT_MIN_SLOTS as u64 - 1, 7, 0)),
        ]);
    {
        let provider = fixture.env.blockchain_provider().database_provider_ro()?;
        let overlay = OutputTrieOverlay::new();
        let current = block.clone().into_sorted();
        let (trie, _) = layered_factories(provider.tx_ref(), &overlay.runs, &current);
        assert_eq!(splittable(&trie, medium())?, Splittable::SmallTrie);
        assert_eq!(splittable(&trie, hot(0))?, Splittable::Yes);
    }
    let (composed, fallbacks) = counting_fallbacks(|| check_output(&fixture, &[block]));
    assert_eq!(composed?, 0, "a small trie or a hot trie with few changes was split");
    // the medium trie, under two unchecked walks and the shadow run
    assert_eq!(fallbacks.get("small_trie"), Some(&3), "fallbacks: {fallbacks:?}");
    assert_eq!(fallbacks.get("shape"), None, "fallbacks: {fallbacks:?}");
    Ok(())
}

/// Every slot of one first nibble of a hot trie deleted: that piece is walked (its removed nodes
/// recorded), its root is empty and the composed root loses that child. Then a block re-creates
/// slots under it: that trie's stored node for the nibble is gone, so the pre-check keeps it on one
/// task.
#[tokio::test]
async fn whole_nibble_emptied() -> eyre::Result<()> {
    let fixture = Fixture::new(base())?;
    let emptied = 0x7;
    let mut block = hot_block(300, 1);
    block.storages.insert(
        hot(0),
        HashedStorage::from_iter(
            false,
            (0..HOT_SLOTS).map(|s| {
                let key = slot(s);
                let value = if key[0] >> 4 == emptied { U256::ZERO } else { U256::from(s + 2) };
                (key, value)
            }),
        ),
    );
    let refill = hot_block(300, 2);
    let (composed, fallbacks) = counting_fallbacks(|| check_output(&fixture, &[block, refill]));
    // both tries in the first block, the other one in the second; two walks and the shadow run
    assert_eq!(composed?, 3 * 3);
    assert_eq!(fallbacks.get("small_trie"), Some(&3), "fallbacks: {fallbacks:?}");
    Ok(())
}

/// A hot trie emptied (every slot deleted, the account kept): the pieces see an empty storage, so
/// the split composes nothing and the walk computes the (empty) storage root; one left with slots
/// under a single nibble has no branch root to compose. Both count `shape`.
#[tokio::test]
async fn emptied_and_single_nibble_storage_fall_back() -> eyre::Result<()> {
    let fixture = Fixture::new(base())?;
    let keep = 0x3;
    let block = HashedPostState::default()
        .with_accounts((0..200).map(|i| (eoa(i), account(i + 5))))
        .with_storages([
            (
                hot(0),
                HashedStorage::from_iter(false, (0..HOT_SLOTS).map(|s| (slot(s), U256::ZERO))),
            ),
            (
                hot(1),
                HashedStorage::from_iter(
                    false,
                    (0..HOT_SLOTS)
                        .filter(|s| slot(*s)[0] >> 4 != keep)
                        .map(|s| (slot(s), U256::ZERO)),
                ),
            ),
        ]);
    let (composed, fallbacks) = counting_fallbacks(|| check_output(&fixture, &[block]));
    assert_eq!(composed?, 0, "an emptied or single-nibble storage root was composed");
    // two tries, under two unchecked walks and the shadow run
    assert_eq!(fallbacks.get("shape"), Some(&(2 * 3)), "fallbacks: {fallbacks:?}");
    Ok(())
}

/// Wiped storage (a hot contract destroyed and re-created, the other destroyed) never splits: a
/// destroyed account gets no storage-root task.
#[tokio::test]
async fn wiped_storage_stays_serial() -> eyre::Result<()> {
    let fixture = Fixture::new(base())?;
    let block = HashedPostState::default()
        .with_accounts([(hot(0), contract(99)), (hot(1), None)])
        .with_accounts((0..200).map(|i| (eoa(i), account(i + 5))))
        .with_storages([
            (
                hot(0),
                HashedStorage::from_iter(true, (0..300).map(|s| (slot(s), U256::from(s + 1)))),
            ),
            (hot(1), HashedStorage::from_iter(true, [])),
        ]);
    let next = HashedPostState::default()
        .with_accounts((0..200).map(|i| (eoa(i), account(i + 6))))
        .with_storages([(hot(0), changed(300, 7, 4))]);
    let (composed, fallbacks) = counting_fallbacks(|| check_output(&fixture, &[block, next]));
    assert_eq!(composed?, 0, "a wiped storage was split");
    // the second block: hot(0)'s re-created trie is small (two unchecked walks and the shadow run)
    assert_eq!(fallbacks.get("small_trie"), Some(&3), "fallbacks: {fallbacks:?}");
    assert_eq!(fallbacks.get("shape"), None, "fallbacks: {fallbacks:?}");
    Ok(())
}

/// Random multi-block outputs over the hot tries (changes and deletions), persisted between
/// outputs so later outputs read their tries from the database.
#[tokio::test]
async fn random_outputs() -> eyre::Result<()> {
    let mut fixture = Fixture::new(base())?;
    let mut composed = 0;
    for seed in 0..3u64 {
        let mut rng = StdRng::seed_from_u64(0x5707_0000 + seed);
        let blocks: Vec<_> = (0..5)
            .map(|b| {
                let block = seed * 100 + b;
                let mut state = HashedPostState::default().with_accounts(
                    (0..rng.random_range(10..300))
                        .map(|_| rng.random_range(0..EOAS))
                        .map(|i| (eoa(i), account(block * 7 + i))),
                );
                for contract in [hot(0), hot(1), medium()] {
                    if rng.random_ratio(1, 5) {
                        continue;
                    }
                    let slots = (0..rng.random_range(1..600)).map(|_| {
                        let s = rng.random_range(0..HOT_SLOTS + 500);
                        (slot(s), U256::from(rng.random_range(0..3u64)))
                    });
                    state.storages.insert(contract, HashedStorage::from_iter(false, slots));
                }
                state
            })
            .collect();
        composed += check_output(&fixture, &blocks)?;
        fixture.persist(&blocks)?;
    }
    assert!(composed > 0, "no storage root was composed: the split was never exercised");
    Ok(())
}

/// With the storage walk on one task: the serial root, nothing composed, no fallback counted (the
/// off switch is not a fallback).
#[tokio::test]
async fn one_task_does_not_split() -> eyre::Result<()> {
    let fixture = Fixture::new(base())?;
    let block = hot_block(300, 0);
    let factory = fixture.env.blockchain_provider();
    let provider = factory.database_provider_ro()?;
    let tx = provider.tx_ref();
    let overlay = OutputTrieOverlay::new();
    let serial = overlay.layered_root_with_updates(tx, block.clone())?;
    let composed_before = STORAGE_SPLITS_COMPOSED.with(|n| n.get());
    let walks = Walks::new(AccountWalk::Split, StorageWalk::OneTask);
    let (results, fallbacks) = counting_fallbacks(|| -> eyre::Result<_> {
        Ok([
            overlay.layered_root_with_updates_parallel(factory, tx, block.clone(), walks)?,
            overlay.block_root_with_updates(
                factory,
                tx,
                block.clone(),
                ParallelRoots::Shadow(walks),
                1,
            )?,
        ])
    });
    for result in results? {
        assert_eq!(result, serial);
    }
    assert_eq!(STORAGE_SPLITS_COMPOSED.with(|n| n.get()), composed_before, "a one-task walk split");
    assert!(fallbacks.values().all(|n| *n == 0), "fallbacks: {fallbacks:?}");
    Ok(())
}

/// The environment value selects the walk: unset or `1` splits, `0` and anything else keep one
/// task.
#[test]
fn env_value_selects_the_walk() {
    assert_eq!(StorageWalk::from_env_value(None), StorageWalk::Split);
    assert_eq!(StorageWalk::from_env_value(Some("1")), StorageWalk::Split);
    assert_eq!(StorageWalk::from_env_value(Some("0")), StorageWalk::OneTask);
    assert_eq!(StorageWalk::from_env_value(Some("yes")), StorageWalk::OneTask);
    assert_eq!(StorageWalk::from_env_value(Some("")), StorageWalk::OneTask);
}

/// Fewer than two non-empty pieces, a piece without its node at `[n]`, or a deleted piece: no
/// root to compose.
#[test]
fn compose_needs_two_branch_children() {
    let empty = || (EMPTY_ROOT_HASH, 0, StorageTrieUpdates::default());
    assert!(compose((0..16).map(|_| empty()).collect()).is_none());
    let mut pieces: Vec<_> = (0..16).map(|_| empty()).collect();
    pieces[3] = (key(8, 1), 1, StorageTrieUpdates::default());
    pieces[4] = (key(8, 2), 1, StorageTrieUpdates::default());
    assert!(compose(pieces).is_none(), "leaf or extension children were composed");
    let mut pieces: Vec<_> = (0..16).map(|_| empty()).collect();
    pieces[0] = (EMPTY_ROOT_HASH, 0, StorageTrieUpdates::deleted());
    assert!(compose(pieces).is_none(), "an empty storage was composed");
}

/// A storage-trie cursor factory that fails (or panics) on every storage-trie cursor.
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

    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor<'_>, DatabaseError> {
        self.inner.account_trie_cursor()
    }

    fn storage_trie_cursor(&self, _: B256) -> Result<Self::StorageTrieCursor<'_>, DatabaseError> {
        assert!(!self.panics, "a storage piece panics");
        Err(DatabaseError::Other("a storage piece fails".into()))
    }
}

/// The pieces of a hot trie compose its serial storage root and trie updates; failed or panicked
/// pieces, or pieces nobody claimed, leave it to the walk (`take` gives `Some(None)` without
/// waiting); a trie that is not split is not the splits' (`None`).
#[tokio::test]
async fn pieces_compose_the_storage_root_or_leave_it_to_the_walk() -> eyre::Result<()> {
    let fixture = Fixture::new(base())?;
    let block = hot_block(300, 0);
    let factory = fixture.env.blockchain_provider();
    let provider = factory.database_provider_ro()?;
    let tx = provider.tx_ref();
    let overlay = OutputTrieOverlay::new();
    let prefix_sets = block.construct_prefix_sets().freeze();
    let current = block.into_sorted();
    let (trie, hashed) = layered_factories(tx, &overlay.runs, &current);
    let target = |address: B256| (address, prefix_sets.storage_prefix_sets[&address].clone());
    let serial = StorageRoot::from_tx_hashed(tx, hot(1))
        .with_trie_cursor_factory(&trie)
        .with_hashed_cursor_factory(&hashed)
        .with_prefix_set(target(hot(1)).1)
        .root_with_updates()?;

    let splits = StorageSplits::new(vec![target(hot(1)), target(hot(0))]);
    splits.work(tx, &trie, &hashed, &|error| panic!("a piece failed: {error}"));
    assert!(splits.all_claimed());
    assert_eq!(splits.take(eoa(1)), None, "an account that is not split");
    assert_eq!(splits.take(hot(1)), Some(Some(serial)), "the composed storage root");
    assert_eq!(splits.take(hot(1)), Some(None), "a storage root is taken once");
    assert_eq!(splits.composed(), 1);

    for panics in [false, true] {
        let splits = StorageSplits::new(vec![target(hot(1))]);
        let failed = AtomicUsize::new(0);
        let broken = Broken { inner: &trie, panics };
        splits.work(tx, &broken, &hashed, &|_| {
            failed.fetch_add(1, Ordering::Relaxed);
        });
        assert_eq!(failed.load(Ordering::Relaxed), 16, "every piece failed (panics: {panics})");
        assert_eq!(splits.take(hot(1)), Some(None), "failed pieces (panics: {panics})");
    }

    // Nothing claimed: take claims and abandons the pieces up to its trie's end, never waiting.
    let splits = StorageSplits::new(vec![target(hot(0)), target(hot(1))]);
    assert_eq!(splits.take(hot(1)), Some(None), "unclaimed pieces");
    assert_eq!(splits.take(hot(0)), Some(None), "pieces abandoned by another trie's take");
    assert!(splits.all_claimed());
    splits.work(tx, &trie, &hashed, &|error| panic!("a piece ran: {error}"));
    Ok(())
}

/// The pieces' storage root equals the whole walk's for a prefix set with keys under one nibble
/// only (fifteen pieces walk no change) and for an empty piece prefix set in every nibble but one.
#[tokio::test]
async fn pieces_with_unchanged_nibbles() -> eyre::Result<()> {
    let fixture = Fixture::new(base())?;
    let changed_nibble = 0xb;
    let block = HashedPostState::default().with_storages([(
        hot(0),
        HashedStorage::from_iter(
            false,
            (0..HOT_SLOTS)
                .map(slot)
                .filter(|key| key[0] >> 4 == changed_nibble)
                .take(100)
                .map(|key| (key, U256::from(77))),
        ),
    )]);
    let factory = fixture.env.blockchain_provider();
    let provider = factory.database_provider_ro()?;
    let tx = provider.tx_ref();
    let overlay = OutputTrieOverlay::new();
    let prefix_sets = block.construct_prefix_sets().freeze();
    let prefix_set = prefix_sets.storage_prefix_sets[&hot(0)].clone();
    assert!(prefix_set.iter().all(|key| key.first() == Some(changed_nibble)));
    let current = block.clone().into_sorted();
    let (trie, hashed) = layered_factories(tx, &overlay.runs, &current);
    let serial = StorageRoot::from_tx_hashed(tx, hot(0))
        .with_trie_cursor_factory(&trie)
        .with_hashed_cursor_factory(&hashed)
        .with_prefix_set(prefix_set.clone())
        .root_with_updates()?;
    let splits = StorageSplits::new(vec![(hot(0), prefix_set)]);
    splits.work(tx, &trie, &hashed, &|error| panic!("a piece failed: {error}"));
    assert_eq!(splits.take(hot(0)), Some(Some(serial)));
    // and through the block's root
    assert_eq!(check_output(&fixture, &[block])?, 3);
    Ok(())
}

/// A commit between the walk's transaction and the helpers' (the finalized block number
/// advances, as a persist does): every helper sees another tip and claims nothing, so the calling
/// thread computes every piece over its own transaction and the root is that transaction's serial
/// root, the hot storage roots still composed.
#[tokio::test]
async fn moved_tip_leaves_the_pieces_to_the_walk() -> eyre::Result<()> {
    let mut fixture = Fixture::new(base())?;
    let block = hot_block(300, 0);
    let factory = fixture.env.blockchain_provider();
    let walk_provider = factory.database_provider_ro()?;
    let walk_tx = walk_provider.tx_ref();
    let overlay = OutputTrieOverlay::new();
    let serial = overlay.layered_root_with_updates(walk_tx, block.clone())?;

    fixture.persist(&[hot_block(400, 9)])?;
    let factory = fixture.env.blockchain_provider();
    let after = factory.database_provider_ro()?;
    let moved_serial = overlay.layered_root_with_updates(after.tx_ref(), block.clone())?;
    assert_ne!(moved_serial.0, serial.0, "the commit must change the block's root");
    drop(after);

    let composed_before = STORAGE_SPLITS_COMPOSED.with(|n| n.get());
    let parallel =
        overlay.layered_root_with_updates_parallel(factory, walk_tx, block, Walks::SPLIT)?;
    assert_eq!(parallel, serial, "the root must be the walk transaction's serial root");
    assert_eq!(STORAGE_SPLITS_COMPOSED.with(|n| n.get()), composed_before + 2, "both composed");
    Ok(())
}

/// A wrong composed storage root (made wrong after its own checks) is used unchecked, but a
/// sampled block's serial check (by its number, or forced by the test hook) replaces it with the
/// serial result and counts the `mismatch` fallback: the production check covers the storage
/// split.
#[tokio::test]
async fn sampled_check_replaces_a_wrong_storage_root() -> eyre::Result<()> {
    let fixture = Fixture::new(base())?;
    let block = hot_block(300, 0);
    let factory = fixture.env.blockchain_provider();
    let provider = factory.database_provider_ro()?;
    let tx = provider.tx_ref();
    let overlay = OutputTrieOverlay::new();
    let serial = overlay.layered_root_with_updates(tx, block.clone())?;

    CORRUPT_NEXT_STORAGE_SPLIT.with(|c| c.set(true));
    let wrong =
        overlay.layered_root_with_updates_parallel(factory, tx, block.clone(), Walks::SPLIT)?;
    assert_ne!(wrong.0, serial.0, "the corrupted storage root must reach the result unchecked");

    for (number, forced) in [(CHECK_ONE_BLOCK_IN, false), (CHECK_ONE_BLOCK_IN + 1, true)] {
        CORRUPT_NEXT_STORAGE_SPLIT.with(|c| c.set(true));
        CHECK_NEXT_SAMPLED_BLOCK.with(|c| c.set(forced));
        let (checked, fallbacks) = counting_fallbacks(|| {
            overlay.block_root_with_updates(
                factory,
                tx,
                block.clone(),
                ParallelRoots::Sampled(Walks::SPLIT),
                number,
            )
        });
        assert_eq!(checked?, serial, "block {number}: a sampled block must use the serial result");
        assert_eq!(fallbacks.get("mismatch"), Some(&1), "block {number}: fallbacks: {fallbacks:?}");
        assert!(!CORRUPT_NEXT_STORAGE_SPLIT.with(|c| c.get()), "the splits were not built");
    }
    Ok(())
}

/// In shadow mode a wrong composed storage root panics.
#[tokio::test]
#[should_panic(expected = "differs from the serial root")]
async fn shadow_panics_on_a_wrong_storage_root() {
    let fixture = Fixture::new(base()).unwrap();
    let factory = fixture.env.blockchain_provider();
    let provider = factory.database_provider_ro().unwrap();
    let overlay = OutputTrieOverlay::new();
    CORRUPT_NEXT_STORAGE_SPLIT.with(|c| c.set(true));
    let _ = overlay.block_root_with_updates(
        factory,
        provider.tx_ref(),
        hot_block(300, 0),
        ParallelRoots::Shadow(Walks::SPLIT),
        1,
    );
}

/// The pre-check refuses a stored root node (reth never writes one).
#[tokio::test]
async fn stored_root_node_is_refused() -> eyre::Result<()> {
    /// A storage-trie view with a node at the empty path.
    #[derive(Debug)]
    struct WithRoot<F>(F);
    /// Its storage-trie cursor.
    #[derive(Debug)]
    struct RootCursor<C>(C);
    impl<C: TrieCursor> TrieCursor for RootCursor<C> {
        fn seek_exact(
            &mut self,
            key: Nibbles,
        ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
            if key.is_empty() {
                let mask = TrieMask::new(0b11);
                return Ok(Some((
                    key,
                    BranchNodeCompact::new(mask, mask, mask, vec![B256::ZERO; 2], None),
                )));
            }
            self.0.seek_exact(key)
        }
        fn seek(
            &mut self,
            key: Nibbles,
        ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
            self.0.seek(key)
        }
        fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
            self.0.next()
        }
        fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
            self.0.current()
        }
        fn reset(&mut self) {
            self.0.reset()
        }
    }
    impl<C: TrieStorageCursor> TrieStorageCursor for RootCursor<C> {
        fn set_hashed_address(&mut self, hashed_address: B256) {
            self.0.set_hashed_address(hashed_address)
        }
    }
    impl<F: TrieCursorFactory> TrieCursorFactory for WithRoot<F> {
        type AccountTrieCursor<'a>
            = F::AccountTrieCursor<'a>
        where
            Self: 'a;
        type StorageTrieCursor<'a>
            = RootCursor<F::StorageTrieCursor<'a>>
        where
            Self: 'a;
        fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor<'_>, DatabaseError> {
            self.0.account_trie_cursor()
        }
        fn storage_trie_cursor(
            &self,
            hashed_address: B256,
        ) -> Result<Self::StorageTrieCursor<'_>, DatabaseError> {
            Ok(RootCursor(self.0.storage_trie_cursor(hashed_address)?))
        }
    }
    let fixture = Fixture::new(base())?;
    let provider = fixture.env.blockchain_provider().database_provider_ro()?;
    let overlay = OutputTrieOverlay::new();
    let current = HashedPostState::default().into_sorted();
    let (trie, _) = layered_factories(provider.tx_ref(), &overlay.runs, &current);
    assert_eq!(splittable(&trie, hot(0))?, Splittable::Yes);
    assert_eq!(splittable(&WithRoot(&trie), hot(0))?, Splittable::StoredRootNode);
    Ok(())
}
