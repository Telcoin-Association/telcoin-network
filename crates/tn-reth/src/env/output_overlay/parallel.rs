//! Parallel storage roots for a block's state root.
//!
//! A block's root is reth's serial `StateRoot` walk over the account trie, which stops at each
//! account leaf under a changed path to compute that account's storage root. The storage roots of
//! the accounts with changed storage (a non-empty storage prefix set) are most of that time on a
//! busy chain: a few hot contracts' tries. Here they run as tasks on rayon's global pool, one per
//! such account, while the account walk runs on the calling thread. The walk takes each ready
//! result through `StateRoot::with_storage_roots` (vendored reth-trie, TN-0139 in
//! `vendor/reth/CHANGES.md`) and computes the rest itself. The walk, the hash builder, the trie
//! updates and the storage-root computation are reth's own code; only where a storage root is
//! computed changes. Measured on the exec bench (Mac, storage v2, serial and parallel on alternate
//! blocks of one run): the root fell from 652 to 378 ms an output at peak (20 blocks of 30M gas)
//! and from 133 to 72 ms on the busy profile (8 blocks of 15M). The gain comes from overlapping
//! the storage roots with the walk, so two threads reach it.
//!
//! # Tasks
//!
//! Each task opens its own read-only transaction (a database transaction is not shared across
//! threads) and builds the walk's cursor stack over it through the same helper
//! (`layered_factories`): the current block's sorted state over the output's earlier blocks over
//! the database. The tasks borrow the overlay's runs and the current block's sorted state inside
//! `rayon::in_place_scope`; nothing is copied. Accounts destroyed in the current block get no task
//! (the walk meets no leaf for them).
//!
//! The walk never waits for a task that has not started: it claims it and computes the root itself
//! (the task then does nothing), and it waits only for a task already running, which waits on
//! nothing. A busy pool (batch validation on a validator shares it) therefore degrades the root to
//! serial instead of stalling it; the scope's end waits only for jobs the pool has not picked up,
//! which then return at once. A task that fails (opening its transaction, a database read, a
//! moved tip, below) or panics leaves its account to the walk, which computes it serially: the
//! failures are logged once a block and change no result.
//!
//! # The account walk
//!
//! A block with at least `SPLIT_MIN_ACCOUNTS` changed accounts also splits the account walk by
//! the first nibble of the hashed address (`account_split.rs`): sixteen subtrie walks, claimed in
//! order by the calling thread and up to [`WALK_HELPERS`] pool jobs (each on its own read-only
//! transaction, with the tip check below), take the storage roots from the tasks above, and the
//! calling thread composes the root branch and the union of their trie updates. A shape the split
//! does not compose (a small trie, a root that is not a branch) runs the walk on the calling thread
//! as before, counted in `tn_reth.account_walk_fallbacks_total`. A block with no storage-root
//! targets but a split walk takes this path too. Measured on the exec bench (storage v2, against
//! the walk on one thread with the storage-root tasks): the root fell from 311 to 198 ms an output
//! at peak and from 63 to 41 ms on the busy profile on the Mac, and from 561 to 322 and from 113 to
//! 70 ms on a 22-thread Linux VM. `TN_PARALLEL_ACCOUNT_WALK=0` turns the split off on its own.
//!
//! # Hot storage tries
//!
//! With the walk split, a busy block's root waits on its hottest contract's storage root. A target
//! with at least `SPLIT_MIN_SLOTS` changed slots whose stored trie has all sixteen first-nibble
//! nodes over hashed children is split the same way (`storage_split.rs`): sixteen pieces, each
//! reth's `StorageRoot` over storage cursors restricted to one nibble, claimed by the calling
//! thread and the same pool jobs before any account subtrie, so the walk that reaches the
//! contract's leaf only waits for running pieces; it composes the storage root and the union of
//! the pieces' trie updates through the storage-root source. Other targets stay one task each, and
//! a split that composes nothing leaves the storage root to the walk, counted in
//! `tn_reth.storage_walk_fallbacks_total`. Measured on the exec bench (storage v2, against the
//! account split alone): the root fell from 199 to 106 ms an output at peak and from 41 to 22 ms on
//! the busy profile on the Mac, and from 329 to 135 and from 68 to 30 ms on a 22-thread Linux VM.
//! `TN_PARALLEL_STORAGE_WALK=0` turns this split off on its own.
//!
//! # Why the tasks' transactions see the walk's state
//!
//! The walk and the tasks read separate transactions, so they see the same database only if
//! nothing commits between their opens. Nothing does on the node path: the engine executes one
//! consensus output at a time (`ExecutorEngine` holds at most one pending output), and
//! `execute_consensus_output` builds every block of an output before it persists them in line
//! (`RethEnv::persist_executed_output`), so an output's persist ends before the next output's
//! first root. No other writer of the state or trie tables runs beside block building: genesis is
//! written before any block, `db load-state` runs on its own env and builds no block, and the
//! startup check refuses to start rather than unwind. The output overlay relies on the same fact
//! (its anchor is the database tip).
//!
//! What would break it: persisting an output while the next one builds (a pipelined persist),
//! building two outputs' blocks at once, or any other writer of the state or trie tables during an
//! output. Each task therefore checks its transaction's tip against the walk's: the finalized
//! block number (`ChainState`), which every persist advances in the same transaction as the state
//! it writes. On a mismatch the task fails and the walk computes that root on its own transaction;
//! the failure is logged at error and counted in `tn_reth.parallel_state_root_task_failures_total`
//! as `tip_moved` (alert on any nonzero value). A writer that changed state without advancing that
//! marker would not be caught.
//!
//! # Checks
//!
//! A wrong root forks the chain, and TN builds its own blocks: no payload's root checks this one.
//! In production the blocks whose number is a multiple of [`CHECK_ONE_BLOCK_IN`] (of those with
//! parallel work: storage-root tasks or a split account walk) also compute the root serially and
//! compare the roots and the trie updates; on a difference the block logs both roots, whether the
//! split composed the block's and how many split storage roots it took, counts it
//! (`tn_reth.parallel_state_root_mismatches_total`, a split walk's also in
//! `tn_reth.account_walk_fallbacks_total` and a block with split storage roots also in
//! `tn_reth.storage_walk_fallbacks_total`, both as `mismatch`) and uses the serial result.
//!
//! The sample is a function of the block alone, never of what one node has seen since it started,
//! so every node checks the same blocks. A repeatable bug in a parallel path then gives every node
//! running the same code the same root: the serial one on a checked block, the same wrong one on an
//! unchecked block. (A per-node count would split such nodes by their counts' phase.) The one
//! per-node difference left is a task that fails (logged, and counted by reason in
//! `tn_reth.parallel_state_root_task_failures_total`): its part goes to the serial computation on
//! that node only. The pool's thread count is no such difference: it changes which thread computes
//! a part, not how, so a node whose pool runs one thread runs the same parallel code (on the
//! calling thread and that one pool thread) and composes the same roots as its peers; only the off
//! switches compute serially.
//!
//! Any nonzero mismatch count therefore means the parallel code computed a wrong root, and the
//! unchecked blocks the same bug hit carry it on every node running this code. The response is
//! network-wide, not local: turn the parallel paths off on every node (`TN_PARALLEL_STATE_ROOT=0`,
//! or only the split the `mismatch` fallback names, `TN_PARALLEL_ACCOUNT_WALK=0` or
//! `TN_PARALLEL_STORAGE_WALK=0`; read at the first block, so a restart) and report it. Turning
//! them off on one node only parts it from its peers on the next block the bug hits.
//!
//! With `TN_PARALLEL_STATE_ROOT_SHADOW=1` (test builds; the default in tn-reth's unit tests) every
//! such block is compared and a difference panics: the shadow runs of the exec bench compare every
//! block, split ones (account walk and storage tries) included.
//!
//! # Off switch
//!
//! `TN_PARALLEL_STATE_ROOT=0` computes every root serially, as before; unset or `1` uses the tasks.
//! Any other value is logged as an error and also turns them off (the serial root is the
//! reference). It is read once, at the first block. `TN_PARALLEL_ACCOUNT_WALK=0` keeps the
//! storage-root tasks and runs the account walk on one thread (`account_split.rs`);
//! `TN_PARALLEL_STORAGE_WALK=0` keeps every storage root on one task (`storage_split.rs`). An
//! environment variable rather than a constant, so an operator can turn the tasks off without a new
//! release, and rather than a config field or CLI flag, which would carry a setting with no use
//! once the code has proven itself; TN reads deployment overrides from the environment elsewhere
//! too (the swarm listener addresses).

use super::{
    account_split::{AccountWalk, Subtries},
    layered_factories,
    sorted_runs::SortedTrieRuns,
    storage_split::{splittable, Splittable, StorageRootResult, StorageSplits, StorageWalk},
    OutputTrieOverlay,
};
use crate::metrics::{
    record_account_walk_fallback, record_parallel_root_task_failure, record_storage_walk_fallback,
    AccountWalkFallback, ParallelRootTaskFailure, StorageWalkFallback, RETH_METRICS,
};
use parking_lot::{Condvar, Mutex};
use reth_db::{tables, transaction::DbTx, DatabaseError};
use reth_errors::ProviderError;
use reth_provider::{DBProvider as _, DatabaseProviderFactory, ProviderResult};
use reth_trie::{
    prefix_set::{PrefixSet, TriePrefixSets},
    trie_cursor::{TrieCursor as _, TrieCursorFactory},
    updates::{StorageTrieUpdates, TrieUpdates},
    HashedPostState, HashedPostStateSorted, Nibbles, StateRoot, StorageRoot, StorageRootSource,
};
use reth_trie_db::DatabaseStorageRoot as _;
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, OnceLock},
};
use tn_types::{BlockNumber, B256};
use tracing::{error, info, warn};

/// One block in this many, of those with parallel work, is also computed serially and compared
/// in production: the blocks whose number is a multiple of it (see the module docs). A power of
/// two.
///
/// A check costs one serial root on top of the parallel one. On the exec bench (Mac, storage v2)
/// a serial root takes 32.6 ms a block at peak and 16.7 ms on the busy profile, and the tasks save
/// 13.7 and 7.6 ms a block. At 1 in 256 the checks cost 0.13 and 0.07 ms a block on average, under
/// 1% of what the tasks save; a peak output (20 blocks) is checked about every 13 outputs.
pub(crate) const CHECK_ONE_BLOCK_IN: u64 = 256;

/// The off switch (module docs).
const PARALLEL_ENV: &str = "TN_PARALLEL_STATE_ROOT";

/// How a block's root uses parallel storage roots and how its walks run with them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParallelRoots {
    /// Every root serial (the off switch).
    Off,
    /// Tasks, with a serial comparison on one block in [`CHECK_ONE_BLOCK_IN`] (production).
    Sampled(Walks),
    /// Tasks and a serial comparison on every block; a difference panics (test builds: tests and
    /// benches).
    #[cfg(any(test, feature = "test-utils"))]
    Shadow(Walks),
}

/// How a block's account walk and its hot storage tries run when its root uses the tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Walks {
    /// The account walk (`account_split.rs`).
    pub(crate) account: AccountWalk,
    /// The hot storage tries (`storage_split.rs`).
    pub(crate) storage: StorageWalk,
}

impl Walks {
    /// Both walks split when a block is worth it (the default).
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) const SPLIT: Self =
        Self { account: AccountWalk::Split, storage: StorageWalk::Split };

    /// The walks `account` and `storage`.
    #[cfg(test)]
    pub(crate) const fn new(account: AccountWalk, storage: StorageWalk) -> Self {
        Self { account, storage }
    }

    /// The walks `TN_PARALLEL_ACCOUNT_WALK` and `TN_PARALLEL_STORAGE_WALK` select, read once.
    fn from_env() -> Self {
        Self { account: AccountWalk::from_env(), storage: StorageWalk::from_env() }
    }
}

impl ParallelRoots {
    /// The mode `TN_PARALLEL_STATE_ROOT`, `TN_PARALLEL_ACCOUNT_WALK` and
    /// `TN_PARALLEL_STORAGE_WALK` (and, in test builds, `TN_PARALLEL_STATE_ROOT_SHADOW`) select,
    /// read once.
    pub(crate) fn from_env() -> Self {
        static MODE: OnceLock<ParallelRoots> = OnceLock::new();
        *MODE.get_or_init(|| {
            match std::env::var(PARALLEL_ENV).as_deref() {
                Err(_) | Ok("1") => {}
                Ok("0") => {
                    info!(target: "engine", "{PARALLEL_ENV}=0: state roots are computed serially");
                    return Self::Off;
                }
                Ok(other) => {
                    error!(target: "engine", "{PARALLEL_ENV}={other}: expected 0 or 1; state roots are computed serially");
                    return Self::Off;
                }
            }
            let walks = Walks::from_env();
            #[cfg(any(test, feature = "test-utils"))]
            if shadow_from_env() {
                return Self::Shadow(walks);
            }
            Self::Sampled(walks)
        })
    }
}

/// `TN_PARALLEL_STATE_ROOT_SHADOW=1` compares every block (`0` off); unit tests default to on,
/// other test builds to off.
#[cfg(any(test, feature = "test-utils"))]
fn shadow_from_env() -> bool {
    match std::env::var("TN_PARALLEL_STATE_ROOT_SHADOW").as_deref() {
        Ok("1") => true,
        Ok("0") => false,
        Ok(other) => panic!("TN_PARALLEL_STATE_ROOT_SHADOW={other}: expected 0 or 1"),
        Err(_) => cfg!(test),
    }
}

/// Whether block `number`'s root, when it has parallel work, is also computed serially and
/// compared: one block in [`CHECK_ONE_BLOCK_IN`], by number alone, so every node checks the same
/// blocks (module docs).
fn block_is_checked(number: BlockNumber) -> bool {
    #[cfg(test)]
    if CHECK_NEXT_SAMPLED_BLOCK.with(|c| c.replace(false)) {
        return true;
    }
    number.is_multiple_of(CHECK_ONE_BLOCK_IN)
}

impl OutputTrieOverlay {
    /// Block `number`'s state root and trie updates, as [`Self::layered_root_with_updates`]
    /// computes them, with the storage roots of the accounts with changed storage on rayon's pool
    /// as `mode` says. `factory` opens the tasks' transactions; it must be `tx`'s database.
    pub(crate) fn block_root_with_updates<F, TX>(
        &self,
        factory: &F,
        tx: &TX,
        current: HashedPostState,
        mode: ParallelRoots,
        number: BlockNumber,
    ) -> ProviderResult<(B256, TrieUpdates)>
    where
        F: DatabaseProviderFactory,
        TX: DbTx,
    {
        let prefix_sets = current.construct_prefix_sets().freeze();
        let current = current.into_sorted();
        let (walks, shadow) = match mode {
            ParallelRoots::Off => return self.serial_root(tx, &current, prefix_sets),
            ParallelRoots::Sampled(walks) => (walks, false),
            #[cfg(any(test, feature = "test-utils"))]
            ParallelRoots::Shadow(walks) => (walks, true),
        };
        let targets = storage_root_targets(&prefix_sets);
        let split = walks.account.splits(&prefix_sets);
        if targets.is_empty() && !split {
            return self.serial_root(tx, &current, prefix_sets);
        }
        let storage = walks.storage;
        if !shadow && !block_is_checked(number) {
            return Ok(self
                .parallel_root(factory, tx, &current, prefix_sets, targets, split, storage)?
                .result);
        }
        let parallel = self.parallel_root(
            factory,
            tx,
            &current,
            prefix_sets.clone(),
            targets,
            split,
            storage,
        )?;
        let serial = self.serial_root(tx, &current, prefix_sets)?;
        Ok(checked(serial, parallel, shadow))
    }

    /// [`Self::block_root_with_updates`] with the tasks, the walks as `walks` says, and no
    /// comparison.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn layered_root_with_updates_parallel<F, TX>(
        &self,
        factory: &F,
        tx: &TX,
        current: HashedPostState,
        walks: Walks,
    ) -> ProviderResult<(B256, TrieUpdates)>
    where
        F: DatabaseProviderFactory,
        TX: DbTx,
    {
        let prefix_sets = current.construct_prefix_sets().freeze();
        let current = current.into_sorted();
        let targets = storage_root_targets(&prefix_sets);
        let split = walks.account.splits(&prefix_sets);
        if targets.is_empty() && !split {
            return self.serial_root(tx, &current, prefix_sets);
        }
        Ok(self
            .parallel_root(factory, tx, &current, prefix_sets, targets, split, walks.storage)?
            .result)
    }

    /// The account walk over `tx`, split by first nibble when `split` says so (on this thread and
    /// pool jobs) and otherwise on this thread, with one storage-root task per target on rayon's
    /// global pool, the hot targets' tries split as `storage` says (pieces on this thread and the
    /// same pool jobs, before the walk).
    #[allow(clippy::too_many_arguments)]
    fn parallel_root<F, TX>(
        &self,
        factory: &F,
        tx: &TX,
        current: &HashedPostStateSorted,
        prefix_sets: TriePrefixSets,
        targets: Vec<(B256, PrefixSet)>,
        split: bool,
        storage: StorageWalk,
    ) -> ProviderResult<ParallelRoot>
    where
        F: DatabaseProviderFactory,
        TX: DbTx,
    {
        let walk_tip = snapshot_tip(tx)?;
        let runs = &self.runs;
        let (trie, hashed) = layered_factories(tx, runs, current);
        let (whole, hot) = split_storage_targets(&trie, targets, storage)?;
        let tasks = Arc::new(StorageRootTasks::new(whole, StorageSplits::new(hot)));
        let source = Arc::clone(&tasks) as Arc<dyn StorageRootSource>;
        let stored_root_node = split && has_stored_root_node(&trie)?;
        if stored_root_node {
            record_account_walk_fallback(AccountWalkFallback::StoredRootNode);
        }
        let subtries = (split && !stored_root_node).then(|| Subtries::new(&prefix_sets));
        let root = rayon::in_place_scope(|scope| -> ProviderResult<ParallelRoot> {
            let shared = &*tasks;
            // However the walk below ends, a task not yet started returns at once: the scope's end
            // waits for every job.
            let _claim = ClaimUnstartedOnExit(shared);
            // The slot jobs go first and the helpers after. From the engine thread (not a pool
            // worker) both enter the pool's FIFO queue, so the helpers start only after every slot
            // job has been taken; spawning them first would start the hot pieces and the subtries
            // sooner on a block with many small targets, but it moves work between threads on
            // every block, so it waits for a timing A/B on the many-small-contracts profile.
            for slot in &shared.slots {
                scope.spawn(move |_| {
                    shared.run(slot, || storage_root_task(factory, walk_tip, runs, current, slot))
                });
            }
            let splits = &shared.splits;
            let subtries = subtries.as_ref();
            if subtries.is_some() || !splits.is_empty() {
                let source = &source;
                for _ in 0..rayon::current_num_threads().min(WALK_HELPERS) {
                    scope.spawn(move |_| {
                        walk_helper(factory, walk_tip, runs, current, subtries, source, shared)
                    });
                }
            }
            // The hot storage tries' pieces first: the walks below consume them.
            splits.work(tx, &trie, &hashed, &|error| shared.record_failure(error));
            let mut composed = None;
            if let Some(subtries) = subtries {
                let source = &source;
                subtries.work(&trie, &hashed, source, &|error| shared.record_failure(error));
                composed =
                    subtries.finish(&trie, &hashed, source).map_err(TaskError::into_provider)?;
            }
            let (result, composed) = match composed {
                Some(result) => (result, true),
                None => {
                    if subtries.is_some() {
                        record_account_walk_fallback(AccountWalkFallback::Shape);
                    }
                    #[cfg(test)]
                    if FAIL_NEXT_WALK.with(|f| f.replace(false)) {
                        return Err(DatabaseError::Other(INJECTED_WALK_FAILURE.into()).into());
                    }
                    let result = StateRoot::new(&trie, &hashed)
                        .with_prefix_sets(prefix_sets)
                        .with_storage_roots(Arc::clone(&source))
                        .root_with_updates()?;
                    (result, false)
                }
            };
            Ok(ParallelRoot { result, composed, storage_composed: splits.composed() })
        });
        tasks.log_failures();
        tasks.splits.record_fallbacks();
        #[cfg(test)]
        if let Ok(root) = &root {
            STORAGE_SPLITS_COMPOSED.with(|n| n.set(n.get() + root.storage_composed));
        }
        root
    }
}

/// A block's root and trie updates from [`OutputTrieOverlay::parallel_root`].
#[derive(Debug)]
struct ParallelRoot {
    /// The root and trie updates.
    result: (B256, TrieUpdates),
    /// Whether the split account walk composed them (otherwise the walk ran on one thread).
    composed: bool,
    /// Storage roots the walk took composed from split tries' pieces.
    storage_composed: usize,
}

/// Pool jobs offered to the split walks besides the calling thread: one per account subtrie but
/// the one the calling thread takes first (the storage pieces come first on each).
const WALK_HELPERS: usize = 15;

/// Storage-root targets: hashed address and storage prefix set.
type Targets = Vec<(B256, PrefixSet)>;

/// `targets` parted into the ones that stay one task and the hot ones whose tries split, as
/// `storage` says and the stored tries' shapes allow (over the walk's `trie`). Refused hot tries
/// are counted by reason.
fn split_storage_targets<T: TrieCursorFactory>(
    trie: &T,
    targets: Targets,
    storage: StorageWalk,
) -> Result<(Targets, Targets), DatabaseError> {
    let mut whole = Vec::with_capacity(targets.len());
    let mut hot = Vec::new();
    for (address, prefix_set) in targets {
        if !storage.wants_split(&prefix_set) {
            whole.push((address, prefix_set));
            continue;
        }
        match splittable(trie, address)? {
            Splittable::Yes => hot.push((address, prefix_set)),
            Splittable::SmallTrie => {
                record_storage_walk_fallback(StorageWalkFallback::SmallTrie);
                whole.push((address, prefix_set));
            }
            Splittable::StoredRootNode => {
                record_storage_walk_fallback(StorageWalkFallback::StoredRootNode);
                whole.push((address, prefix_set));
            }
        }
    }
    Ok((whole, hot))
}

/// Whether the account trie holds a node at the empty path. Reth never writes one (its trie
/// updates drop the root path); a walk that found one would skip unchanged nibbles by its hashes,
/// which the split does not reproduce.
fn has_stored_root_node<T: TrieCursorFactory>(trie: &T) -> Result<bool, DatabaseError> {
    Ok(trie.account_trie_cursor()?.seek_exact(Nibbles::default())?.is_some())
}

/// A pool job working the split storage tries' pieces and then the account walk's subtries (when
/// split) on its own read-only transaction, unless the calling thread has already claimed them
/// all. A failure leaves its work to the calling thread.
fn walk_helper<F: DatabaseProviderFactory>(
    factory: &F,
    walk_tip: Option<BlockNumber>,
    runs: &SortedTrieRuns,
    current: &HashedPostStateSorted,
    subtries: Option<&Subtries>,
    source: &Arc<dyn StorageRootSource>,
    tasks: &StorageRootTasks,
) {
    if tasks.splits.all_claimed() && subtries.is_none_or(Subtries::all_claimed) {
        return;
    }
    let helper = || -> Result<(), TaskError> {
        let provider = factory.database_provider_ro()?;
        let tx = provider.tx_ref();
        let task_tip = snapshot_tip(tx)?;
        if task_tip != walk_tip {
            return Err(TaskError::TipMoved { walk: walk_tip, task: task_tip });
        }
        let (trie, hashed) = layered_factories(tx, runs, current);
        let failures = |error| tasks.record_failure(error);
        tasks.splits.work(tx, &trie, &hashed, &failures);
        if let Some(subtries) = subtries {
            subtries.work(&trie, &hashed, source, &failures);
        }
        Ok(())
    };
    let result = catch_unwind(AssertUnwindSafe(helper)).unwrap_or(Err(TaskError::Panicked));
    if let Err(error) = result {
        tasks.record_failure(error);
    }
}

/// The accounts that get a storage-root task: changed storage (a non-empty prefix set), not
/// destroyed in this block. Sorted by hashed address, the walk's order.
fn storage_root_targets(prefix_sets: &TriePrefixSets) -> Vec<(B256, PrefixSet)> {
    let mut targets: Vec<_> = prefix_sets
        .storage_prefix_sets
        .iter()
        .filter(|(address, prefix_set)| {
            !prefix_set.is_empty() && !prefix_sets.destroyed_accounts.contains(*address)
        })
        .map(|(address, prefix_set)| (*address, prefix_set.clone()))
        .collect();
    targets.sort_unstable_by_key(|(address, _)| *address);
    targets
}

/// The tip a transaction sees: the finalized block number, which every persist advances in the
/// same transaction as the state it writes.
fn snapshot_tip<TX: DbTx>(tx: &TX) -> Result<Option<BlockNumber>, DatabaseError> {
    tx.get::<tables::ChainState>(tables::ChainStateKey::LastFinalizedBlock)
}

/// One account's storage root on its own read-only transaction, over the walk's cursor stack.
fn storage_root_task<F: DatabaseProviderFactory>(
    factory: &F,
    walk_tip: Option<BlockNumber>,
    runs: &SortedTrieRuns,
    current: &HashedPostStateSorted,
    slot: &Slot,
) -> Result<(B256, usize, StorageTrieUpdates), TaskError> {
    let provider = factory.database_provider_ro()?;
    let tx = provider.tx_ref();
    let task_tip = snapshot_tip(tx)?;
    if task_tip != walk_tip {
        return Err(TaskError::TipMoved { walk: walk_tip, task: task_tip });
    }
    let (trie, hashed) = layered_factories(tx, runs, current);
    // Built from reth-trie-db's constructor, which supplies the metrics argument when reth-trie's
    // `metrics` feature is on, then given the walk's factories.
    Ok(StorageRoot::from_tx_hashed(tx, slot.hashed_address)
        .with_trie_cursor_factory(trie)
        .with_hashed_cursor_factory(hashed)
        .with_prefix_set(slot.prefix_set.clone())
        .root_with_updates()
        .map_err(DatabaseError::from)?)
}

/// Why a storage-root or subtrie task left its work to the walk.
#[derive(Debug, thiserror::Error)]
pub(super) enum TaskError {
    /// Opening the task's transaction failed.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// A database read failed.
    #[error(transparent)]
    Database(#[from] DatabaseError),
    /// The task's transaction sees another tip than the walk's: the database moved.
    #[error("the task's transaction sees finalized block {task:?}, the walk's {walk:?}")]
    TipMoved {
        /// The walk's finalized block number.
        walk: Option<BlockNumber>,
        /// The task's.
        task: Option<BlockNumber>,
    },
    /// The task panicked.
    #[error("the task panicked")]
    Panicked,
}

impl TaskError {
    /// The `reason` this failure is counted under in
    /// `tn_reth.parallel_state_root_task_failures_total`.
    const fn reason(&self) -> ParallelRootTaskFailure {
        match self {
            Self::Provider(_) => ParallelRootTaskFailure::Provider,
            Self::Database(_) => ParallelRootTaskFailure::Database,
            Self::TipMoved { .. } => ParallelRootTaskFailure::TipMoved,
            Self::Panicked => ParallelRootTaskFailure::Panicked,
        }
    }

    /// The error of a computation on the walk's own transaction (a database read: the walk's
    /// threads never see the other variants).
    fn into_provider(self) -> ProviderError {
        match self {
            Self::Provider(error) => error,
            Self::Database(error) => error.into(),
            other => ProviderError::Database(DatabaseError::Other(other.to_string())),
        }
    }
}

/// Where one storage-root task stands.
#[derive(Debug)]
enum TaskState {
    /// Not started: the walk may claim it.
    Pending,
    /// Running on a pool thread: the walk waits for it.
    Running,
    /// Finished: the root, or `None` when the task failed.
    Done(Option<(B256, usize, StorageTrieUpdates)>),
    /// Claimed by the walk (or its result taken): the task does nothing.
    Claimed,
}

/// One account's task, shared by the task and the walk.
#[derive(Debug)]
struct Slot {
    /// The account.
    hashed_address: B256,
    /// Its storage prefix set, as the walk would use it.
    prefix_set: PrefixSet,
    /// Where the task stands.
    state: Mutex<TaskState>,
    /// Signalled when a running task finishes.
    finished: Condvar,
}

/// A block's storage-root tasks and split storage tries: the walk's [`StorageRootSource`].
#[derive(Debug)]
struct StorageRootTasks {
    /// One per target that stays one task, sorted by hashed address.
    slots: Vec<Slot>,
    /// The hot targets' split tries.
    splits: StorageSplits,
    /// Failed tasks and the first failure, logged once after the walk.
    failures: Mutex<(usize, Option<TaskError>)>,
}

impl StorageRootTasks {
    /// The tasks for `targets`, sorted by hashed address, and the split tries `splits`.
    fn new(targets: Vec<(B256, PrefixSet)>, splits: StorageSplits) -> Self {
        let slots = targets
            .into_iter()
            .map(|(hashed_address, prefix_set)| Slot {
                hashed_address,
                prefix_set,
                state: Mutex::new(TaskState::Pending),
                finished: Condvar::new(),
            })
            .collect();
        Self { slots, splits, failures: Mutex::new((0, None)) }
    }

    /// Run `slot`'s task unless the walk claimed it; a failure or panic leaves the account to the
    /// walk.
    fn run(
        &self,
        slot: &Slot,
        task: impl FnOnce() -> Result<(B256, usize, StorageTrieUpdates), TaskError>,
    ) {
        {
            let mut state = slot.state.lock();
            if !matches!(*state, TaskState::Pending) {
                return;
            }
            *state = TaskState::Running;
        }
        #[cfg(test)]
        TASKS_STARTED.with(|n| n.set(n.get() + 1));
        let result = catch_unwind(AssertUnwindSafe(task)).unwrap_or(Err(TaskError::Panicked));
        let root = result.map_err(|error| self.record_failure(error)).ok();
        *slot.state.lock() = TaskState::Done(root);
        slot.finished.notify_all();
    }

    /// Count a failed task, in the block's log line (keeping the first failure for it) and in
    /// `tn_reth.parallel_state_root_task_failures_total` by reason.
    fn record_failure(&self, error: TaskError) {
        record_parallel_root_task_failure(error.reason());
        let mut failures = self.failures.lock();
        failures.0 += 1;
        failures.1.get_or_insert(error);
    }

    /// Mark the tasks that have not started as claimed, so they return at once (the walk is done
    /// or failed).
    fn claim_unstarted(&self) {
        for slot in &self.slots {
            let mut state = slot.state.lock();
            if matches!(*state, TaskState::Pending) {
                *state = TaskState::Claimed;
            }
        }
    }

    /// Log the block's failed tasks, if any.
    fn log_failures(&self) {
        let (count, first) = &*self.failures.lock();
        if let Some(error) = first {
            let tasks = self.slots.len() + self.splits.pieces();
            if matches!(error, TaskError::TipMoved { .. }) {
                error!(target: "engine", %error, count, tasks, "root tasks saw another database tip than the block's root; computed serially");
            } else {
                warn!(target: "engine", %error, count, tasks, "root tasks failed; computed serially");
            }
        }
    }
}

/// Claims a block's unstarted storage-root tasks when the walk ends, with a result, an error or a
/// panic. The walk's early exits all come after the calling thread's `work` calls, which leave no
/// piece or subtrie unclaimed, so the slot tasks are all that is left to stop.
struct ClaimUnstartedOnExit<'a>(&'a StorageRootTasks);

impl Drop for ClaimUnstartedOnExit<'_> {
    fn drop(&mut self) {
        self.0.claim_unstarted();
    }
}

impl StorageRootSource for StorageRootTasks {
    fn storage_root(&self, hashed_address: B256) -> Option<StorageRootResult> {
        let Ok(index) = self.slots.binary_search_by_key(&hashed_address, |s| s.hashed_address)
        else {
            return self.splits.take(hashed_address).flatten();
        };
        let slot = &self.slots[index];
        let mut state = slot.state.lock();
        loop {
            match std::mem::replace(&mut *state, TaskState::Claimed) {
                TaskState::Running => {
                    *state = TaskState::Running;
                    slot.finished.wait(&mut state);
                }
                TaskState::Done(root) => {
                    #[cfg(test)]
                    if root.is_some() {
                        TASK_ROOTS_TAKEN.with(|n| n.set(n.get() + 1));
                    }
                    return root;
                }
                TaskState::Pending | TaskState::Claimed => return None,
            }
        }
    }
}

/// A compared block's result: the serial one. On a difference, panic in shadow mode; otherwise
/// log both roots and count it, a split walk's also as an account-walk fallback.
fn checked(
    serial: (B256, TrieUpdates),
    parallel: ParallelRoot,
    shadow: bool,
) -> (B256, TrieUpdates) {
    let ParallelRoot { result: parallel, composed: split, storage_composed } = parallel;
    if serial == parallel {
        return serial;
    }
    let (serial_root, parallel_root) = (serial.0, parallel.0);
    let updates_equal = serial.1 == parallel.1;
    if shadow {
        panic!(
            "parallel state root {parallel_root} differs from the serial root {serial_root} \
             (trie updates equal: {updates_equal}, split walk: {split}, split storage roots: \
             {storage_composed})"
        );
    }
    error!(target: "engine", %serial_root, %parallel_root, updates_equal, split, storage_composed, "parallel state root differs from the serial one; the block uses the serial result");
    RETH_METRICS.parallel_state_root_mismatches_total.increment(1);
    if split {
        record_account_walk_fallback(AccountWalkFallback::Mismatch);
    }
    if storage_composed > 0 {
        record_storage_walk_fallback(StorageWalkFallback::Mismatch);
    }
    serial
}

#[cfg(test)]
thread_local! {
    /// Storage roots composed from split tries in the roots this thread computed (tests check the
    /// storage split ran).
    pub(super) static STORAGE_SPLITS_COMPOSED: std::cell::Cell<usize> =
        const { std::cell::Cell::new(0) };
    /// Task results the walks on this thread took (tests check the tasks ran).
    static TASK_ROOTS_TAKEN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Set by a test to make this thread's next sampled block a compared one.
    pub(super) static CHECK_NEXT_SAMPLED_BLOCK: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
    /// Storage-root tasks this thread started (not claimed before they ran).
    static TASKS_STARTED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Set by a test to make this thread's next walk on one thread fail before it reads anything,
    /// as a database read error would.
    static FAIL_NEXT_WALK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The error a walk failed by [`FAIL_NEXT_WALK`] returns.
#[cfg(test)]
const INJECTED_WALK_FAILURE: &str = "injected walk failure";

#[cfg(test)]
#[path = "parallel_tests.rs"]
mod tests;
