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
//! order by the calling thread and up to [`SUBTRIE_HELPERS`] pool jobs (each on its own read-only
//! transaction, with the tip check below), take the storage roots from the tasks above, and the
//! calling thread composes the root branch and the union of their trie updates. A shape the split
//! does not compose (a small trie, a root that is not a branch) runs the walk on the calling thread
//! as before, counted in `tn_reth.account_walk_fallbacks_total`. A block with no storage-root
//! targets but a split walk takes this path too. Measured on the exec bench (storage v2, against
//! the walk on one thread with the storage-root tasks): the root fell from 311 to 198 ms an output
//! at peak and from 63 to 41 ms on the busy profile on the Mac, and from 561 to 322 and from 113 to
//! 70 ms on a 22-thread Linux VM. `TN_PARALLEL_ACCOUNT_WALK=0` turns the split off on its own.
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
//! it writes. On a mismatch the task fails and the walk computes that root on its own transaction.
//! A writer that changed state without advancing that marker would not be caught.
//!
//! # Checks
//!
//! A wrong root forks the chain, and TN builds its own blocks: no payload's root checks this one.
//! In production one block in [`CHECK_ONE_BLOCK_IN`] (of those with parallel work: storage-root
//! tasks or a split account walk) also computes the root serially and compares the roots and the
//! trie updates; on a difference it logs both roots and whether the split composed the block's,
//! counts it (`tn_reth.parallel_state_root_mismatches_total`, and a split's also in
//! `tn_reth.account_walk_fallbacks_total` as `mismatch`) and uses the serial result. With
//! `TN_PARALLEL_STATE_ROOT_SHADOW=1` (test builds; the default in tn-reth's unit tests) every such
//! block is compared and a difference panics: the shadow runs of the exec bench compare every
//! block, split ones included.
//!
//! # Off switch
//!
//! `TN_PARALLEL_STATE_ROOT=0` computes every root serially, as before; unset or `1` uses the tasks.
//! Any other value is logged as an error and also turns them off (the serial root is the
//! reference). It is read once, at the first block. `TN_PARALLEL_ACCOUNT_WALK=0` keeps the
//! storage-root tasks and runs the account walk on one thread (`account_split.rs`). An environment
//! variable rather than a constant, so an operator can turn the tasks off without a new release,
//! and rather than a config field or CLI flag, which would carry a setting with no use once the
//! code has proven itself; TN reads deployment overrides from the environment elsewhere too (the
//! swarm listener addresses).

use super::{
    account_split::{AccountWalk, Subtries},
    layered_factories,
    sorted_runs::SortedTrieRuns,
    OutputTrieOverlay,
};
use crate::metrics::{record_account_walk_fallback, AccountWalkFallback, RETH_METRICS};
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
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
};
use tn_types::{BlockNumber, B256};
use tracing::{error, info, warn};

/// One block in this many, of those with storage-root tasks, is also computed serially and
/// compared in production (see the module docs). A power of two.
///
/// A check costs one serial root on top of the parallel one. On the exec bench (Mac, storage v2)
/// a serial root takes 32.6 ms a block at peak and 16.7 ms on the busy profile, and the tasks save
/// 13.7 and 7.6 ms a block. At 1 in 256 the checks cost 0.13 and 0.07 ms a block on average, under
/// 1% of what the tasks save; a peak output (20 blocks) is checked about every 13 outputs.
pub(crate) const CHECK_ONE_BLOCK_IN: u64 = 256;

/// The off switch (module docs).
const PARALLEL_ENV: &str = "TN_PARALLEL_STATE_ROOT";

/// How a block's root uses parallel storage roots and how its account walk runs with them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParallelRoots {
    /// Every root serial (the off switch).
    Off,
    /// Tasks, with a serial comparison on one block in [`CHECK_ONE_BLOCK_IN`] (production).
    Sampled(AccountWalk),
    /// Tasks and a serial comparison on every block; a difference panics (test builds: tests and
    /// benches).
    #[cfg(any(test, feature = "test-utils"))]
    Shadow(AccountWalk),
}

impl ParallelRoots {
    /// The mode `TN_PARALLEL_STATE_ROOT` and `TN_PARALLEL_ACCOUNT_WALK` (and, in test builds,
    /// `TN_PARALLEL_STATE_ROOT_SHADOW`) select, read once.
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
            let walk = AccountWalk::from_env();
            #[cfg(any(test, feature = "test-utils"))]
            if shadow_from_env() {
                return Self::Shadow(walk);
            }
            Self::Sampled(walk)
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

/// Whether the next sampled block is compared: one in [`CHECK_ONE_BLOCK_IN`], the first included.
fn next_block_is_checked() -> bool {
    #[cfg(test)]
    if CHECK_NEXT_SAMPLED_BLOCK.with(|c| c.replace(false)) {
        return true;
    }
    static BLOCKS: AtomicU64 = AtomicU64::new(0);
    BLOCKS.fetch_add(1, Ordering::Relaxed).is_multiple_of(CHECK_ONE_BLOCK_IN)
}

impl OutputTrieOverlay {
    /// A block's state root and trie updates, as [`Self::layered_root_with_updates`] computes them,
    /// with the storage roots of the accounts with changed storage on rayon's pool as `mode` says.
    /// `factory` opens the tasks' transactions; it must be `tx`'s database.
    pub(crate) fn block_root_with_updates<F, TX>(
        &self,
        factory: &F,
        tx: &TX,
        current: HashedPostState,
        mode: ParallelRoots,
    ) -> ProviderResult<(B256, TrieUpdates)>
    where
        F: DatabaseProviderFactory,
        TX: DbTx,
    {
        let prefix_sets = current.construct_prefix_sets().freeze();
        let current = current.into_sorted();
        let (walk, shadow) = match mode {
            ParallelRoots::Off => return self.serial_root(tx, &current, prefix_sets),
            ParallelRoots::Sampled(walk) => (walk, false),
            #[cfg(any(test, feature = "test-utils"))]
            ParallelRoots::Shadow(walk) => (walk, true),
        };
        let targets = storage_root_targets(&prefix_sets);
        let split = walk.splits(&prefix_sets);
        if targets.is_empty() && !split {
            return self.serial_root(tx, &current, prefix_sets);
        }
        if !shadow && !next_block_is_checked() {
            return Ok(self
                .parallel_root(factory, tx, &current, prefix_sets, targets, split)?
                .result);
        }
        let parallel =
            self.parallel_root(factory, tx, &current, prefix_sets.clone(), targets, split)?;
        let serial = self.serial_root(tx, &current, prefix_sets)?;
        Ok(checked(serial, parallel, shadow))
    }

    /// [`Self::block_root_with_updates`] with the tasks, the account walk as `walk` says, and no
    /// comparison.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn layered_root_with_updates_parallel<F, TX>(
        &self,
        factory: &F,
        tx: &TX,
        current: HashedPostState,
        walk: AccountWalk,
    ) -> ProviderResult<(B256, TrieUpdates)>
    where
        F: DatabaseProviderFactory,
        TX: DbTx,
    {
        let prefix_sets = current.construct_prefix_sets().freeze();
        let current = current.into_sorted();
        let targets = storage_root_targets(&prefix_sets);
        let split = walk.splits(&prefix_sets);
        if targets.is_empty() && !split {
            return self.serial_root(tx, &current, prefix_sets);
        }
        Ok(self.parallel_root(factory, tx, &current, prefix_sets, targets, split)?.result)
    }

    /// The account walk over `tx`, split by first nibble when `split` says so (on this thread and
    /// pool jobs) and otherwise on this thread, with one storage-root task per target on rayon's
    /// global pool.
    fn parallel_root<F, TX>(
        &self,
        factory: &F,
        tx: &TX,
        current: &HashedPostStateSorted,
        prefix_sets: TriePrefixSets,
        targets: Vec<(B256, PrefixSet)>,
        split: bool,
    ) -> ProviderResult<ParallelRoot>
    where
        F: DatabaseProviderFactory,
        TX: DbTx,
    {
        let walk_tip = snapshot_tip(tx)?;
        let tasks = Arc::new(StorageRootTasks::new(targets));
        let source = Arc::clone(&tasks) as Arc<dyn StorageRootSource>;
        let runs = &self.runs;
        let (trie, hashed) = layered_factories(tx, runs, current);
        let stored_root_node = split && has_stored_root_node(&trie)?;
        if stored_root_node {
            record_account_walk_fallback(AccountWalkFallback::StoredRootNode);
        }
        let subtries = (split && !stored_root_node).then(|| Subtries::new(&prefix_sets));
        let root = rayon::in_place_scope(|scope| -> ProviderResult<ParallelRoot> {
            let shared = &*tasks;
            for slot in &shared.slots {
                scope.spawn(move |_| {
                    shared.run(slot, || storage_root_task(factory, walk_tip, runs, current, slot))
                });
            }
            let mut composed = None;
            if let Some(subtries) = &subtries {
                let source = &source;
                for _ in 0..rayon::current_num_threads().min(SUBTRIE_HELPERS) {
                    scope.spawn(move |_| {
                        subtrie_helper(factory, walk_tip, runs, current, subtries, source, shared)
                    });
                }
                subtries.work(&trie, &hashed, source, &|error| shared.record_failure(error));
                composed =
                    subtries.finish(&trie, &hashed, source).map_err(TaskError::into_provider)?;
            }
            let root = match composed {
                Some(result) => ParallelRoot { result, composed: true },
                None => {
                    if subtries.is_some() {
                        record_account_walk_fallback(AccountWalkFallback::Shape);
                    }
                    let result = StateRoot::new(&trie, &hashed)
                        .with_prefix_sets(prefix_sets)
                        .with_storage_roots(Arc::clone(&source))
                        .root_with_updates()?;
                    ParallelRoot { result, composed: false }
                }
            };
            shared.claim_unstarted();
            Ok(root)
        });
        tasks.log_failures();
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
}

/// Pool jobs offered to the account walk's subtries besides the calling thread: one per nibble but
/// the one the calling thread takes first.
const SUBTRIE_HELPERS: usize = 15;

/// Whether the account trie holds a node at the empty path. Reth never writes one (its trie
/// updates drop the root path); a walk that found one would skip unchanged nibbles by its hashes,
/// which the split does not reproduce.
fn has_stored_root_node<T: TrieCursorFactory>(trie: &T) -> Result<bool, DatabaseError> {
    Ok(trie.account_trie_cursor()?.seek_exact(Nibbles::default())?.is_some())
}

/// A pool job working the account walk's subtries on its own read-only transaction, unless the
/// calling thread has already claimed them all. A failure leaves its subtries to the calling
/// thread.
fn subtrie_helper<F: DatabaseProviderFactory>(
    factory: &F,
    walk_tip: Option<BlockNumber>,
    runs: &SortedTrieRuns,
    current: &HashedPostStateSorted,
    subtries: &Subtries,
    source: &Arc<dyn StorageRootSource>,
    tasks: &StorageRootTasks,
) {
    if subtries.all_claimed() {
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
        subtries.work(&trie, &hashed, source, &|error| tasks.record_failure(error));
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

/// A block's storage-root tasks: the walk's [`StorageRootSource`].
#[derive(Debug)]
struct StorageRootTasks {
    /// One per target, sorted by hashed address.
    slots: Vec<Slot>,
    /// Failed tasks and the first failure, logged once after the walk.
    failures: Mutex<(usize, Option<TaskError>)>,
}

impl StorageRootTasks {
    /// The tasks for `targets`, sorted by hashed address.
    fn new(targets: Vec<(B256, PrefixSet)>) -> Self {
        let slots = targets
            .into_iter()
            .map(|(hashed_address, prefix_set)| Slot {
                hashed_address,
                prefix_set,
                state: Mutex::new(TaskState::Pending),
                finished: Condvar::new(),
            })
            .collect();
        Self { slots, failures: Mutex::new((0, None)) }
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
        let result = catch_unwind(AssertUnwindSafe(task)).unwrap_or(Err(TaskError::Panicked));
        let root = result.map_err(|error| self.record_failure(error)).ok();
        *slot.state.lock() = TaskState::Done(root);
        slot.finished.notify_all();
    }

    /// Count a failed task, keeping the first failure for the log.
    fn record_failure(&self, error: TaskError) {
        let mut failures = self.failures.lock();
        failures.0 += 1;
        failures.1.get_or_insert(error);
    }

    /// Mark the tasks that have not started as claimed, so they return at once (the walk is done).
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
            let tasks = self.slots.len();
            if matches!(error, TaskError::TipMoved { .. }) {
                error!(target: "engine", %error, count, tasks, "root tasks saw another database tip than the block's root; computed serially");
            } else {
                warn!(target: "engine", %error, count, tasks, "root tasks failed; computed serially");
            }
        }
    }
}

impl StorageRootSource for StorageRootTasks {
    fn storage_root(&self, hashed_address: B256) -> Option<(B256, usize, StorageTrieUpdates)> {
        let index = self.slots.binary_search_by_key(&hashed_address, |s| s.hashed_address).ok()?;
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
    let ParallelRoot { result: parallel, composed: split } = parallel;
    if serial == parallel {
        return serial;
    }
    let (serial_root, parallel_root) = (serial.0, parallel.0);
    let updates_equal = serial.1 == parallel.1;
    if shadow {
        panic!(
            "parallel state root {parallel_root} differs from the serial root {serial_root} \
             (trie updates equal: {updates_equal}, split walk: {split})"
        );
    }
    error!(target: "engine", %serial_root, %parallel_root, updates_equal, split, "parallel state root differs from the serial one; the block uses the serial result");
    RETH_METRICS.parallel_state_root_mismatches_total.increment(1);
    if split {
        record_account_walk_fallback(AccountWalkFallback::Mismatch);
    }
    serial
}

#[cfg(test)]
thread_local! {
    /// Task results the walks on this thread took (tests check the tasks ran).
    static TASK_ROOTS_TAKEN: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Set by a test to make this thread's next sampled block a compared one.
    pub(super) static CHECK_NEXT_SAMPLED_BLOCK: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(test)]
#[path = "parallel_tests.rs"]
mod tests;
