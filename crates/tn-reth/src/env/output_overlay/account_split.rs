//! The account-trie walk of a block's root split by the first nibble of the hashed address.
//!
//! The account trie has no stored root node: reth's `TrieUpdates::finalize` drops the empty path,
//! so the walker starts every root by seeking each first nibble's node `[n]`, which carries no
//! root hash and so is never skipped: the serial walk re-reads and re-hashes all sixteen of them,
//! changed or not, and its trie updates hold each one (updated and removed). The sixteen subtries
//! are otherwise independent, and here each is one task: reth's own [`StateRoot`] (walker, node
//! iterator, hash builder, and the storage-root source of the parallel storage roots) over cursor
//! factories that show only the keys starting with that nibble (the account trie cursor and the
//! hashed account cursor; storage cursors pass through), with the prefix sets cut to that nibble.
//! Inside its range the walk sees exactly what the serial walk sees, so its trie updates are the
//! serial walk's updates under `[n]`; the sixteen sets are disjoint and their union is the block's
//! updates.
//!
//! A task's hash builder sees only keys under `[n]`, so its root is the hash of an extension
//! `[n]` over the node at `[n]`. That node is rebuilt from the task's own update for path `[n]`
//! when all its children are hashed (`hash_mask == state_mask`, which holds unless a child is a
//! leaf or an extension: tries of a few thousand accounts or fewer), and checked by re-hashing the
//! extension against the task's root. The block's root is then the hash of the branch over the
//! sixteen nodes, as reth's hash builder computes it. Any other shape (fewer than two non-empty
//! nibbles, a non-branch node at `[n]`, a failed check) is reported as `None` and the caller
//! computes the root as before.
//!
//! # Tasks
//!
//! The calling thread and up to fifteen pool jobs claim the nibbles in order (`parallel.rs` starts
//! the jobs; each opens its own read-only transaction and checks its tip, as the storage-root
//! tasks do). The calling thread then waits for every claimed nibble: a claimed nibble is always
//! running, so the wait never depends on a job the pool has not started. A nibble whose task
//! failed (a database error, a panic) is recomputed on the calling thread's transaction; the split
//! still composes. The subtrie walks take the storage roots from the storage-root tasks like the
//! one-thread walk does.
//!
//! # When the walk runs on one thread
//!
//! [`AccountWalk::OneThread`] (`TN_PARALLEL_ACCOUNT_WALK=0`), or a block with fewer than
//! [`SPLIT_MIN_ACCOUNTS`] changed accounts, runs the walk on the calling thread as before. So does
//! a block the split cannot compose: a node at the empty path in the database (reth never writes
//! one), or a shape `compose` refuses (a small trie: a first nibble's node with a leaf or
//! extension child, or fewer than two non-empty nibbles). These fallbacks are counted by reason in
//! `tn_reth.account_walk_fallbacks_total` ([`crate::metrics::AccountWalkFallback`]), as is a
//! sampled serial check that found the split's result wrong.
//!
//! # Off switch
//!
//! `TN_PARALLEL_ACCOUNT_WALK=0` in the node's environment runs every account walk on one thread
//! (the storage-root tasks stay on); unset or `1` splits. Any other value is logged as an error and
//! also turns the split off. Read once, at the first block, like `TN_PARALLEL_STATE_ROOT`, which
//! turns both off. Tests and benches choose the walk explicitly, never through the variable.

use super::parallel::TaskError;
use crate::metrics::{record_account_walk_fallback, AccountWalkFallback};
use parking_lot::{Condvar, Mutex};
use reth_db::DatabaseError;
use reth_errors::ProviderError;
use reth_trie::{
    hashed_cursor::{HashedCursor, HashedCursorFactory},
    prefix_set::{PrefixSetMut, TriePrefixSets},
    trie_cursor::{TrieCursor, TrieCursorFactory},
    updates::TrieUpdates,
    BranchNodeCompact, BranchNodeRef, ExtensionNodeRef, Nibbles, RlpNode, StateRoot,
    StorageRootSource, TrieMask, EMPTY_ROOT_HASH,
};
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, OnceLock,
    },
};
use tn_types::{keccak256, B256};
use tracing::{debug, error, info};

/// Fewest changed accounts (account prefix-set keys) for which a block's walk is split. Below it
/// the walk is short and the tasks' fixed costs (a read transaction and cursors each) are not
/// worth paying.
pub(crate) const SPLIT_MIN_ACCOUNTS: usize = 64;

/// The split's off switch (module docs).
const SPLIT_ENV: &str = "TN_PARALLEL_ACCOUNT_WALK";

/// How a block's account walk runs when its root uses the parallel tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AccountWalk {
    /// On the calling thread (the off switch).
    OneThread,
    /// Split by first nibble when the block is worth it (this module).
    Split,
}

impl AccountWalk {
    /// The walk `TN_PARALLEL_ACCOUNT_WALK` selects, read once.
    pub(crate) fn from_env() -> Self {
        static WALK: OnceLock<AccountWalk> = OnceLock::new();
        *WALK.get_or_init(|| Self::from_env_value(std::env::var(SPLIT_ENV).ok().as_deref()))
    }

    /// The walk a value of `TN_PARALLEL_ACCOUNT_WALK` selects (`None`: unset).
    pub(crate) fn from_env_value(value: Option<&str>) -> Self {
        match value {
            None | Some("1") => Self::Split,
            Some("0") => {
                info!(target: "engine", "{SPLIT_ENV}=0: the account walk runs on one thread");
                Self::OneThread
            }
            Some(other) => {
                error!(target: "engine", "{SPLIT_ENV}={other}: expected 0 or 1; the account walk runs on one thread");
                Self::OneThread
            }
        }
    }

    /// Whether a block with these prefix sets splits its walk. A split walk that skips a block
    /// with too few changes counts it as a fallback.
    pub(crate) fn splits(self, prefix_sets: &TriePrefixSets) -> bool {
        match self {
            Self::OneThread => false,
            Self::Split if worth_splitting(prefix_sets) => true,
            Self::Split => {
                record_account_walk_fallback(AccountWalkFallback::FewChanges);
                false
            }
        }
    }
}

/// Whether a block with these prefix sets is worth splitting.
pub(crate) fn worth_splitting(prefix_sets: &TriePrefixSets) -> bool {
    let accounts = &prefix_sets.account_prefix_set;
    !accounts.all() && accounts.len() >= SPLIT_MIN_ACCOUNTS
}

/// The sixteen subtrie tasks of one block, claimed in nibble order by the calling thread and
/// any pool threads that start before the work runs out.
#[derive(Debug)]
pub(crate) struct Subtries {
    /// The next unclaimed nibble.
    next: AtomicUsize,
    /// One per first nibble.
    slots: Vec<SubtrieSlot>,
}

/// One nibble's task.
#[derive(Debug)]
struct SubtrieSlot {
    /// The block's prefix sets cut to this nibble.
    prefix_sets: TriePrefixSets,
    /// Where the task stands.
    state: Mutex<SubtrieState>,
    /// Signalled when the task finishes.
    finished: Condvar,
}

/// Where one subtrie task stands.
#[derive(Debug)]
enum SubtrieState {
    /// Not claimed, or claimed and about to run.
    Pending,
    /// Finished: the task's root and trie updates, or `None` when it failed.
    Done(Option<(B256, TrieUpdates)>),
    /// Its result was taken.
    Taken,
}

impl Subtries {
    /// The tasks for `prefix_sets`, cut by first nibble.
    pub(crate) fn new(prefix_sets: &TriePrefixSets) -> Self {
        let mut accounts: [Vec<Nibbles>; 16] = Default::default();
        for key in prefix_sets.account_prefix_set.iter() {
            if let Some(nibble) = key.first() {
                accounts[nibble as usize].push(*key);
            }
        }
        let mut sets: Vec<TriePrefixSets> = accounts
            .into_iter()
            .map(|keys| TriePrefixSets {
                account_prefix_set: PrefixSetMut::from(keys).freeze(),
                ..Default::default()
            })
            .collect();
        for (address, set) in &prefix_sets.storage_prefix_sets {
            sets[first_nibble(address)].storage_prefix_sets.insert(*address, set.clone());
        }
        for address in &prefix_sets.destroyed_accounts {
            sets[first_nibble(address)].destroyed_accounts.insert(*address);
        }
        let slots = sets
            .into_iter()
            .map(|prefix_sets| SubtrieSlot {
                prefix_sets,
                state: Mutex::new(SubtrieState::Pending),
                finished: Condvar::new(),
            })
            .collect();
        Self { next: AtomicUsize::new(0), slots }
    }

    /// Whether every nibble is claimed.
    pub(crate) fn all_claimed(&self) -> bool {
        self.next.load(Ordering::Relaxed) >= self.slots.len()
    }

    /// Claim and compute nibbles over `trie` and `hashed` until none is left. A failed or panicked
    /// task leaves its nibble to [`Self::finish`].
    pub(crate) fn work<T, H>(
        &self,
        trie: &T,
        hashed: &H,
        storage_roots: &Arc<dyn StorageRootSource>,
        failures: &(dyn Fn(TaskError) + Sync),
    ) where
        T: TrieCursorFactory,
        H: HashedCursorFactory,
    {
        loop {
            let index = self.next.fetch_add(1, Ordering::Relaxed);
            let Some(slot) = self.slots.get(index) else { return };
            let result = catch_unwind(AssertUnwindSafe(|| {
                subtrie_root(trie, hashed, index as u8, slot, storage_roots)
            }))
            .unwrap_or(Err(TaskError::Panicked));
            let result = result.map_err(failures).ok();
            *slot.state.lock() = SubtrieState::Done(result);
            slot.finished.notify_all();
        }
    }

    /// Wait for every nibble (all claimed, by [`Self::work`] on the calling thread first), compute
    /// the failed ones over `trie` and `hashed`, and compose the block's root and trie updates.
    /// `None` when the trie's shape is not one the split composes: the caller computes the root
    /// as before.
    pub(crate) fn finish<T, H>(
        &self,
        trie: &T,
        hashed: &H,
        storage_roots: &Arc<dyn StorageRootSource>,
    ) -> Result<Option<(B256, TrieUpdates)>, TaskError>
    where
        T: TrieCursorFactory,
        H: HashedCursorFactory,
    {
        debug_assert!(self.all_claimed());
        let mut results = Vec::with_capacity(self.slots.len());
        for (index, slot) in self.slots.iter().enumerate() {
            let done = {
                let mut state = slot.state.lock();
                loop {
                    match std::mem::replace(&mut *state, SubtrieState::Taken) {
                        SubtrieState::Pending => {
                            *state = SubtrieState::Pending;
                            slot.finished.wait(&mut state);
                        }
                        SubtrieState::Done(done) => break done,
                        SubtrieState::Taken => unreachable!("a subtrie result is taken once"),
                    }
                }
            };
            let done = match done {
                Some(done) => done,
                None => subtrie_root(trie, hashed, index as u8, slot, storage_roots)?,
            };
            results.push(done);
        }
        let composed = compose(results);
        #[cfg(test)]
        let composed = composed.map(test_observed);
        Ok(composed)
    }
}

/// The index of `address`'s first nibble.
fn first_nibble(address: &B256) -> usize {
    (address[0] >> 4) as usize
}

/// Nibble `nibble`'s subtrie: reth's state root over the cursors restricted to it.
fn subtrie_root<T, H>(
    trie: &T,
    hashed: &H,
    nibble: u8,
    slot: &SubtrieSlot,
    storage_roots: &Arc<dyn StorageRootSource>,
) -> Result<(B256, TrieUpdates), TaskError>
where
    T: TrieCursorFactory,
    H: HashedCursorFactory,
{
    Ok(StateRoot::new(Subtrie::new(trie, nibble), Subtrie::new(hashed, nibble))
        .with_prefix_sets(slot.prefix_sets.clone())
        .with_storage_roots(Arc::clone(storage_roots))
        .root_with_updates()
        .map_err(ProviderError::from)?)
}

/// The block's root and trie updates from the sixteen subtries' (in nibble order), or `None` when
/// the shape is not one the split composes.
pub(crate) fn compose(subtries: Vec<(B256, TrieUpdates)>) -> Option<(B256, TrieUpdates)> {
    let mut children = Vec::with_capacity(subtries.len());
    let mut state_mask = TrieMask::default();
    let mut updates = TrieUpdates::default();
    for (nibble, (root, subtrie)) in subtries.into_iter().enumerate() {
        let nibble = nibble as u8;
        if root != EMPTY_ROOT_HASH {
            let Some(child) = child_node(nibble, root, &subtrie) else {
                debug!(target: "tn_reth::account_split", nibble, "subtrie node is not a branch with hashed children; the walk runs on one thread");
                return None;
            };
            children.push(child);
            state_mask.set_bit(nibble);
        }
        updates.account_nodes.extend(subtrie.account_nodes);
        updates.removed_nodes.extend(subtrie.removed_nodes);
        updates.storage_tries.extend(subtrie.storage_tries);
    }
    // One child or none: the root is that child's leaf or extension (or empty), not a branch.
    if children.len() < 2 {
        debug!(target: "tn_reth::account_split", children = children.len(), "root is not a branch; the walk runs on one thread");
        return None;
    }
    let mut buf = Vec::new();
    let node = BranchNodeRef::new(&children, state_mask).rlp(&mut buf);
    let root = node.as_hash().unwrap_or_else(|| keccak256(node.as_slice()));
    debug!(target: "tn_reth::account_split", children = children.len(), "root composed from subtries");
    Some((root, updates))
}

/// The node at path `[nibble]` of the block's trie, rebuilt from the subtrie's update for that
/// path when all its children are hashed, and checked against the subtrie's root (the hash of an
/// extension `[nibble]` over it).
fn child_node(nibble: u8, subtrie_root: B256, subtrie: &TrieUpdates) -> Option<RlpNode> {
    let path = Nibbles::from_nibbles_unchecked([nibble]);
    let BranchNodeCompact { state_mask, hash_mask, hashes, .. } =
        subtrie.account_nodes.get(&path)?;
    if hash_mask != state_mask {
        return None;
    }
    let stack: Vec<RlpNode> = hashes.iter().map(RlpNode::word_rlp).collect();
    let mut buf = Vec::new();
    let node = BranchNodeRef::new(&stack, *state_mask).rlp(&mut buf);
    buf.clear();
    let extension = ExtensionNodeRef::new(&path, &node).rlp(&mut buf);
    let root = extension.as_hash().unwrap_or_else(|| keccak256(extension.as_slice()));
    (root == subtrie_root).then_some(node)
}

/// A cursor factory showing only the account-trie nodes and hashed accounts whose keys start with
/// one nibble; storage cursors pass through.
#[derive(Debug, Clone)]
pub(crate) struct Subtrie<F> {
    /// The full factory.
    inner: F,
    /// The first nibble shown.
    nibble: u8,
}

impl<F> Subtrie<F> {
    /// `inner` restricted to keys starting with `nibble`.
    pub(crate) const fn new(inner: F, nibble: u8) -> Self {
        Self { inner, nibble }
    }
}

impl<F: TrieCursorFactory> TrieCursorFactory for Subtrie<F> {
    type AccountTrieCursor<'a>
        = SubtrieCursor<F::AccountTrieCursor<'a>>
    where
        Self: 'a;
    type StorageTrieCursor<'a>
        = F::StorageTrieCursor<'a>
    where
        Self: 'a;

    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor<'_>, DatabaseError> {
        Ok(SubtrieCursor { inner: self.inner.account_trie_cursor()?, nibble: self.nibble })
    }

    fn storage_trie_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageTrieCursor<'_>, DatabaseError> {
        self.inner.storage_trie_cursor(hashed_address)
    }
}

impl<F: HashedCursorFactory> HashedCursorFactory for Subtrie<F> {
    type AccountCursor<'a>
        = SubtrieCursor<F::AccountCursor<'a>>
    where
        Self: 'a;
    type StorageCursor<'a>
        = F::StorageCursor<'a>
    where
        Self: 'a;

    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor<'_>, DatabaseError> {
        Ok(SubtrieCursor { inner: self.inner.hashed_account_cursor()?, nibble: self.nibble })
    }

    fn hashed_storage_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageCursor<'_>, DatabaseError> {
        self.inner.hashed_storage_cursor(hashed_address)
    }
}

/// A cursor over one first nibble's keys: a seek below them starts at the first, and a key past
/// them reads as the end.
#[derive(Debug)]
pub(crate) struct SubtrieCursor<C> {
    /// The full cursor.
    inner: C,
    /// The first nibble shown.
    nibble: u8,
}

impl<C: TrieCursor> SubtrieCursor<C> {
    /// `entry` when its key starts with the nibble.
    fn shown(
        &self,
        entry: Option<(Nibbles, BranchNodeCompact)>,
    ) -> Option<(Nibbles, BranchNodeCompact)> {
        entry.filter(|(key, _)| key.first() == Some(self.nibble))
    }
}

impl<C: TrieCursor> TrieCursor for SubtrieCursor<C> {
    fn seek_exact(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        if key.first() != Some(self.nibble) {
            return Ok(None);
        }
        self.inner.seek_exact(key)
    }

    fn seek(
        &mut self,
        key: Nibbles,
    ) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        let start = Nibbles::from_nibbles_unchecked([self.nibble]);
        let entry = self.inner.seek(key.max(start))?;
        Ok(self.shown(entry))
    }

    fn next(&mut self) -> Result<Option<(Nibbles, BranchNodeCompact)>, DatabaseError> {
        let entry = self.inner.next()?;
        Ok(self.shown(entry))
    }

    fn current(&mut self) -> Result<Option<Nibbles>, DatabaseError> {
        Ok(self.inner.current()?.filter(|key| key.first() == Some(self.nibble)))
    }

    fn reset(&mut self) {
        self.inner.reset();
    }
}

impl<C: HashedCursor> HashedCursor for SubtrieCursor<C> {
    type Value = C::Value;

    fn seek(&mut self, key: B256) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        let mut start = B256::ZERO;
        start[0] = self.nibble << 4;
        let entry = self.inner.seek(key.max(start))?;
        Ok(entry.filter(|(key, _)| first_nibble(key) == self.nibble as usize))
    }

    fn next(&mut self) -> Result<Option<(B256, Self::Value)>, DatabaseError> {
        let entry = self.inner.next()?;
        Ok(entry.filter(|(key, _)| first_nibble(key) == self.nibble as usize))
    }

    fn reset(&mut self) {
        self.inner.reset();
    }
}

#[cfg(test)]
thread_local! {
    /// Roots this thread composed from subtries (tests check the split ran).
    static SPLITS_COMPOSED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    /// Set by a test to make the next root this thread composes wrong (the checks must catch it).
    pub(super) static CORRUPT_NEXT_COMPOSED_ROOT: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

/// A composed root as tests see it: counted, and made wrong when a test asked for that.
#[cfg(test)]
fn test_observed((root, updates): (B256, TrieUpdates)) -> (B256, TrieUpdates) {
    SPLITS_COMPOSED.with(|n| n.set(n.get() + 1));
    let corrupt = CORRUPT_NEXT_COMPOSED_ROOT.with(|c| c.replace(false));
    (if corrupt { keccak256(root) } else { root }, updates)
}

#[cfg(test)]
#[path = "account_split_tests.rs"]
mod tests;
