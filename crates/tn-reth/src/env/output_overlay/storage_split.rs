//! A hot contract's storage root split by the first nibble of the hashed slot.
//!
//! With the account walk split (`account_split.rs`), a busy block's root waits on its hottest
//! contract's storage root: one storage-root task (`parallel.rs`) walking hundreds of changed slots
//! of a deep trie while the other threads are done. Here such a storage trie is cut like the
//! account trie: sixteen pieces, each reth's own [`StorageRoot`] over cursor factories that show
//! only the slots starting with that nibble (the storage trie cursor and the hashed storage
//! cursor), with the prefix set cut to that nibble. Storage tries have no stored root node either
//! (reth's `StorageTrieUpdates::finalize` drops the empty path), so inside its range a piece's walk
//! sees exactly what the whole walk sees, its trie updates are the whole walk's under `[n]`, and
//! the sixteen pieces' updates are disjoint and their union is the trie's. The root is composed as
//! the account split composes it: each first nibble's node rebuilt from its piece's update for
//! `[n]` (all children hashed), checked by re-hashing the extension `[n]` over it against the
//! piece's root, and hashed into the root branch.
//!
//! A piece's hashed storage cursor answers `is_storage_empty` for the whole storage, so reth's
//! empty-storage short circuit (a deleted trie, no walk) fires only when the whole storage is
//! empty, which is never composed; a nibble whose slots are all gone is walked and records its
//! removed nodes as the whole walk does.
//!
//! # Which tries split
//!
//! A storage-root target (changed storage, not destroyed in the block) whose prefix set is not
//! `all` (a wiped storage's is) and holds at least [`SPLIT_MIN_SLOTS`] changed slots, whose stored
//! trie (the walk's layered view) has
//! no node at the empty path and all sixteen first-nibble nodes stored with every child hashed
//! ([`splittable`]: a big trie). Every other target stays one task.
//!
//! # Tasks
//!
//! All split tries' pieces share one claim index (trie-major). The calling thread and the pool
//! jobs that also take the account subtries (`parallel.rs`) claim pieces until none is left and
//! only then walk: the producers of the hot storage roots run before their consumers. A claimed
//! piece runs to its end on its claimer and waits on nothing. The account walk that reaches a split
//! contract's leaf takes its root from the storage-root source ([`StorageSplits::take`]): it waits
//! for the pieces (all claimed, since its own thread exhausted the index first), composes them and
//! hands the walk the root, the slots walked and the trie updates. `take` never waits for an
//! unclaimed piece whatever the call order: it claims what is left up to its trie's end and leaves
//! it undone. A failed, panicked or unclaimed piece, or a shape `compose` refuses, leaves the
//! whole storage root to the walk, which computes it serially as before.
//!
//! # Fallbacks
//!
//! Counted in `tn_reth.storage_walk_fallbacks_total` ([`crate::metrics::StorageWalkFallback`]): a
//! hot trie the pre-check refused (`small_trie`, not tried), a split that composed nothing
//! (`shape`, paid and then computed serially), a stored root node (`stored_root_node`), and a
//! sampled serial check that found a block with a composed storage root wrong (`mismatch`).
//!
//! # Off switch
//!
//! `TN_PARALLEL_STORAGE_WALK=0` in the node's environment keeps every storage root on one task (the
//! account split and the storage-root tasks stay on); unset or `1` splits. Any other value is
//! logged as an error and also turns the split off. Read once, at the first block, like
//! `TN_PARALLEL_STATE_ROOT`, which turns everything off. Tests and benches choose explicitly.

use super::{
    account_split::{branch_root, child_node, SubtrieCursor},
    parallel::TaskError,
};
use crate::metrics::{record_storage_walk_fallback, StorageWalkFallback};
use parking_lot::{Condvar, Mutex};
use reth_db::{transaction::DbTx, DatabaseError};
use reth_trie::{
    hashed_cursor::{HashedCursorFactory, HashedStorageCursor},
    prefix_set::{PrefixSet, PrefixSetMut},
    trie_cursor::{TrieCursor as _, TrieCursorFactory, TrieStorageCursor},
    updates::StorageTrieUpdates,
    Nibbles, StorageRoot, TrieMask, EMPTY_ROOT_HASH,
};
use reth_trie_db::DatabaseStorageRoot as _;
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicUsize, Ordering},
        OnceLock,
    },
};
use tn_types::B256;
use tracing::{debug, error, info};

/// Fewest changed slots (storage prefix-set keys) for which a trie that passes [`splittable`] is
/// split. A big trie costs about 20 us a changed slot on the exec bench, so a split of this size
/// saves about a millisecond against sixteen pieces' fixed costs (cursors each).
pub(crate) const SPLIT_MIN_SLOTS: usize = 64;

/// Pieces per split trie: one per first nibble.
const PIECES: usize = 16;

/// The split's off switch (module docs).
const SPLIT_ENV: &str = "TN_PARALLEL_STORAGE_WALK";

/// A storage root as the storage-root source hands it to the walk: the root, the slots walked, and
/// the trie updates.
pub(super) type StorageRootResult = (B256, usize, StorageTrieUpdates);

/// How a block's hot storage tries are computed when its root uses the parallel tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageWalk {
    /// One task per storage root (the off switch).
    OneTask,
    /// Hot tries split by first nibble (this module).
    Split,
}

impl StorageWalk {
    /// The walk `TN_PARALLEL_STORAGE_WALK` selects, read once.
    pub(crate) fn from_env() -> Self {
        static WALK: OnceLock<StorageWalk> = OnceLock::new();
        *WALK.get_or_init(|| Self::from_env_value(std::env::var(SPLIT_ENV).ok().as_deref()))
    }

    /// The walk a value of `TN_PARALLEL_STORAGE_WALK` selects (`None`: unset).
    pub(crate) fn from_env_value(value: Option<&str>) -> Self {
        match value {
            None | Some("1") => Self::Split,
            Some("0") => {
                info!(target: "engine", "{SPLIT_ENV}=0: every storage root runs on one task");
                Self::OneTask
            }
            Some(other) => {
                error!(target: "engine", "{SPLIT_ENV}={other}: expected 0 or 1; every storage root runs on one task");
                Self::OneTask
            }
        }
    }

    /// Whether a target with this storage prefix set is a candidate for the split (the shape is
    /// checked separately, [`splittable`]).
    pub(crate) fn wants_split(self, prefix_set: &PrefixSet) -> bool {
        self == Self::Split && !prefix_set.all() && prefix_set.len() >= SPLIT_MIN_SLOTS
    }
}

/// Whether a storage trie passes the split's pre-check, as [`splittable`] reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Splittable {
    /// No stored root node; all sixteen first-nibble nodes stored with every child hashed.
    Yes,
    /// A first-nibble node missing or with a leaf or extension child: a small trie.
    SmallTrie,
    /// A node stored at the empty path, which reth never writes.
    StoredRootNode,
}

/// The pre-check of `hashed_address`'s stored storage trie over `trie` (the walk's view): the shape
/// the split composes, judged before the block's changes (a big trie keeps it).
pub(super) fn splittable<T: TrieCursorFactory>(
    trie: &T,
    hashed_address: B256,
) -> Result<Splittable, DatabaseError> {
    let mut cursor = trie.storage_trie_cursor(hashed_address)?;
    if cursor.seek_exact(Nibbles::default())?.is_some() {
        return Ok(Splittable::StoredRootNode);
    }
    for nibble in 0..PIECES as u8 {
        match cursor.seek_exact(Nibbles::from_nibbles_unchecked([nibble]))? {
            Some((_, node)) if node.hash_mask == node.state_mask => {}
            _ => return Ok(Splittable::SmallTrie),
        }
    }
    Ok(Splittable::Yes)
}

/// A block's split storage tries and their pieces.
#[derive(Debug, Default)]
pub(super) struct StorageSplits {
    /// The split tries, sorted by hashed address.
    tries: Vec<SplitTrie>,
    /// The next unclaimed piece: trie `next / PIECES`, nibble `next % PIECES`.
    next: AtomicUsize,
    /// Storage roots composed from pieces (taken by the walk).
    composed: AtomicUsize,
    /// Split tries whose pieces composed nothing (counted on the calling thread after the walk,
    /// [`Self::record_fallbacks`]).
    refused: AtomicUsize,
    /// Set by a test to make the next composed root wrong (the checks must catch it).
    #[cfg(test)]
    corrupt: std::sync::atomic::AtomicBool,
}

/// One split trie.
#[derive(Debug)]
struct SplitTrie {
    /// The account.
    hashed_address: B256,
    /// One per first nibble.
    pieces: Vec<Piece>,
}

/// One first nibble's piece of a split trie.
#[derive(Debug)]
struct Piece {
    /// The trie's storage prefix set cut to this nibble.
    prefix_set: PrefixSet,
    /// Where the piece stands.
    state: Mutex<PieceState>,
    /// Signalled when the piece finishes.
    finished: Condvar,
}

/// Where one piece stands.
#[derive(Debug)]
enum PieceState {
    /// Not finished (claimed or not: the claim index says which).
    Pending,
    /// Finished: the piece's result, or `None` when it failed or was never computed.
    Done(Option<StorageRootResult>),
    /// Its result was taken.
    Taken,
}

impl StorageSplits {
    /// The pieces of `tries` (hashed address and storage prefix set), sorted by hashed address.
    pub(super) fn new(mut tries: Vec<(B256, PrefixSet)>) -> Self {
        tries.sort_unstable_by_key(|(address, _)| *address);
        let tries = tries
            .into_iter()
            .map(|(hashed_address, prefix_set)| {
                let mut keys: [Vec<Nibbles>; PIECES] = Default::default();
                for key in prefix_set.iter() {
                    if let Some(nibble) = key.first() {
                        keys[nibble as usize].push(*key);
                    }
                }
                let pieces = keys
                    .into_iter()
                    .map(|keys| Piece {
                        prefix_set: PrefixSetMut::from(keys).freeze(),
                        state: Mutex::new(PieceState::Pending),
                        finished: Condvar::new(),
                    })
                    .collect();
                SplitTrie { hashed_address, pieces }
            })
            .collect();
        let splits = Self { tries, ..Default::default() };
        #[cfg(test)]
        splits
            .corrupt
            .store(CORRUPT_NEXT_STORAGE_SPLIT.with(|c| c.replace(false)), Ordering::Relaxed);
        splits
    }

    /// Whether no trie is split.
    pub(super) fn is_empty(&self) -> bool {
        self.tries.is_empty()
    }

    /// The pieces in all.
    pub(super) fn pieces(&self) -> usize {
        self.tries.len() * PIECES
    }

    /// Whether every piece is claimed.
    pub(super) fn all_claimed(&self) -> bool {
        self.next.load(Ordering::Relaxed) >= self.pieces()
    }

    /// Storage roots composed so far.
    pub(super) fn composed(&self) -> usize {
        self.composed.load(Ordering::Relaxed)
    }

    /// Count the split tries whose pieces composed nothing as `shape` fallbacks. Called by the
    /// calling thread once the walk is done (a pool thread may have taken them).
    pub(super) fn record_fallbacks(&self) {
        for _ in 0..self.refused.load(Ordering::Relaxed) {
            record_storage_walk_fallback(StorageWalkFallback::Shape);
        }
    }

    /// Claim and compute pieces over `trie` and `hashed` (`tx`'s, which only supplies reth's
    /// storage-root constructor) until none is left. A failed or panicked piece is reported to
    /// `failures` and leaves its trie to the walk.
    pub(super) fn work<TX, T, H>(
        &self,
        tx: &TX,
        trie: &T,
        hashed: &H,
        failures: &(dyn Fn(TaskError) + Sync),
    ) where
        TX: DbTx,
        T: TrieCursorFactory,
        H: HashedCursorFactory,
    {
        loop {
            let index = self.next.fetch_add(1, Ordering::Relaxed);
            let Some(split) = self.tries.get(index / PIECES) else { return };
            let nibble = (index % PIECES) as u8;
            let piece = &split.pieces[nibble as usize];
            let result = catch_unwind(AssertUnwindSafe(|| {
                piece_root(tx, trie, hashed, split.hashed_address, nibble, &piece.prefix_set)
            }))
            .unwrap_or(Err(TaskError::Panicked));
            let result = result.map_err(failures).ok();
            *piece.state.lock() = PieceState::Done(result);
            piece.finished.notify_all();
        }
    }

    /// The storage root of `hashed_address` composed from its pieces: `None` when its trie is not
    /// split, `Some(None)` when the walk must compute it (a piece failed or was never computed, a
    /// shape `compose` refuses, or the result was already taken).
    pub(super) fn take(&self, hashed_address: B256) -> Option<Option<StorageRootResult>> {
        let index = self.tries.binary_search_by_key(&hashed_address, |t| t.hashed_address).ok()?;
        let end = (index + 1) * PIECES;
        // Never wait for an unclaimed piece: claim what is left up to this trie's end and leave
        // it undone (its trie is then computed by the walk).
        let claimed = self.next.fetch_max(end, Ordering::Relaxed);
        for unclaimed in claimed..end {
            let piece = &self.tries[unclaimed / PIECES].pieces[unclaimed % PIECES];
            *piece.state.lock() = PieceState::Done(None);
            piece.finished.notify_all();
        }
        let mut results = Vec::with_capacity(PIECES);
        for piece in &self.tries[index].pieces {
            let mut state = piece.state.lock();
            loop {
                match std::mem::replace(&mut *state, PieceState::Taken) {
                    PieceState::Pending => {
                        *state = PieceState::Pending;
                        piece.finished.wait(&mut state);
                    }
                    PieceState::Done(Some(result)) => {
                        results.push(result);
                        break;
                    }
                    PieceState::Done(None) | PieceState::Taken => break,
                }
            }
        }
        if results.len() < PIECES {
            debug!(target: "tn_reth::storage_split", %hashed_address, pieces = results.len(), "storage pieces missing; the walk computes the storage root");
            return Some(None);
        }
        let Some((root, walked, updates)) = compose(results) else {
            self.refused.fetch_add(1, Ordering::Relaxed);
            return Some(None);
        };
        self.composed.fetch_add(1, Ordering::Relaxed);
        #[cfg(test)]
        let root = if self.corrupt.swap(false, Ordering::Relaxed) {
            tn_types::keccak256(root)
        } else {
            root
        };
        Some(Some((root, walked, updates)))
    }
}

/// Nibble `nibble`'s piece of `hashed_address`'s storage root: reth's storage root over the storage
/// cursors restricted to it.
fn piece_root<TX, T, H>(
    tx: &TX,
    trie: &T,
    hashed: &H,
    hashed_address: B256,
    nibble: u8,
    prefix_set: &PrefixSet,
) -> Result<StorageRootResult, TaskError>
where
    TX: DbTx,
    T: TrieCursorFactory,
    H: HashedCursorFactory,
{
    // Built from reth-trie-db's constructor, which supplies the metrics argument when reth-trie's
    // `metrics` feature is on, then given the restricted factories.
    Ok(StorageRoot::from_tx_hashed(tx, hashed_address)
        .with_trie_cursor_factory(StoragePiece::new(trie, nibble))
        .with_hashed_cursor_factory(StoragePiece::new(hashed, nibble))
        .with_prefix_set(prefix_set.clone())
        .root_with_updates()
        .map_err(DatabaseError::from)?)
}

/// A split trie's storage root, trie updates and slots walked from its sixteen pieces' (in nibble
/// order), or `None` when the shape is not one the split composes.
pub(super) fn compose(pieces: Vec<StorageRootResult>) -> Option<StorageRootResult> {
    let mut children = Vec::with_capacity(PIECES);
    let mut state_mask = TrieMask::default();
    let mut walked = 0;
    let mut updates = StorageTrieUpdates::default();
    for (nibble, (root, slots, piece)) in pieces.into_iter().enumerate() {
        // Only reth's empty-storage short circuit marks a piece deleted: the whole storage is
        // empty.
        if piece.is_deleted {
            debug!(target: "tn_reth::storage_split", "storage emptied; the walk computes the storage root");
            return None;
        }
        let nibble = nibble as u8;
        if root != EMPTY_ROOT_HASH {
            let path = Nibbles::from_nibbles_unchecked([nibble]);
            let Some(child) = child_node(&path, root, piece.storage_nodes.get(&path)) else {
                debug!(target: "tn_reth::storage_split", nibble, "storage piece node is not a branch with hashed children; the walk computes the storage root");
                return None;
            };
            children.push(child);
            state_mask.set_bit(nibble);
        }
        walked += slots;
        updates.storage_nodes.extend(piece.storage_nodes);
        updates.removed_nodes.extend(piece.removed_nodes);
    }
    // One child or none: the root is that child's leaf or extension (or empty), not a branch.
    if children.len() < 2 {
        debug!(target: "tn_reth::storage_split", children = children.len(), "storage root is not a branch; the walk computes the storage root");
        return None;
    }
    debug!(target: "tn_reth::storage_split", children = children.len(), "storage root composed from pieces");
    Some((branch_root(&children, state_mask), walked, updates))
}

/// A cursor factory showing only the storage-trie nodes and hashed slots whose keys start with one
/// nibble; account cursors pass through (a storage root never opens one).
#[derive(Debug, Clone)]
pub(super) struct StoragePiece<F> {
    /// The full factory.
    inner: F,
    /// The first nibble shown.
    nibble: u8,
}

impl<F> StoragePiece<F> {
    /// `inner`'s storage cursors restricted to keys starting with `nibble`.
    pub(super) const fn new(inner: F, nibble: u8) -> Self {
        Self { inner, nibble }
    }
}

impl<F: TrieCursorFactory> TrieCursorFactory for StoragePiece<F> {
    type AccountTrieCursor<'a>
        = F::AccountTrieCursor<'a>
    where
        Self: 'a;
    type StorageTrieCursor<'a>
        = SubtrieCursor<F::StorageTrieCursor<'a>>
    where
        Self: 'a;

    fn account_trie_cursor(&self) -> Result<Self::AccountTrieCursor<'_>, DatabaseError> {
        self.inner.account_trie_cursor()
    }

    fn storage_trie_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageTrieCursor<'_>, DatabaseError> {
        Ok(SubtrieCursor::new(self.inner.storage_trie_cursor(hashed_address)?, self.nibble))
    }
}

impl<F: HashedCursorFactory> HashedCursorFactory for StoragePiece<F> {
    type AccountCursor<'a>
        = F::AccountCursor<'a>
    where
        Self: 'a;
    type StorageCursor<'a>
        = SubtrieCursor<F::StorageCursor<'a>>
    where
        Self: 'a;

    fn hashed_account_cursor(&self) -> Result<Self::AccountCursor<'_>, DatabaseError> {
        self.inner.hashed_account_cursor()
    }

    fn hashed_storage_cursor(
        &self,
        hashed_address: B256,
    ) -> Result<Self::StorageCursor<'_>, DatabaseError> {
        Ok(SubtrieCursor::new(self.inner.hashed_storage_cursor(hashed_address)?, self.nibble))
    }
}

impl<C: TrieStorageCursor> TrieStorageCursor for SubtrieCursor<C> {
    fn set_hashed_address(&mut self, hashed_address: B256) {
        self.inner_mut().set_hashed_address(hashed_address);
    }
}

impl<C: HashedStorageCursor> HashedStorageCursor for SubtrieCursor<C> {
    /// Whether the WHOLE storage is empty, not this nibble's slots (module docs).
    fn is_storage_empty(&mut self) -> Result<bool, DatabaseError> {
        self.inner_mut().is_storage_empty()
    }

    fn set_hashed_address(&mut self, hashed_address: B256) {
        self.inner_mut().set_hashed_address(hashed_address);
    }
}

#[cfg(test)]
thread_local! {
    /// Set by a test to make the next block's first composed storage root wrong (the checks must
    /// catch it). Read on the calling thread when the block's splits are built.
    pub(super) static CORRUPT_NEXT_STORAGE_SPLIT: std::cell::Cell<bool> =
        const { std::cell::Cell::new(false) };
}

#[cfg(test)]
#[path = "storage_split_tests.rs"]
mod tests;
