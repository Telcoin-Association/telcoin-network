//! Output-scoped accumulated trie overlay for multi-block consensus output (issue #1301).
//!
//! For blocks 2..N of a multi-block `ConsensusOutput` the parent block is still
//! unpersisted, so reth's `MemoryOverlayStateProvider` re-merges EVERY in-memory
//! ancestor's deltas from scratch on each block's state-root computation
//! (`trie_input()`), deep-copies the merged input (`prepend_self(...clone())`), and
//! re-sorts it - Θ(N² · M) per output, with the merged work discarded after every
//! block because the provider's `OnceLock` cache is cloned empty into each delegated
//! call. TN bypasses that reth-side path (it is NOT fixed upstream): the engine keeps
//! ONE [`OutputTrieOverlay`] per consensus output, extends it in place with each
//! built block's already-sorted deltas, and drives each block's state root directly
//! against the database transaction with layered in-memory cursors
//! ([`OutputTrieOverlay::layered_root_with_updates`]).
//!
//! Layer precedence (outermost wins, wipes and `None` tombstones shadow inner
//! layers): current block's sorted deltas > accumulated ancestor overlay > database.
//! The cursor factories are self-composable, so stacking replaces reth's
//! merge+clone+re-sort; prefix sets come from the current block's own hashed state
//! only, exactly as in reth's path (ancestors contribute empty prefix sets there).

use std::sync::Arc;

use reth_chain_state::ComputedTrieData;
use reth_db::transaction::DbTx;
use reth_provider::{
    providers::BlockchainProvider, AccountReader, BlockHashReader, DatabaseProviderFactory as _,
    HashedPostStateProvider, ProviderResult, StateProofProvider, StateProvider, StateRootProvider,
    StorageRootProvider,
};
use reth_trie::{
    hashed_cursor::HashedPostStateCursorFactory,
    trie_cursor::InMemoryTrieCursorFactory,
    updates::{TrieUpdates, TrieUpdatesSorted},
    AccountProof, HashedPostState, HashedPostStateSorted, HashedStorage, MultiProof,
    MultiProofTargets, StateRoot, StorageMultiProof, TrieInput,
};
use reth_trie_db::{DatabaseHashedCursorFactory, DatabaseTrieCursorFactory};
use tn_types::{Address, B256};

use crate::traits::TelcoinNode;

/// Accumulated sorted trie deltas of the blocks built so far within ONE consensus output.
///
/// Starts empty at the beginning of `execute_consensus_output` (the database tip is the
/// overlay's anchor: TN persistence is strictly sequential and happens only after the
/// whole output builds, and no reorg path exists) and is extended in place after each
/// built block via [`Self::extend_from_block`]. The extension is one linear merge pass
/// per structure (`extend_ref_and_sort`), so the accumulated size is bounded by the
/// DISTINCT keys touched across the output rather than the sum of per-block delta sizes.
#[derive(Debug, Default)]
pub struct OutputTrieOverlay {
    /// Accumulated ancestor hashed-state deltas, sorted and merged in place.
    state: Arc<HashedPostStateSorted>,
    /// Accumulated ancestor trie-node updates, sorted and merged in place.
    nodes: Arc<TrieUpdatesSorted>,
}

impl OutputTrieOverlay {
    /// Create an empty overlay anchored at the current database tip.
    pub fn new() -> Self {
        Self::default()
    }

    /// Extend the overlay in place with a just-built block's sorted trie deltas.
    ///
    /// Uses `Arc::make_mut` + `extend_ref_and_sort` on both structures: one linear
    /// merge-insert pass each, no clone of the accumulated data and no re-sort. The
    /// `Arc`s are the same ones carried by the block's `ComputedTrieData`, so nothing
    /// is recomputed here. Call this only after every borrow of the overlay taken for
    /// the block's root computation is dropped - an outstanding `Arc` clone would turn
    /// `Arc::make_mut` into a deep copy.
    pub fn extend_from_block(&mut self, trie_data: &ComputedTrieData) {
        Arc::make_mut(&mut self.state).extend_ref_and_sort(&trie_data.hashed_state);
        Arc::make_mut(&mut self.nodes).extend_ref_and_sort(&trie_data.trie_updates);
    }

    /// Compute a block's state root and trie updates with layered in-memory cursors
    /// over a read-only database transaction.
    ///
    /// Mirrors reth's `DatabaseStateRoot::overlay_root_from_nodes_with_updates` plus
    /// one extra layer for the current block: prefix sets are constructed from the
    /// CURRENT block's hashed state only, the current block's sorted deltas form the
    /// outermost hashed-cursor layer, this overlay's accumulated state and nodes form
    /// the middle layers, and the database cursors sit at the bottom. Precedence is
    /// current > accumulated > database, including destroyed-account (`None`) and
    /// wiped-storage shadowing - pinned by the differential tests in
    /// `tests/it/trie_overlay.rs`.
    pub fn layered_root_with_updates<TX: DbTx>(
        &self,
        tx: &TX,
        current: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        let prefix_sets = current.construct_prefix_sets().freeze();
        let current_sorted = current.into_sorted();
        Ok(StateRoot::new(
            InMemoryTrieCursorFactory::new(DatabaseTrieCursorFactory::new(tx), self.nodes.as_ref()),
            HashedPostStateCursorFactory::new(
                HashedPostStateCursorFactory::new(
                    DatabaseHashedCursorFactory::new(tx),
                    self.state.as_ref(),
                ),
                &current_sorted,
            ),
        )
        .with_prefix_sets(prefix_sets)
        .root_with_updates()?)
    }
}

/// Thin [`StateProvider`] wrapper that swaps ONLY the state-root computation.
///
/// Reth's `BasicBlockBuilder::finish` calls exactly two provider methods on its
/// state argument: `hashed_post_state(&bundle_state)` and
/// `state_root_with_updates(hashed_state)`. This wrapper delegates EVERYTHING -
/// including `hashed_post_state` (key-hasher identity must stay with the inner
/// provider) and the other three `StateRootProvider` methods, which `finish` never
/// calls and whose inner memory-overlay implementations are semantically identical,
/// just quadratic - to the inner provider, and overrides only
/// `state_root_with_updates` with [`OutputTrieOverlay::layered_root_with_updates`]
/// over a fresh read-only database transaction (issue #1301).
pub(crate) struct OverlayRootStateProvider<'a, P> {
    /// The parent-state provider from `state_by_block_hash`, unchanged for all
    /// account/storage/bytecode reads.
    inner: &'a P,
    /// The output-scoped accumulated overlay: the sorted deltas of every block built
    /// so far in this consensus output (empty for the first block).
    overlay: &'a OutputTrieOverlay,
    /// Source of the read-only database transaction the layered root runs over. The
    /// transaction is opened lazily inside the override and lives only for the root
    /// computation; the database tip equals the overlay anchor for the whole output
    /// because TN persists strictly after the output's builds and has no reorg path.
    blockchain_provider: &'a BlockchainProvider<TelcoinNode>,
}

impl<'a, P> OverlayRootStateProvider<'a, P> {
    /// Wrap `inner`, routing state-root computation through `overlay` layered over
    /// `blockchain_provider`'s database.
    pub(crate) fn new(
        inner: &'a P,
        overlay: &'a OutputTrieOverlay,
        blockchain_provider: &'a BlockchainProvider<TelcoinNode>,
    ) -> Self {
        Self { inner, overlay, blockchain_provider }
    }
}

/// Delegated 1:1 to the inner provider.
impl<P: StateProvider> BlockHashReader for OverlayRootStateProvider<'_, P> {
    fn block_hash(&self, number: u64) -> ProviderResult<Option<B256>> {
        self.inner.block_hash(number)
    }

    fn canonical_hashes_range(&self, start: u64, end: u64) -> ProviderResult<Vec<B256>> {
        self.inner.canonical_hashes_range(start, end)
    }
}

/// Delegated 1:1 to the inner provider.
impl<P: StateProvider> AccountReader for OverlayRootStateProvider<'_, P> {
    fn basic_account(
        &self,
        address: &Address,
    ) -> ProviderResult<Option<reth_primitives_traits::Account>> {
        self.inner.basic_account(address)
    }
}

/// Delegated 1:1 to the inner provider.
impl<P: StateProvider> reth_provider::BytecodeReader for OverlayRootStateProvider<'_, P> {
    fn bytecode_by_hash(
        &self,
        code_hash: &B256,
    ) -> ProviderResult<Option<reth_primitives_traits::Bytecode>> {
        self.inner.bytecode_by_hash(code_hash)
    }
}

/// Delegated 1:1 to the inner provider: the hashed post-state MUST come from the
/// inner provider so the key-hasher identity used for the deltas matches the one
/// used for every read.
impl<P: StateProvider> HashedPostStateProvider for OverlayRootStateProvider<'_, P> {
    fn hashed_post_state(&self, bundle_state: &reth_revm::db::BundleState) -> HashedPostState {
        self.inner.hashed_post_state(bundle_state)
    }
}

/// Only `state_root_with_updates` - the one method `BasicBlockBuilder::finish`
/// calls - is overridden; the other three delegate to the inner memory-overlay
/// path, which is semantically identical (just quadratic in ancestor count).
impl<P: StateProvider> StateRootProvider for OverlayRootStateProvider<'_, P> {
    fn state_root(&self, hashed_state: HashedPostState) -> ProviderResult<B256> {
        self.inner.state_root(hashed_state)
    }

    fn state_root_from_nodes(&self, input: TrieInput) -> ProviderResult<B256> {
        self.inner.state_root_from_nodes(input)
    }

    /// The #1301 layered root: current-block deltas over the accumulated output
    /// overlay over the database, with prefix sets from the current block only.
    fn state_root_with_updates(
        &self,
        hashed_state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        let provider = self.blockchain_provider.database_provider_ro()?;
        self.overlay.layered_root_with_updates(provider.tx_ref(), hashed_state)
    }

    fn state_root_from_nodes_with_updates(
        &self,
        input: TrieInput,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        self.inner.state_root_from_nodes_with_updates(input)
    }
}

/// Delegated 1:1 to the inner provider.
impl<P: StateProvider> StorageRootProvider for OverlayRootStateProvider<'_, P> {
    fn storage_root(
        &self,
        address: Address,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<B256> {
        self.inner.storage_root(address, hashed_storage)
    }

    fn storage_proof(
        &self,
        address: Address,
        slot: B256,
        hashed_storage: HashedStorage,
    ) -> ProviderResult<reth_trie::StorageProof> {
        self.inner.storage_proof(address, slot, hashed_storage)
    }

    fn storage_multiproof(
        &self,
        address: Address,
        slots: &[B256],
        hashed_storage: HashedStorage,
    ) -> ProviderResult<StorageMultiProof> {
        self.inner.storage_multiproof(address, slots, hashed_storage)
    }
}

/// Delegated 1:1 to the inner provider.
impl<P: StateProvider> StateProofProvider for OverlayRootStateProvider<'_, P> {
    fn proof(
        &self,
        input: TrieInput,
        address: Address,
        slots: &[B256],
    ) -> ProviderResult<AccountProof> {
        self.inner.proof(input, address, slots)
    }

    fn multiproof(
        &self,
        input: TrieInput,
        targets: MultiProofTargets,
    ) -> ProviderResult<MultiProof> {
        self.inner.multiproof(input, targets)
    }

    fn witness(
        &self,
        input: TrieInput,
        target: HashedPostState,
    ) -> ProviderResult<Vec<alloy::primitives::Bytes>> {
        self.inner.witness(input, target)
    }
}

/// Delegated 1:1 to the inner provider (the defaulted account helpers resolve
/// through the delegated `basic_account`/`bytecode_by_hash`).
impl<P: StateProvider> StateProvider for OverlayRootStateProvider<'_, P> {
    fn storage(
        &self,
        account: Address,
        storage_key: alloy::primitives::StorageKey,
    ) -> ProviderResult<Option<alloy::primitives::StorageValue>> {
        self.inner.storage(account, storage_key)
    }

    fn storage_by_hashed_key(
        &self,
        address: Address,
        hashed_storage_key: alloy::primitives::StorageKey,
    ) -> ProviderResult<Option<alloy::primitives::StorageValue>> {
        self.inner.storage_by_hashed_key(address, hashed_storage_key)
    }
}
