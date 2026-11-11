//! Output-scoped accumulated trie overlay for multi-block consensus output (issue #1301).
//!
//! For blocks 2..N of a multi-block `ConsensusOutput` the parent block is still
//! unpersisted, so reth's `MemoryOverlayStateProvider` re-merges EVERY in-memory
//! ancestor's deltas from scratch on each block's state-root computation
//! (`trie_input()`), deep-copies the merged input (`prepend_self(...clone())`), and
//! re-sorts it - Θ(N² · M) per output, with the merged work discarded after every
//! block because the provider's `OnceLock` cache is cloned empty into each delegated
//! call. TN bypasses that reth-side path (it is NOT fixed upstream): the engine keeps
//! ONE [`OutputTrieOverlay`] per consensus output, geometrically compacts each
//! built block's already-sorted deltas, and drives each block's state root directly
//! against the database transaction with layered in-memory cursors
//! ([`OutputTrieOverlay::layered_root_with_updates`]).
//!
//! Layer precedence (outermost wins, wipes and `None` tombstones shadow inner
//! layers): current block's sorted deltas > newest through oldest runs > database.
//! The cursor factories are self-composable, so stacking replaces reth's
//! merge+clone+re-sort; prefix sets come from the current block's own hashed state
//! only, exactly as in reth's path (ancestors contribute empty prefix sets there).
//!
//! The database layer is the overlay's anchor, and the runs must hold exactly the deltas of
//! the blocks after it. Two anchors exist:
//! - Unpinned ([`OutputTrieOverlay::new`]): every root opens a fresh read-only transaction on the
//!   committed database. This is sound only while nothing commits during the output's builds, which
//!   holds when the engine persists each output before it builds the next.
//! - Pinned ([`RethEnv::pin_output_overlay`]): one read-only transaction is opened when the overlay
//!   is created and every root reads it, whatever commits afterwards. A pipelined engine pins the
//!   overlay before it starts persisting output N, extends it with N's blocks, and builds output
//!   N+1 on it, so N+1's roots read the database as of N-1 with N's deltas layered on top, even
//!   when N commits in the middle of N+1's builds.

use std::{
    cell::Cell,
    sync::Arc,
    time::{Duration, Instant},
};

use reth_chain_state::ComputedTrieData;
use reth_db::{transaction::DbTx, Database};
use reth_node_types::NodeTypesWithDB;
use reth_provider::{
    providers::BlockchainProvider, AccountReader, BlockHashReader, BlockNumReader as _,
    DatabaseProviderFactory as _, HashedPostStateProvider, ProviderError, ProviderResult,
    StateProofProvider, StateProvider, StateRootProvider, StorageRootProvider,
};
use reth_trie::{
    hashed_cursor::HashedPostStateCursorFactory, updates::TrieUpdates, AccountProof,
    HashedPostState, HashedStorage, MultiProof, MultiProofTargets, StateRoot, StorageMultiProof,
    TrieInput,
};
use reth_trie_db::{DatabaseHashedCursorFactory, DatabaseTrieCursorFactory};
use tn_types::{Address, BlockNumHash, B256};

use crate::{error::TnRethResult, traits::TelcoinNode, RethEnv};

mod cursors;
mod sorted_runs;
mod storage_cursor;

#[cfg(test)]
mod tests;

use cursors::RunCursorFactory;
use sorted_runs::SortedTrieRuns;
use storage_cursor::OverlayCursorFactory;

/// Geometrically compacted trie deltas for the blocks built since the overlay's anchor.
///
/// The anchor is the committed database the roots read under the runs (see the module docs).
/// An unpinned overlay ([`Self::new`]) reads the database tip through a fresh transaction per
/// root; that tip is stable because the engine commits nothing while the output builds. A
/// pinned overlay ([`RethEnv::pin_output_overlay`]) reads one snapshot taken when it was
/// created, so a commit during the builds is invisible to its roots and the runs must hold
/// every block after the snapshot, committed since or not. TN has no reorg path, so the runs
/// only ever grow.
///
/// The overlay starts with no runs and is extended after each built block via
/// [`Self::extend_from_block`]. Equal-level runs merge, bounding cumulative merge work by
/// `O(N M log N)` for `N` blocks of at most `M` delta entries. Roots borrow at most `O(log N)`
/// runs directly; they never flatten the accumulated data. Repeated keys can remain in
/// separate runs, with the newest run taking precedence.
#[derive(Debug, Default)]
pub struct OutputTrieOverlay {
    /// Chronological state and trie-node runs, compacted together.
    runs: SortedTrieRuns,
    /// The pinned database snapshot every root reads, or `None` for a fresh transaction per
    /// root on the database tip.
    anchor: Option<PinnedSnapshot>,
}

impl OutputTrieOverlay {
    /// Create an empty, unpinned overlay: each root reads the database tip through a fresh
    /// read-only transaction.
    pub fn new() -> Self {
        Self::default()
    }

    /// The committed block the pinned snapshot was taken at, or `None` for an unpinned
    /// overlay.
    pub fn anchor_block(&self) -> Option<BlockNumHash> {
        self.anchor.as_ref().map(|pinned| pinned.block)
    }

    /// Compute a block's state root and trie updates over this overlay's anchor: the pinned
    /// snapshot when there is one, else a read-only transaction opened now on the database tip.
    ///
    /// See [`Self::layered_root_with_updates`] for the layering.
    pub(crate) fn root_with_updates(
        &self,
        blockchain_provider: &BlockchainProvider<TelcoinNode>,
        current: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        match &self.anchor {
            Some(pinned) => self.layered_root_with_updates(&pinned.tx, current),
            None => blockchain_provider
                .database_provider_ro()
                .and_then(|provider| self.layered_root_with_updates(provider.tx_ref(), current)),
        }
    }

    /// Extend the overlay with a just-built block's sorted trie deltas.
    ///
    /// Shares the block's immutable `Arc`s until compaction is needed. Only adjacent
    /// equal-level runs merge, with newer writes taking precedence. Reth's general
    /// sorted merge allocates a replacement vector, but no full accumulated merge
    /// is performed after every block. Persistence remains output-scoped.
    pub fn extend_from_block(&mut self, trie_data: &ComputedTrieData) {
        self.runs.extend(Arc::clone(&trie_data.hashed_state), Arc::clone(&trie_data.trie_updates));
    }

    /// Feed pre-sorted deltas to the production accumulator for allocation benchmarks.
    ///
    /// Accepts the same shared data as [`Self::extend_from_block`] without requiring
    /// unrelated block metadata in the benchmark fixture.
    #[cfg(feature = "bench-internals")]
    #[doc(hidden)]
    pub fn extend_sorted(
        &mut self,
        state: Arc<reth_trie::HashedPostStateSorted>,
        nodes: Arc<reth_trie::updates::TrieUpdatesSorted>,
    ) {
        self.runs.extend(state, nodes);
    }

    /// Compute a block's state root and trie updates with layered in-memory cursors
    /// over a read-only database transaction.
    ///
    /// Mirrors reth's `DatabaseStateRoot::overlay_root_from_nodes_with_updates` plus
    /// one extra layer for the current block: prefix sets are constructed from the
    /// CURRENT block's hashed state only, the current block's sorted deltas form the
    /// outermost hashed-cursor layer, this overlay's runs form the middle layers,
    /// and the database cursors sit at the bottom. Precedence is
    /// current > newer runs > older runs > database, including destroyed-account (`None`) and
    /// wiped-storage shadowing - pinned by the differential tests in
    /// `tests/it/trie_overlay.rs`.
    /// Storage emptiness is checked across the complete overlay before consulting the
    /// raw database, so newer zeros shadow older values without changing Reth's exact
    /// choice between a deleted-trie marker and individual node removals.
    pub fn layered_root_with_updates<TX: DbTx>(
        &self,
        tx: &TX,
        current: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        let prefix_sets = current.construct_prefix_sets().freeze();
        let current_sorted = current.into_sorted();
        Ok(StateRoot::new(
            RunCursorFactory::new(DatabaseTrieCursorFactory::new(tx), &self.runs),
            OverlayCursorFactory::new(
                HashedPostStateCursorFactory::new(
                    RunCursorFactory::new(DatabaseHashedCursorFactory::new(tx), &self.runs),
                    &current_sorted,
                ),
                DatabaseHashedCursorFactory::new(tx),
                &self.runs,
                &current_sorted,
            ),
        )
        .with_prefix_sets(prefix_sets)
        .root_with_updates()?)
    }
}

impl RethEnv {
    /// Open a read-only snapshot of the committed database now and return an empty overlay
    /// anchored on it: every state root computed over the returned overlay (and whatever is
    /// extended into it) reads this snapshot, whatever commits afterwards.
    ///
    /// The snapshot's block ([`OutputTrieOverlay::anchor_block`]) is the last block committed
    /// when the transaction opened, read inside that transaction (the pipeline checkpoint the
    /// persist advances in the same commit), so it always names the state the snapshot holds.
    /// Cheap (one MDBX read transaction); safe on an async thread. The transaction stays open
    /// until the overlay drops, and MDBX cannot reuse pages freed by later commits until then,
    /// so the caller keeps the overlay for one output at a time.
    ///
    /// # Errors
    ///
    /// A provider error when the transaction cannot be opened or the block it was taken at
    /// cannot be read.
    pub fn pin_output_overlay(&self) -> TnRethResult<OutputTrieOverlay> {
        let provider = self.inner.blockchain_provider.database_provider_ro()?;
        let number = provider.best_block_number()?;
        let hash =
            provider.block_hash(number)?.ok_or(ProviderError::HeaderNotFound(number.into()))?;
        let pinned =
            PinnedSnapshot { tx: provider.into_tx(), block: BlockNumHash::new(number, hash) };
        Ok(OutputTrieOverlay { runs: SortedTrieRuns::default(), anchor: Some(pinned) })
    }

    /// TEST-ONLY: keccak over the AccountsTrie, StoragesTrie, HashedAccounts and HashedStorages
    /// tables of the committed database, in key order, for differential tests between two
    /// nodes.
    ///
    /// Each table contributes its name, then every entry's encoded key and compressed value in
    /// cursor order (duplicate values of a dup-sorted table in their stored order), so two
    /// databases digest equal exactly when these four tables hold the same rows.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn trie_tables_digest(&self) -> TnRethResult<B256> {
        use reth_db::tables;

        let provider = self.inner.blockchain_provider.database_provider_ro()?;
        let tx = provider.tx_ref();
        let mut hasher = alloy::primitives::Keccak256::new();
        digest_table::<tables::AccountsTrie>(tx, &mut hasher)?;
        digest_table::<tables::StoragesTrie>(tx, &mut hasher)?;
        digest_table::<tables::HashedAccounts>(tx, &mut hasher)?;
        digest_table::<tables::HashedStorages>(tx, &mut hasher)?;
        Ok(hasher.finalize())
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
/// over the overlay's anchor (issue #1301): its pinned snapshot, or a fresh read-only
/// database transaction for an unpinned overlay.
pub(crate) struct OverlayRootStateProvider<'a, P> {
    /// The parent-state provider from `state_by_block_hash`, unchanged for all
    /// account/storage/bytecode reads.
    inner: &'a P,
    /// The accumulated overlay: the sorted deltas of every block after its anchor (for an
    /// unpinned overlay, the blocks built so far in this consensus output).
    overlay: &'a OutputTrieOverlay,
    /// Source of the read-only database transaction the layered root runs over when the
    /// overlay is unpinned. The transaction is opened lazily inside the override and lives
    /// only for the root computation; the database tip then equals the overlay anchor for the
    /// whole output because nothing commits while the output builds. A pinned overlay reads
    /// its own snapshot instead and never opens one here.
    blockchain_provider: &'a BlockchainProvider<TelcoinNode>,
    /// Wall time spent in `state_root_with_updates`, read by the block build after `finish`
    /// returns (observability only). A `Cell` is enough: one builder borrows the wrapper on
    /// one thread, and `StateProvider` has no `Send`/`Sync` supertrait.
    root_elapsed: Cell<Duration>,
}

impl<'a, P> OverlayRootStateProvider<'a, P> {
    /// Wrap `inner`, routing state-root computation through `overlay` layered over
    /// `blockchain_provider`'s database.
    pub(crate) fn new(
        inner: &'a P,
        overlay: &'a OutputTrieOverlay,
        blockchain_provider: &'a BlockchainProvider<TelcoinNode>,
    ) -> Self {
        Self { inner, overlay, blockchain_provider, root_elapsed: Cell::new(Duration::ZERO) }
    }

    /// Total wall time of the state-root computations routed through this wrapper (exactly
    /// one per block build).
    pub(crate) fn root_elapsed(&self) -> Duration {
        self.root_elapsed.get()
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
    /// overlay over the overlay's anchor, with prefix sets from the current block only.
    fn state_root_with_updates(
        &self,
        hashed_state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        let start = Instant::now();
        let root = self.overlay.root_with_updates(self.blockchain_provider, hashed_state);
        self.root_elapsed.set(self.root_elapsed.get().saturating_add(start.elapsed()));
        root
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

/// The read-only transaction behind a pinned [`OutputTrieOverlay`], with the committed block it
/// sees as the database tip.
///
/// MDBX runs without thread-local reader slots, so the transaction may be opened on one thread
/// and read on another: a pipelined engine pins on its own task and builds on a blocking one.
struct PinnedSnapshot {
    /// The pinned read-only transaction every root of the overlay reads.
    tx: <<TelcoinNode as NodeTypesWithDB>::DB as Database>::TX,
    /// The last block committed in `tx`'s snapshot.
    block: BlockNumHash,
}

impl std::fmt::Debug for PinnedSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedSnapshot").field("block", &self.block).finish_non_exhaustive()
    }
}

/// Feed table `T`'s name and every row (encoded key, compressed value), in cursor order, to
/// `hasher`.
#[cfg(any(test, feature = "test-utils"))]
fn digest_table<T: reth_db::table::Table>(
    tx: &impl DbTx,
    hasher: &mut alloy::primitives::Keccak256,
) -> TnRethResult<()> {
    use reth_db::{
        cursor::DbCursorRO as _,
        table::{Compress as _, Encode as _},
    };

    hasher.update(T::NAME.as_bytes());
    let mut cursor = tx.cursor_read::<T>().map_err(ProviderError::from)?;
    for row in cursor.walk(None).map_err(ProviderError::from)? {
        let (key, value) = row.map_err(ProviderError::from)?;
        hasher.update(key.encode().as_ref());
        hasher.update(value.compress().as_ref());
    }
    Ok(())
}

#[cfg(test)]
mod pinned_tests {
    //! The pinned anchor: one snapshot per overlay, whatever commits after it was taken.

    use super::*;
    use rand::{rngs::StdRng, RngCore as _, SeedableRng as _};
    use reth_primitives_traits::Account;
    use reth_provider::{DBProvider as _, StateWriter as _, TrieWriter as _};
    use reth_trie::{updates::TrieUpdatesSorted, HashedPostStateSorted};
    use tempfile::TempDir;
    use tn_types::{test_genesis, TaskManager, U256};

    /// A temp chain at genesis and the directory and task manager that keep it alive.
    fn genesis_env() -> eyre::Result<(RethEnv, TempDir, TaskManager)> {
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("pinned overlay tests");
        let chain = Arc::new(test_genesis().into());
        let env = RethEnv::new_for_temp_chain(chain, tmp_dir.path(), &task_manager, None)?;
        Ok((env, tmp_dir, task_manager))
    }

    /// A deterministic 32-byte key.
    fn rand_b256(rng: &mut StdRng) -> B256 {
        let mut buf = [0u8; 32];
        rng.fill_bytes(&mut buf);
        B256::from(buf)
    }

    /// A delta that writes `accounts` fresh accounts with two slots each, overwrites one slot of
    /// every account in `touched`, destroys the first touched account and wipes the second's
    /// storage, so later deltas shadow earlier ones in every way a run can.
    fn rand_delta(rng: &mut StdRng, accounts: usize, touched: &[B256]) -> HashedPostState {
        let account = |rng: &mut StdRng| {
            Some(Account {
                nonce: rng.next_u64(),
                balance: U256::from(rng.next_u64()),
                bytecode_hash: None,
            })
        };
        let mut delta = HashedPostState::default();
        for _ in 0..accounts {
            let address = rand_b256(rng);
            let mut slot = || (rand_b256(rng), U256::from(rng.next_u64() | 1));
            let slots = [slot(), slot()];
            delta.accounts.insert(address, account(rng));
            delta.storages.insert(address, HashedStorage::from_iter(false, slots));
        }
        for (i, address) in touched.iter().enumerate() {
            match i {
                0 => {
                    delta.accounts.insert(*address, None);
                }
                1 => {
                    delta.storages.insert(*address, HashedStorage::from_iter(true, []));
                }
                _ => {
                    let slot = (rand_b256(rng), U256::from(rng.next_u64()));
                    delta.accounts.insert(*address, account(rng));
                    delta.storages.insert(*address, HashedStorage::from_iter(false, [slot]));
                }
            }
        }
        delta
    }

    /// The accounts `delta` writes, in key order.
    fn sorted_accounts(delta: &HashedPostState) -> Vec<B256> {
        let mut accounts: Vec<B256> = delta.accounts.keys().copied().collect();
        accounts.sort();
        accounts
    }

    /// The root and sorted deltas of `delta` over `overlay`, ready to extend an overlay with.
    fn block_trie_data(
        env: &RethEnv,
        overlay: &OutputTrieOverlay,
        delta: &HashedPostState,
    ) -> eyre::Result<(B256, ComputedTrieData)> {
        let (root, updates) =
            overlay.root_with_updates(&env.inner.blockchain_provider, delta.clone())?;
        let trie_data = ComputedTrieData::without_trie_input(
            Arc::new(delta.clone().into_sorted()),
            Arc::new(updates.into_sorted()),
        );
        Ok((root, trie_data))
    }

    /// Commit a block's hashed state and trie updates in a write transaction of their own, as
    /// a persist does.
    fn commit_trie_delta(
        env: &RethEnv,
        state: &HashedPostStateSorted,
        nodes: &TrieUpdatesSorted,
    ) -> eyre::Result<()> {
        let provider_rw = env.inner.blockchain_provider.database_provider_rw()?;
        provider_rw.write_hashed_state(state)?;
        provider_rw.write_trie_updates_sorted(nodes)?;
        provider_rw.commit()?;
        Ok(())
    }

    /// I3: a root over a pinned overlay reads the snapshot taken at the pin. `D1` (the
    /// committing output) is layered over the database as of the pin; committing `D1` between
    /// two roots of `D2` changes neither the root nor the trie updates, and both equal the root
    /// of `D2` over the committed `S0 + D1` with nothing layered.
    #[tokio::test]
    async fn test_pinned_anchor_root_ignores_later_commit() -> eyre::Result<()> {
        let (env, _dir, _tasks) = genesis_env()?;
        let provider = &env.inner.blockchain_provider;
        let genesis = env.lookup_head()?;
        let mut rng = StdRng::seed_from_u64(0x0012_a001);

        // S0 is genesis; D1 is the block whose persist runs while D2's block builds
        let d1 = rand_delta(&mut rng, 6, &[]);
        let (d1_root, d1_trie) = block_trie_data(&env, &OutputTrieOverlay::new(), &d1)?;
        let touched: Vec<B256> = sorted_accounts(&d1).into_iter().take(4).collect();
        let d2 = rand_delta(&mut rng, 3, &touched);

        let mut pinned = env.pin_output_overlay()?;
        assert_eq!(pinned.anchor_block(), Some(genesis.num_hash()), "pinned at the durable tip");
        pinned.extend_from_block(&d1_trie);
        let before = pinned.root_with_updates(provider, d2.clone())?;
        // the same snapshot with nothing layered: D2 over S0 alone, before and after the commit
        let bare = env.pin_output_overlay()?;
        let bare_before = bare.root_with_updates(provider, d2.clone())?;

        commit_trie_delta(&env, &d1_trie.hashed_state, &d1_trie.trie_updates)?;
        // control: a fresh transaction sees the commit (the stored trie now roots at D1)
        let committed = OutputTrieOverlay::new().root_with_updates(provider, Default::default())?;
        assert_eq!(committed.0, d1_root, "the commit is visible to a fresh transaction");

        let after = pinned.root_with_updates(provider, d2.clone())?;
        assert_eq!(before.0, after.0, "a commit after the pin changed the pinned root");
        assert_eq!(before.1, after.1, "a commit after the pin changed the pinned trie updates");

        let fresh = OutputTrieOverlay::new().root_with_updates(provider, d2.clone())?;
        assert_eq!(after.0, fresh.0, "the pinned root differs from D2 over the committed S0 + D1");

        // the snapshot itself never moves: without D1 layered, the pinned root is still D2 over
        // S0, which the commit made different from D2 over the committed database
        let bare_after = bare.root_with_updates(provider, d2)?;
        assert_eq!(bare_before, bare_after, "the pinned snapshot saw a commit made after the pin");
        assert_ne!(bare_after.0, fresh.0, "control: committing D1 changes the root of D2");
        Ok(())
    }

    /// A pinned overlay extended with blocks 1..k roots every later delta exactly as an
    /// unpinned overlay extended with the same blocks over the same, unchanged database.
    #[tokio::test]
    async fn test_seeded_overlay_equals_extended_overlay() -> eyre::Result<()> {
        let (env, _dir, _tasks) = genesis_env()?;
        let provider = &env.inner.blockchain_provider;
        let mut rng = StdRng::seed_from_u64(0x0012_a002);
        let mut pinned = env.pin_output_overlay()?;
        let mut unpinned = OutputTrieOverlay::new();
        let mut written: Vec<B256> = Vec::new();

        for block in 0..6 {
            let touched: Vec<B256> = written.iter().rev().take(3).copied().collect();
            let delta = rand_delta(&mut rng, 4, &touched);
            written.extend(sorted_accounts(&delta));
            let (unpinned_root, trie_data) = block_trie_data(&env, &unpinned, &delta)?;
            let (pinned_root, _) = block_trie_data(&env, &pinned, &delta)?;
            assert_eq!(pinned_root, unpinned_root, "block {block} roots differ");
            unpinned.extend_from_block(&trie_data);
            pinned.extend_from_block(&trie_data);

            // random `current` deltas over both overlays
            for _ in 0..3 {
                let touched: Vec<B256> = written.iter().step_by(3).take(4).copied().collect();
                let current = rand_delta(&mut rng, 2, &touched);
                assert_eq!(
                    pinned.root_with_updates(provider, current.clone())?,
                    unpinned.root_with_updates(provider, current)?,
                    "roots or trie updates differ after block {block}"
                );
            }
        }
        Ok(())
    }

    /// An overlay from `new()` has no anchor and opens a fresh transaction per root, so a commit
    /// between two of its roots is visible to the second, as on today's sequential path.
    #[tokio::test]
    async fn test_unpinned_overlay_keeps_fresh_transaction() -> eyre::Result<()> {
        let (env, _dir, _tasks) = genesis_env()?;
        let provider = &env.inner.blockchain_provider;
        let mut rng = StdRng::seed_from_u64(0x0012_a003);
        let unpinned = OutputTrieOverlay::new();
        assert_eq!(unpinned.anchor_block(), None, "new() is unpinned");

        let d1 = rand_delta(&mut rng, 6, &[]);
        let (_, d1_trie) = block_trie_data(&env, &unpinned, &d1)?;
        let touched: Vec<B256> = sorted_accounts(&d1).into_iter().take(4).collect();
        let d2 = rand_delta(&mut rng, 3, &touched);

        // the root of D2 over S0 + D1, layered: what the second root must read
        let mut layered = env.pin_output_overlay()?;
        layered.extend_from_block(&d1_trie);
        let expected = layered.root_with_updates(provider, d2.clone())?;

        let before = unpinned.root_with_updates(provider, d2.clone())?;
        commit_trie_delta(&env, &d1_trie.hashed_state, &d1_trie.trie_updates)?;
        let after = unpinned.root_with_updates(provider, d2)?;
        assert_ne!(before.0, after.0, "the second root did not see the commit");
        assert_eq!(after.0, expected.0, "the second root is not D2 over the committed S0 + D1");
        Ok(())
    }
}
