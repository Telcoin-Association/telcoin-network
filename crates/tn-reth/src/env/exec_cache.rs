//! Cross-block execution cache: account, storage and bytecode values read or written by earlier
//! blocks, kept across blocks and consensus outputs and anchored at one block hash.
//!
//! Adapted from reth v1.11.4, `crates/engine/tree/src/tree/cached_state.rs` (`CachedStateProvider`,
//! `ExecutionCache::insert_state`, `SavedCache`), licensed MIT OR Apache-2.0, copyright (c)
//! 2022-2025 Reth Contributors. reth-engine-tree is not part of TN's vendored reth, so the code
//! lives here. Upstream later moved it into its own crate, `reth-execution-cache`
//! (`crates/engine/execution-cache`, reth commit 5a66d0064); a full upstream sync of the vendored
//! reth should switch to that crate instead of carrying this copy, after checking each difference
//! listed below.
//!
//! # Where it sits
//!
//! [`RethEnv::build_block_from_batch_payload`] wraps the parent's state provider
//! (`state_by_block_hash(parent)`: the in-memory blocks of the output over the database) in a
//! [`CachedStateProvider`], under the block's own `CachedReads` and revm `State`. A hit skips the
//! in-memory overlay scan and the database read (and, in storage layout v2, the keccak of the key).
//! A miss reads the parent state and fills the entry. After the block builds, its bundle (the exact
//! `BundleState` that `RethEnv::persist_executed_output` writes) is applied with
//! [`ExecutionCache::insert_state`] and the cache is re-anchored at the new block's hash.
//!
//! # Invariant
//!
//! Every entry holds its key's value in the state AT the anchor block hash. The next block uses
//! the cache only when its parent hash equals the anchor; otherwise it starts with an empty cache
//! anchored at its parent.
//!
//! Keying by block hash (not by the persisted tip) is sound because the state at a block hash never
//! changes: the header commits to its post-state root, and TN has no reorgs or fork choice
//! (consensus output is the only source of blocks, see `execution.rs`). Entries come from two
//! places only: misses filled from `state_by_block_hash(parent)`, which is the parent's state by
//! contract, and the block's full bundle, which carries every account, slot and contract the block
//! changed, system calls included (TN's epoch-close and fork writes all commit through revm's
//! `State`, see `evm/block.rs`).
//!
//! What would break it, and why it does not happen on the node path:
//! - a write to the state tables outside a block while a cache is live. Genesis is written in
//!   `RethEnv::new` before any block is built; `db load-state` (`SnapshotRestorer`) runs on its own
//!   env, refuses a datadir past genesis and builds no block; the startup consistency check refuses
//!   to start instead of unwinding state; the finalized-marker heal writes markers only. Tests that
//!   plant state rows by hand must call [`RethEnv::clear_execution_cache`] before building on top
//!   of them.
//! - a state change inside a block that bypasses `State` (it would be missing from the bundle).
//! - two threads using one cache: fixed-cache silently drops an insert into a bucket another thread
//!   holds locked, which would leave the old value. The cache is checked out of the env by one
//!   block build at a time and `insert_state` takes `&mut self`.
//! - a failed block build or persist: the build never checks the cache back in (any error drops
//!   it), and a failed persist clears it.
//!
//! # Differences from upstream
//! - Every miss fills the cache (upstream's prewarm mode); TN runs no prewarming, so the block's
//!   own reads are the warm-up. A missing bytecode is not cached.
//! - A destroyed account that existed at the anchor (EIP-161 removal of an empty account; any
//!   SELFDESTRUCT before Cancun) invalidates the whole cache, code or not: a flat storage cache
//!   cannot drop one account's slots. Upstream invalidates only when the account had code. An
//!   account created and destroyed within the block (the only SELFDESTRUCT since Cancun) loses its
//!   account entry and the entries of its listed slots, and keeps the rest: no other slot of it can
//!   be cached, since it did not exist at the anchor.
//! - Invalidation drops the cache and the next block allocates a new one. fixed-cache 0.1.8's O(1)
//!   epoch clear zeroes the buckets on the first epoch wrap only, so after 2048 clears entries of
//!   an old epoch would read as live again.
//! - Every hash has its top bit set ([`TopBitHash`]): without `Drop` values fixed-cache tags a live
//!   entry with the hash's high bits alone, the same tag 0 as an empty bucket, so a key of all zero
//!   bytes whose hash had zero high bits would read the empty bucket's zeroed value.
//! - No prometheus metrics, no `stats` feature of fixed-cache (it needs a crate TN does not carry),
//!   no usage guard (the env hands the cache to one build at a time). Fixed entry counts instead of
//!   a byte budget split.

use std::{
    hash::{BuildHasher, Hasher},
    sync::atomic::{AtomicU64, Ordering},
};

use alloy::primitives::{map::DefaultHashBuilder, Address, Bytes, StorageKey, StorageValue, B256};
use reth_errors::ProviderResult;
use reth_primitives_traits::{Account, Bytecode};
use reth_provider::{
    AccountReader, BlockHashReader, BytecodeReader, HashedPostStateProvider, StateProofProvider,
    StateProvider, StateRootProvider, StorageRootProvider,
};
use reth_revm::db::BundleState;
use reth_trie::{
    updates::TrieUpdates, AccountProof, HashedPostState, HashedStorage, MultiProof,
    MultiProofTargets, StorageMultiProof, StorageProof, TrieInput,
};
use tracing::{debug, warn};

use super::RethEnv;

/// Bytes one fixed-cache bucket takes for this cache's keys and values: each bucket is aligned to
/// 128 bytes and holds a tag word, the key and the value (fixed-cache's `Bucket`).
pub(crate) const BUCKET_BYTES: usize = 128;

/// Default number of account entries (power of two): 32 MiB of buckets.
///
/// With [`DEFAULT_STORAGE_ENTRIES`], chosen on the exec bench's peak profile (20 blocks of 30M
/// gas an output, storage v2, hit rates over 40 outputs after 40): account / slot hit rates were
/// 0.52 / 0.53 at 2^14 / 2^16 entries (10 MiB), 0.73 / 0.74 at 2^16 / 2^18 (40 MiB), 0.81 / 0.82
/// here (160 MiB), 0.84 / 0.85 at 2^20 / 2^22 (640 MiB) and 0.84 / 0.85 at 2^22 / 2^24 (2.5 GiB).
/// Each table's rate depends on its own size only. Past this point four times the memory saves
/// about 1.1k of the 44k reads an output.
pub(crate) const DEFAULT_ACCOUNT_ENTRIES: usize = 1 << 18;
/// Default number of storage-slot entries (power of two): 128 MiB of buckets.
pub(crate) const DEFAULT_STORAGE_ENTRIES: usize = 1 << 20;
/// Default number of bytecode entries (power of two): 512 KiB of buckets plus the code itself
/// (hit rate 0.99 on the peak profile).
pub(crate) const DEFAULT_CODE_ENTRIES: usize = 1 << 12;

/// A fixed-cache with [`TopBitHash`] and the default config (no epochs, no stats).
type FixedCache<K, V> = fixed_cache::Cache<K, V, TopBitHash>;

/// The entry counts of an [`ExecutionCache`]. Each is a power of two, at least 4 (fixed-cache's
/// minimum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CacheCapacity {
    /// Account entries.
    pub(crate) accounts: usize,
    /// Storage-slot entries.
    pub(crate) storage: usize,
    /// Bytecode entries.
    pub(crate) code: usize,
}

impl Default for CacheCapacity {
    fn default() -> Self {
        Self {
            accounts: DEFAULT_ACCOUNT_ENTRIES,
            storage: DEFAULT_STORAGE_ENTRIES,
            code: DEFAULT_CODE_ENTRIES,
        }
    }
}

impl CacheCapacity {
    /// The default, with `TN_EXEC_CACHE_ACCOUNTS`, `TN_EXEC_CACHE_SLOTS` and
    /// `TN_EXEC_CACHE_CODE` (entry counts) over it in test builds: the knobs of the hit-rate
    /// measurement that chose the defaults.
    pub(crate) fn from_env() -> Self {
        #[allow(unused_mut)]
        let mut capacity = Self::default();
        #[cfg(any(test, feature = "test-utils"))]
        {
            let read = |name: &str, default: usize| {
                std::env::var(name).ok().map_or(default, |v| {
                    v.parse::<usize>().unwrap_or_else(|e| panic!("{name}={v}: {e}"))
                })
            };
            capacity.accounts = read("TN_EXEC_CACHE_ACCOUNTS", capacity.accounts);
            capacity.storage = read("TN_EXEC_CACHE_SLOTS", capacity.storage);
            capacity.code = read("TN_EXEC_CACHE_CODE", capacity.code);
        }
        capacity
    }

    /// Bytes of buckets this capacity allocates (bytecode heap not included).
    pub(crate) const fn bucket_bytes(&self) -> usize {
        (self.accounts + self.storage + self.code) * BUCKET_BYTES
    }
}

/// Hit and miss counts of an [`ExecutionCache`] since it was created.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExecutionCacheStats {
    /// Account reads answered by the cache.
    pub account_hits: u64,
    /// Account reads passed to the parent state.
    pub account_misses: u64,
    /// Storage reads answered by the cache.
    pub storage_hits: u64,
    /// Storage reads passed to the parent state.
    pub storage_misses: u64,
    /// Bytecode reads answered by the cache.
    pub code_hits: u64,
    /// Bytecode reads passed to the parent state.
    pub code_misses: u64,
}

/// The counters behind [`ExecutionCacheStats`]. Atomics so the provider stays `Sync`; only the
/// building thread touches them.
#[derive(Debug, Default)]
struct Counters {
    account_hits: AtomicU64,
    account_misses: AtomicU64,
    storage_hits: AtomicU64,
    storage_misses: AtomicU64,
    code_hits: AtomicU64,
    code_misses: AtomicU64,
}

impl Counters {
    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ExecutionCacheStats {
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
        ExecutionCacheStats {
            account_hits: load(&self.account_hits),
            account_misses: load(&self.account_misses),
            storage_hits: load(&self.storage_hits),
            storage_misses: load(&self.storage_misses),
            code_hits: load(&self.code_hits),
            code_misses: load(&self.code_misses),
        }
    }
}

/// Why [`ExecutionCache::insert_state`] refused a bundle: the cache can no longer be proven to
/// hold the post-block state and must be dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertStateError {
    /// An account that existed at the anchor was destroyed: its storage was wiped, and a flat
    /// storage cache cannot drop one account's slots.
    DestroyedExisting(Address),
    /// A modified, not destroyed account without account info (unrepresentable in revm).
    ModifiedWithoutInfo(Address),
}

/// Account, storage and bytecode values at one block's state (the anchor, held by
/// [`SavedCache`]). See the module docs for the invariant.
#[derive(Debug)]
pub(crate) struct ExecutionCache {
    /// `keccak256(code) -> code`: never stale, only real code is stored.
    code: FixedCache<B256, Bytecode>,
    /// `(address, slot) -> value`, zero for an unset slot.
    storage: FixedCache<(Address, StorageKey), StorageValue>,
    /// `address -> account`, `None` for an account that does not exist.
    accounts: FixedCache<Address, Option<Account>>,
    /// Hit and miss counts.
    counters: Counters,
    /// Verify every hit against the parent state and panic on a mismatch (tests and benches).
    #[cfg(any(test, feature = "test-utils"))]
    shadow: bool,
}

impl ExecutionCache {
    /// An empty cache with `capacity` entries. Allocation is lazy (zeroed pages), so a large
    /// capacity costs memory only as entries fill.
    pub(crate) fn new(capacity: CacheCapacity) -> Self {
        Self {
            code: FixedCache::new(capacity.code, TopBitHash::default()),
            storage: FixedCache::new(capacity.storage, TopBitHash::default()),
            accounts: FixedCache::new(capacity.accounts, TopBitHash::default()),
            counters: Counters::default(),
            #[cfg(any(test, feature = "test-utils"))]
            shadow: shadow_from_env(),
        }
    }

    /// Hit and miss counts since this cache was created.
    pub(crate) fn stats(&self) -> ExecutionCacheStats {
        self.counters.snapshot()
    }

    /// Apply one block's post-execution `bundle`, moving every entry from the parent's state to
    /// the block's.
    ///
    /// Order follows upstream: bytecodes first, then each account's slots, then the account, so an
    /// account entry never names code the cache cannot serve. On `Err` the cache is no longer
    /// known to match the block's state and the caller drops it.
    pub(crate) fn insert_state(&mut self, bundle: &BundleState) -> Result<(), InsertStateError> {
        for (code_hash, bytecode) in &bundle.contracts {
            self.code.insert(*code_hash, Bytecode(bytecode.clone()));
        }

        for (address, account) in &bundle.state {
            // loaded, not changed: every cached value of it is still the block's value
            if account.status.is_not_modified() {
                continue;
            }

            if account.was_destroyed() {
                if account.original_info.is_some() {
                    return Err(InsertStateError::DestroyedExisting(*address));
                }
                // created and destroyed within this block (and maybe created again): its slots
                // were never read from the parent state, where it did not exist, so only the
                // slots the block lists can be cached (and only as zero). Drop them and the
                // account; the next read takes the post-block state from the parent provider.
                self.accounts.remove(address);
                for slot in account.storage.keys() {
                    self.storage.remove(&(*address, StorageKey::from(*slot)));
                }
                continue;
            }

            let Some(info) = &account.info else {
                return Err(InsertStateError::ModifiedWithoutInfo(*address));
            };
            for (slot, value) in &account.storage {
                self.storage.insert((*address, StorageKey::from(*slot)), value.present_value);
            }
            self.accounts.insert(*address, Some(Account::from(info)));
        }
        Ok(())
    }
}

/// An [`ExecutionCache`] holding the state at block `hash`.
#[derive(Debug)]
pub(crate) struct SavedCache {
    /// The anchor: the block whose post-state the cache holds.
    pub(crate) hash: B256,
    /// The cache.
    pub(crate) cache: ExecutionCache,
}

/// The env's cache slot: the saved cache between block builds, and the capacity of a new one.
#[derive(Debug)]
pub(crate) struct ExecutionCacheSlot {
    /// The cache, anchored at the last block built, or `None` (startup, or dropped after a
    /// failure).
    saved: Option<SavedCache>,
    /// Entry counts of a new cache.
    capacity: CacheCapacity,
}

impl ExecutionCacheSlot {
    /// An empty slot (no cache yet) at the default capacity, or the test knobs' (see
    /// [`CacheCapacity::from_env`]).
    pub(crate) fn new() -> Self {
        Self { saved: None, capacity: CacheCapacity::from_env() }
    }
}

impl RethEnv {
    /// Take the saved cache for a block built on `parent`: the saved one when it is anchored at
    /// `parent`, else a new empty one (anchored at `parent` by the caller's use of it).
    ///
    /// The env holds no cache while a block builds: only [`Self::check_in_execution_cache`] puts
    /// one back, so a build that fails at any point leaves none.
    pub(crate) fn check_out_execution_cache(&self, parent: B256) -> ExecutionCache {
        let (saved, capacity) = {
            let mut slot = self.inner.execution_cache.lock();
            (slot.saved.take(), slot.capacity)
        };
        match saved {
            Some(saved) if saved.hash == parent => saved.cache,
            stale => {
                if let Some(stale) = &stale {
                    debug!(target: "engine", anchor = %stale.hash, %parent, "execution cache anchor is not the parent: starting empty");
                }
                drop(stale);
                debug!(target: "engine", %parent, bytes = capacity.bucket_bytes(), "new execution cache");
                ExecutionCache::new(capacity)
            }
        }
    }

    /// Apply the block's `bundle` to `cache` and save it anchored at `block_hash`. A refused
    /// bundle drops the cache instead.
    pub(crate) fn check_in_execution_cache(
        &self,
        mut cache: ExecutionCache,
        block_hash: B256,
        bundle: &BundleState,
    ) {
        if let Err(error) = cache.insert_state(bundle) {
            warn!(target: "engine", ?error, %block_hash, "execution cache dropped: the block's bundle cannot be applied to it");
            return;
        }
        #[cfg(any(test, feature = "test-utils"))]
        print_block_stats(block_hash, &cache);
        self.inner.execution_cache.lock().saved = Some(SavedCache { hash: block_hash, cache });
    }

    /// Drop the saved execution cache: the next block starts empty.
    ///
    /// Called when a persist fails. Anything that writes account, storage or bytecode state outside
    /// a block build must call it too (see the module docs of `env/exec_cache.rs`).
    pub fn clear_execution_cache(&self) {
        let saved = self.inner.execution_cache.lock().saved.take();
        drop(saved);
    }

    /// The saved cache's anchor and its hit and miss counts, or `None` when no cache is saved.
    pub fn execution_cache_stats(&self) -> Option<(B256, ExecutionCacheStats)> {
        let slot = self.inner.execution_cache.lock();
        slot.saved.as_ref().map(|saved| (saved.hash, saved.cache.stats()))
    }

    /// TEST-ONLY: set the entry counts of the next new cache and drop the saved one.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_execution_cache_capacity_for_test(&self, accounts: usize, storage: usize) {
        let mut slot = self.inner.execution_cache.lock();
        slot.capacity = CacheCapacity { accounts, storage, code: 4 };
        slot.saved = None;
    }
}

/// `TN_EXEC_CACHE_SHADOW=1` turns the shadow check on (`0` off); unit tests default to on, other
/// test builds to off.
#[cfg(any(test, feature = "test-utils"))]
fn shadow_from_env() -> bool {
    match std::env::var("TN_EXEC_CACHE_SHADOW").as_deref() {
        Ok("1") => true,
        Ok("0") => false,
        Ok(other) => panic!("TN_EXEC_CACHE_SHADOW={other}: expected 0 or 1"),
        Err(_) => cfg!(test),
    }
}

/// With `TN_EXEC_CACHE_STATS=1`, print the cache's cumulative counts after each block to stderr
/// (the hit-rate measurement's input).
#[cfg(any(test, feature = "test-utils"))]
fn print_block_stats(block_hash: B256, cache: &ExecutionCache) {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if *ON.get_or_init(|| std::env::var("TN_EXEC_CACHE_STATS").is_ok_and(|v| v == "1")) {
        let s = cache.stats();
        eprintln!(
            "exec-cache block={block_hash} account_hits={} account_misses={} storage_hits={} \
             storage_misses={} code_hits={} code_misses={}",
            s.account_hits,
            s.account_misses,
            s.storage_hits,
            s.storage_misses,
            s.code_hits,
            s.code_misses
        );
    }
}

/// A state provider that answers account, storage and bytecode reads from an [`ExecutionCache`]
/// and passes the rest, and every miss, to `inner` (the parent block's state).
#[derive(Debug)]
pub(crate) struct CachedStateProvider<'a, S> {
    /// The parent block's state.
    inner: S,
    /// The cache, anchored at the parent.
    cache: &'a ExecutionCache,
}

impl<'a, S> CachedStateProvider<'a, S> {
    /// Wrap `inner`, the state at `cache`'s anchor.
    pub(crate) const fn new(inner: S, cache: &'a ExecutionCache) -> Self {
        Self { inner, cache }
    }
}

impl<S: StateProvider> CachedStateProvider<'_, S> {
    /// Shadow check: panic if a hit differs from the parent state. Accounts compare as revm sees
    /// them (an empty code hash and no code hash are one), slots with unset as zero.
    #[cfg(any(test, feature = "test-utils"))]
    fn shadow_account(&self, address: &Address, cached: Option<Account>) {
        if self.cache.shadow {
            let norm = |a: Option<Account>| a.map(|a| (a.nonce, a.balance, a.get_bytecode_hash()));
            let fresh = self.inner.basic_account(address).expect("shadow read of an account");
            assert_eq!(
                norm(cached),
                norm(fresh),
                "execution cache served a stale account {address}: cached {cached:?}, state {fresh:?}"
            );
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn shadow_storage(&self, address: Address, slot: StorageKey, cached: StorageValue) {
        if self.cache.shadow {
            let fresh = self
                .inner
                .storage(address, slot)
                .expect("shadow read of a slot")
                .unwrap_or_default();
            assert_eq!(
                cached, fresh,
                "execution cache served a stale slot {address} {slot}: cached {cached}, state {fresh}"
            );
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn shadow_code(&self, code_hash: &B256, cached: &Bytecode) {
        if self.cache.shadow {
            let fresh = self.inner.bytecode_by_hash(code_hash).expect("shadow read of code");
            assert_eq!(
                Some(cached.original_bytes()),
                fresh.map(|code| code.original_bytes()),
                "execution cache served stale code {code_hash}"
            );
        }
    }
}

impl<S: StateProvider> AccountReader for CachedStateProvider<'_, S> {
    fn basic_account(&self, address: &Address) -> ProviderResult<Option<Account>> {
        if let Some(account) = self.cache.accounts.get(address) {
            Counters::bump(&self.cache.counters.account_hits);
            #[cfg(any(test, feature = "test-utils"))]
            self.shadow_account(address, account);
            return Ok(account);
        }
        Counters::bump(&self.cache.counters.account_misses);
        let account = self.inner.basic_account(address)?;
        self.cache.accounts.insert(*address, account);
        Ok(account)
    }
}

impl<S: StateProvider> StateProvider for CachedStateProvider<'_, S> {
    fn storage(
        &self,
        account: Address,
        storage_key: StorageKey,
    ) -> ProviderResult<Option<StorageValue>> {
        if let Some(value) = self.cache.storage.get(&(account, storage_key)) {
            Counters::bump(&self.cache.counters.storage_hits);
            #[cfg(any(test, feature = "test-utils"))]
            self.shadow_storage(account, storage_key, value);
            // an unset slot and a slot set to zero read alike: `None` for both
            return Ok(Some(value).filter(|v| !v.is_zero()));
        }
        Counters::bump(&self.cache.counters.storage_misses);
        let value = self.inner.storage(account, storage_key)?;
        self.cache.storage.insert((account, storage_key), value.unwrap_or_default());
        Ok(value)
    }

    fn storage_by_hashed_key(
        &self,
        address: Address,
        hashed_storage_key: StorageKey,
    ) -> ProviderResult<Option<StorageValue>> {
        self.inner.storage_by_hashed_key(address, hashed_storage_key)
    }
}

impl<S: StateProvider> BytecodeReader for CachedStateProvider<'_, S> {
    fn bytecode_by_hash(&self, code_hash: &B256) -> ProviderResult<Option<Bytecode>> {
        if let Some(code) = self.cache.code.get(code_hash) {
            Counters::bump(&self.cache.counters.code_hits);
            #[cfg(any(test, feature = "test-utils"))]
            self.shadow_code(code_hash, &code);
            return Ok(Some(code));
        }
        Counters::bump(&self.cache.counters.code_misses);
        let code = self.inner.bytecode_by_hash(code_hash)?;
        if let Some(code) = &code {
            self.cache.code.insert(*code_hash, code.clone());
        }
        Ok(code)
    }
}

impl<S: StateRootProvider> StateRootProvider for CachedStateProvider<'_, S> {
    fn state_root(&self, hashed_state: HashedPostState) -> ProviderResult<B256> {
        self.inner.state_root(hashed_state)
    }

    fn state_root_from_nodes(&self, input: TrieInput) -> ProviderResult<B256> {
        self.inner.state_root_from_nodes(input)
    }

    fn state_root_with_updates(
        &self,
        hashed_state: HashedPostState,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        self.inner.state_root_with_updates(hashed_state)
    }

    fn state_root_from_nodes_with_updates(
        &self,
        input: TrieInput,
    ) -> ProviderResult<(B256, TrieUpdates)> {
        self.inner.state_root_from_nodes_with_updates(input)
    }
}

impl<S: StateProofProvider> StateProofProvider for CachedStateProvider<'_, S> {
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

    fn witness(&self, input: TrieInput, target: HashedPostState) -> ProviderResult<Vec<Bytes>> {
        self.inner.witness(input, target)
    }
}

impl<S: StorageRootProvider> StorageRootProvider for CachedStateProvider<'_, S> {
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
    ) -> ProviderResult<StorageProof> {
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

impl<S: BlockHashReader> BlockHashReader for CachedStateProvider<'_, S> {
    fn block_hash(&self, number: alloy::primitives::BlockNumber) -> ProviderResult<Option<B256>> {
        self.inner.block_hash(number)
    }

    fn canonical_hashes_range(
        &self,
        start: alloy::primitives::BlockNumber,
        end: alloy::primitives::BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        self.inner.canonical_hashes_range(start, end)
    }
}

impl<S: HashedPostStateProvider> HashedPostStateProvider for CachedStateProvider<'_, S> {
    fn hashed_post_state(&self, bundle_state: &BundleState) -> HashedPostState {
        self.inner.hashed_post_state(bundle_state)
    }
}

/// The hasher of the cache's tables: alloy's default (foldhash, seeded per process) with the top
/// bit of every hash set, so no live entry's tag is the empty bucket's tag 0 (see the module docs).
#[derive(Debug, Clone, Default)]
pub(crate) struct TopBitHash<S = DefaultHashBuilder>(S);

impl<S: BuildHasher> BuildHasher for TopBitHash<S> {
    type Hasher = TopBitHasher<S::Hasher>;

    fn build_hasher(&self) -> Self::Hasher {
        TopBitHasher(self.0.build_hasher())
    }
}

/// [`TopBitHash`]'s hasher: every write goes to the inner hasher unchanged.
#[derive(Debug)]
pub(crate) struct TopBitHasher<H>(H);

impl<H: Hasher> Hasher for TopBitHasher<H> {
    fn finish(&self) -> u64 {
        self.0.finish() | (1 << 63)
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes)
    }

    fn write_u8(&mut self, i: u8) {
        self.0.write_u8(i)
    }

    fn write_u16(&mut self, i: u16) {
        self.0.write_u16(i)
    }

    fn write_u32(&mut self, i: u32) {
        self.0.write_u32(i)
    }

    fn write_u64(&mut self, i: u64) {
        self.0.write_u64(i)
    }

    fn write_u128(&mut self, i: u128) {
        self.0.write_u128(i)
    }

    fn write_usize(&mut self, i: usize) {
        self.0.write_usize(i)
    }
}

#[cfg(test)]
mod tests;
