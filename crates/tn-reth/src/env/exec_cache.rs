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
//! Every block build ([`RethEnv::build_block_from_batch_payload`], and the engine's
//! [`RethEnv::build_block_from_recovered_payload`]) wraps the parent's state provider
//! (`state_by_block_hash(parent)`: the in-memory blocks of the output over the database) in a
//! [`CachedStateProvider`], under the block's own `CachedReads` and revm `State`. A hit skips the
//! in-memory overlay scan and the database read (and, in storage layout v2, the keccak of the key).
//! A miss reads the parent state and fills the entry. After the block builds, its bundle (the exact
//! `BundleState` that `RethEnv::persist_executed_output` writes) is applied with
//! [`ExecutionCache::insert_state`] and the cache is re-anchored at the new block's hash.
//!
//! # Invariant
//!
//! Every entry holds its key's value in the state AT the anchor block hash, except the storage
//! entries of a wiped account (below), which are never read. The next block uses the cache only
//! when its parent hash equals the anchor; otherwise it starts with an empty cache anchored at its
//! parent.
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
//! - a failed block build or persist: the build never checks the cache back in (any error drops its
//!   account and storage tables), and a failed persist drops them too.
//!
//! The bytecode table is the exception to every drop but one: `keccak256(code) -> code` cannot go
//! stale, so a dropped cache hands its code table to the next one (see [`ExecutionCacheSlot`]).
//! Only a stale hit (below) drops the code table too, since it means an invariant failed somewhere.
//!
//! # Wiped accounts
//!
//! An account that existed at the anchor and is destroyed in a block (EIP-161 removal of an empty
//! account; since Cancun also a CREATE2 over a funded, codeless address whose init code
//! self-destructs) loses all its storage, and a flat `(address, slot)` table cannot find that
//! account's entries. Its address joins the cache's wiped set instead: every later storage read of
//! it goes to the parent state (no get, no fill), `insert_state` skips its slots, and its account
//! entry is removed. Its old slot entries stay in their buckets, unreachable, until evicted. The
//! set lives as long as the cache; when a wipe would take it past [`CacheCapacity::wiped`]
//! ([`DEFAULT_WIPED_ACCOUNTS`]), the cache is dropped instead (the code table kept).
//!
//! The rule holds in both storage layouts: it does not depend on whether a codeless account can
//! hold storage (storage v1 allows it in genesis, v2 refuses it), only on the parent state reading
//! a wiped account's slots right, which a cache-less node relies on too.
//!
//! Cost: with an empty set, one length check per storage read. With a non-empty one, a lookup in a
//! seeded hash set per storage read. An attacker forcing a drop needs one more wipe than the cap,
//! each a funded address plus a CREATE2 (at least about 58k gas), so about 240M gas per drop at the
//! default cap (eight full 30M-gas blocks); before the set, one wipe (about 60-80k gas a block)
//! dropped the whole cache.
//!
//! # Checks
//!
//! TN builds its own blocks, so no payload's state root catches a stale hit: it would fork the
//! chain. In production one hit of each kind in [`CHECK_ONE_HIT_IN`] is re-read from the parent
//! state. A differing value is logged with its key and both values, counted
//! (`tn_reth.exec_cache_stale_hits_total`), and makes `build_block_from_batch_payload` drop the
//! whole cache and build the block again without one, so the block is right and the node goes on.
//! With `TN_EXEC_CACHE_SHADOW=1` (test builds; the default in tn-reth's unit tests) every hit is
//! checked and a stale one panics.
//!
//! # Differences from upstream
//! - Every miss fills the cache (upstream's prewarm mode); TN runs no prewarming, so the block's
//!   own reads are the warm-up. A missing bytecode is not cached.
//! - A destroyed account that existed at the anchor joins the wiped set (above), code or not.
//!   Upstream invalidates the whole cache, and only when the account had code. An account created
//!   and destroyed within the block (it did not exist at the anchor) loses its account entry and
//!   the entries of its listed slots, and keeps the rest: no other slot of it can be cached.
//! - Invalidation drops the cache and the next block allocates a new one. fixed-cache 0.1.8's O(1)
//!   epoch clear zeroes the buckets on the first epoch wrap only, so after 2048 clears entries of
//!   an old epoch would read as live again.
//! - Every hash has its top bit set ([`TopBitHash`]): without `Drop` values fixed-cache tags a live
//!   entry with the hash's high bits alone, the same tag 0 as an empty bucket, so a key of all zero
//!   bytes whose hash had zero high bits would read the empty bucket's zeroed value.
//! - Sampled checks of hits against the parent state (above); upstream relies on the payload's
//!   state root instead.
//! - No prometheus metrics for hits, no `stats` feature of fixed-cache (it needs a crate TN does
//!   not carry), no usage guard (the env hands the cache to one build at a time). Fixed entry
//!   counts instead of a byte budget split.
//!
//! fixed-cache is pinned to exactly 0.1.8 (root `Cargo.toml`): the points above rely on its
//! internals (strong compare-exchange bucket locks, tag 0 for an empty bucket, `remove` unlocking
//! to 0). Re-read them before any bump.

use std::{
    fmt,
    hash::{BuildHasher, Hasher},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use alloy::primitives::{
    map::{DefaultHashBuilder, HashSet},
    Address, Bytes, StorageKey, StorageValue, B256,
};
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
use tracing::{debug, error, warn};

use super::RethEnv;
use crate::metrics::RETH_METRICS;

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

/// Default most wiped accounts a cache tracks (see the module docs): a wipe past it drops the
/// cache. At most about 170 KiB of set. Large enough that forcing a drop costs an attacker about
/// eight full 30M-gas blocks of wipes, and that a steady legitimate stream (deposit sweepers that
/// CREATE2 over a funded address and self-destruct) drops it rarely; small enough that a lookup
/// stays in cache.
pub(crate) const DEFAULT_WIPED_ACCOUNTS: usize = 4096;

/// One hit of each kind (account, slot, code) in this many is re-read from the parent state in
/// production (see the module docs). A power of two.
///
/// Chosen for under 0.5 ms an output on the exec bench's peak profile (20 blocks of 30M gas, about
/// 36.6k hits an output: 13.1k accounts, 23.5k slots, 60 code). A check costs one parent-state
/// read: the shadow run, which checks every hit, took 186.0 ms to execute an output against 138.7
/// ms without checks (Mac, storage v2, default sizes), 1.3 us a check. At 1 in 256 that is about
/// 143 checks, 0.19 ms an output, and still under 0.5 ms if a read costs 2.5 times as much.
pub(crate) const CHECK_ONE_HIT_IN: u64 = 256;

/// A fixed-cache with [`TopBitHash`] and the default config (no epochs, no stats).
type FixedCache<K, V> = fixed_cache::Cache<K, V, TopBitHash>;

/// The bytecode table: `keccak256(code) -> code`, kept across caches.
pub(crate) type CodeTable = FixedCache<B256, Bytecode>;

/// The entry counts of an [`ExecutionCache`]. Each table's is a power of two, at least 4
/// (fixed-cache's minimum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CacheCapacity {
    /// Account entries.
    pub(crate) accounts: usize,
    /// Storage-slot entries.
    pub(crate) storage: usize,
    /// Bytecode entries.
    pub(crate) code: usize,
    /// Most wiped accounts before the cache is dropped.
    pub(crate) wiped: usize,
}

impl Default for CacheCapacity {
    fn default() -> Self {
        Self {
            accounts: DEFAULT_ACCOUNT_ENTRIES,
            storage: DEFAULT_STORAGE_ENTRIES,
            code: DEFAULT_CODE_ENTRIES,
            wiped: DEFAULT_WIPED_ACCOUNTS,
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

    /// Bytes of buckets this capacity allocates (bytecode heap and wiped set not included).
    pub(crate) const fn bucket_bytes(&self) -> usize {
        (self.accounts + self.storage + self.code) * BUCKET_BYTES
    }
}

/// Hit and miss counts of an [`ExecutionCache`] since it was created (a kept code table starts
/// its counts again), and the size of its wiped set.
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
    /// Storage reads of a wiped account, passed to the parent state without a lookup or a fill.
    pub storage_bypasses: u64,
    /// Bytecode reads answered by the cache.
    pub code_hits: u64,
    /// Bytecode reads passed to the parent state.
    pub code_misses: u64,
    /// Hits re-read from the parent state to check them.
    pub checked_hits: u64,
    /// Accounts in the wiped set.
    pub wiped_accounts: u64,
}

/// The counters behind [`ExecutionCacheStats`]. Atomics so the provider stays `Sync`; only the
/// building thread touches them.
#[derive(Debug, Default)]
struct Counters {
    account_hits: AtomicU64,
    account_misses: AtomicU64,
    storage_hits: AtomicU64,
    storage_misses: AtomicU64,
    storage_bypasses: AtomicU64,
    code_hits: AtomicU64,
    code_misses: AtomicU64,
    checked_hits: AtomicU64,
}

impl Counters {
    /// Add one to `counter` and return its value before.
    fn bump(counter: &AtomicU64) -> u64 {
        counter.fetch_add(1, Ordering::Relaxed)
    }

    fn snapshot(&self) -> ExecutionCacheStats {
        let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
        ExecutionCacheStats {
            account_hits: load(&self.account_hits),
            account_misses: load(&self.account_misses),
            storage_hits: load(&self.storage_hits),
            storage_misses: load(&self.storage_misses),
            storage_bypasses: load(&self.storage_bypasses),
            code_hits: load(&self.code_hits),
            code_misses: load(&self.code_misses),
            checked_hits: load(&self.checked_hits),
            wiped_accounts: 0,
        }
    }
}

/// Why [`ExecutionCache::insert_state`] refused a bundle: the cache can no longer be proven to
/// hold the post-block state and must be dropped (its code table may be kept).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertStateError {
    /// An account that existed at the anchor was destroyed, and the wiped set is full.
    WipedSetFull(Address),
    /// A modified, not destroyed account without account info (unrepresentable in revm).
    ModifiedWithoutInfo(Address),
}

/// Account, storage and bytecode values at one block's state (the anchor, held by
/// [`SavedCache`]). See the module docs for the invariant.
#[derive(Debug)]
pub(crate) struct ExecutionCache {
    /// `keccak256(code) -> code`: never stale, only real code is stored.
    code: CodeTable,
    /// `(address, slot) -> value`, zero for an unset slot. Entries of a wiped account are never
    /// read.
    storage: FixedCache<(Address, StorageKey), StorageValue>,
    /// `address -> account`, `None` for an account that does not exist.
    accounts: FixedCache<Address, Option<Account>>,
    /// Accounts destroyed since the cache was created that existed at the anchor of their
    /// destruction: their storage reads bypass the cache. Seeded hashing, so an attacker cannot
    /// pick colliding addresses.
    wiped: HashSet<Address>,
    /// The most addresses `wiped` may hold.
    wiped_limit: usize,
    /// Hit and miss counts.
    counters: Counters,
    /// Check every hit against the parent state and panic on a stale one (tests and benches).
    #[cfg(any(test, feature = "test-utils"))]
    shadow: bool,
}

impl ExecutionCache {
    /// An empty cache with `capacity` entries, over `code` (a dropped cache's bytecode table) when
    /// there is one. Allocation is lazy (zeroed pages), so a large capacity costs memory only as
    /// entries fill.
    fn with_code(capacity: CacheCapacity, code: Option<CodeTable>) -> Self {
        Self {
            code: code.unwrap_or_else(|| FixedCache::new(capacity.code, TopBitHash::default())),
            storage: FixedCache::new(capacity.storage, TopBitHash::default()),
            accounts: FixedCache::new(capacity.accounts, TopBitHash::default()),
            wiped: HashSet::default(),
            wiped_limit: capacity.wiped,
            counters: Counters::default(),
            #[cfg(any(test, feature = "test-utils"))]
            shadow: shadow_from_env(),
        }
    }

    /// The bytecode table, dropping the rest.
    fn into_code(self) -> CodeTable {
        self.code
    }

    /// Hit and miss counts since this cache was created, and its wiped set's size.
    pub(crate) fn stats(&self) -> ExecutionCacheStats {
        ExecutionCacheStats { wiped_accounts: self.wiped.len() as u64, ..self.counters.snapshot() }
    }

    /// Whether `address` is wiped: its storage reads bypass the cache. The emptiness check first
    /// keeps the common case (no wipe since the cache was created) free of a hash.
    #[inline]
    fn is_wiped(&self, address: &Address) -> bool {
        !self.wiped.is_empty() && self.wiped.contains(address)
    }

    /// Apply one block's post-execution `bundle`, moving every entry from the parent's state to
    /// the block's.
    ///
    /// Order follows upstream: bytecodes first, then each account's slots, then the account, so an
    /// account entry never names code the cache cannot serve. On `Err` the cache is no longer
    /// known to match the block's state and the caller drops it (the code table may be kept: the
    /// bytecodes are in).
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
                // the next read takes the post-block account from the parent provider
                self.accounts.remove(address);
                if account.original_info.is_some() {
                    // it existed at the anchor and every slot of it was wiped: the cache cannot
                    // find them, so its storage reads bypass the cache from now on
                    if self.wiped.len() >= self.wiped_limit && !self.wiped.contains(address) {
                        return Err(InsertStateError::WipedSetFull(*address));
                    }
                    self.wiped.insert(*address);
                } else {
                    // created and destroyed within this block (and maybe created again): its
                    // slots were never read from the parent state, where it did not exist, so
                    // only the slots the block lists can be cached (and only as zero)
                    for slot in account.storage.keys() {
                        self.storage.remove(&(*address, StorageKey::from(*slot)));
                    }
                }
                continue;
            }

            let Some(info) = &account.info else {
                return Err(InsertStateError::ModifiedWithoutInfo(*address));
            };
            // a wiped account's slots are never read from the cache: do not fill buckets
            if !self.is_wiped(address) {
                for (slot, value) in &account.storage {
                    self.storage.insert((*address, StorageKey::from(*slot)), value.present_value);
                }
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

/// The env's cache slot: the saved cache between block builds, a dropped cache's code table, and
/// the capacity of a new one.
#[derive(Debug)]
pub(crate) struct ExecutionCacheSlot {
    /// The cache, anchored at the last block built, or `None` (startup, or dropped after a
    /// failure).
    saved: Option<SavedCache>,
    /// The bytecode table of the last cache dropped, for the next new one. `None` while `saved`
    /// holds one, at startup, and after a stale hit.
    code: Option<CodeTable>,
    /// Entry counts of a new cache.
    capacity: CacheCapacity,
    /// Blocks built again without the cache after a checked hit was stale.
    rebuilds: u64,
    /// Build without any cache (the differential tests' baseline).
    #[cfg(any(test, feature = "test-utils"))]
    disabled: bool,
}

impl ExecutionCacheSlot {
    /// An empty slot (no cache yet) at the default capacity, or the test knobs' (see
    /// [`CacheCapacity::from_env`]).
    pub(crate) fn new() -> Self {
        Self {
            saved: None,
            code: None,
            capacity: CacheCapacity::from_env(),
            rebuilds: 0,
            #[cfg(any(test, feature = "test-utils"))]
            disabled: false,
        }
    }
}

impl RethEnv {
    /// Take the saved cache for a block built on `parent`: the saved one when it is anchored at
    /// `parent`, else a new empty one (anchored at `parent` by the caller's use of it) over the
    /// last dropped cache's bytecode table. `None` only in tests that disabled the cache.
    ///
    /// The env holds no cache while a block builds: only [`Self::check_in_execution_cache`] puts
    /// one back, so a build that fails at any point leaves none.
    pub(crate) fn check_out_execution_cache(&self, parent: B256) -> Option<ExecutionCache> {
        let (saved, code, capacity) = {
            let mut slot = self.inner.execution_cache.lock();
            #[cfg(any(test, feature = "test-utils"))]
            if slot.disabled {
                return None;
            }
            (slot.saved.take(), slot.code.take(), slot.capacity)
        };
        let cache = match saved {
            Some(saved) if saved.hash == parent => saved.cache,
            stale => {
                if let Some(stale) = &stale {
                    debug!(target: "engine", anchor = %stale.hash, %parent, "execution cache anchor is not the parent: starting empty");
                }
                // the stale cache's accounts and storage drop here; its code carries over
                let code = stale.map(|stale| stale.cache.into_code()).or(code);
                debug!(target: "engine", %parent, bytes = capacity.bucket_bytes(), "new execution cache");
                ExecutionCache::with_code(capacity, code)
            }
        };
        Some(cache)
    }

    /// Apply the block's `bundle` to `cache` and save it anchored at `block_hash`. A refused
    /// bundle drops the cache instead, keeping its code table.
    pub(crate) fn check_in_execution_cache(
        &self,
        mut cache: ExecutionCache,
        block_hash: B256,
        bundle: &BundleState,
    ) {
        if let Err(error) = cache.insert_state(bundle) {
            warn!(target: "engine", ?error, %block_hash, "execution cache dropped: the block's bundle cannot be applied to it");
            self.keep_execution_cache_code(cache);
            return;
        }
        #[cfg(any(test, feature = "test-utils"))]
        print_block_stats(block_hash, &cache);
        self.inner.execution_cache.lock().saved = Some(SavedCache { hash: block_hash, cache });
    }

    /// Drop `cache` but keep its bytecode table for the next new cache: a failed build or a
    /// refused bundle says nothing against `keccak256(code) -> code`.
    pub(crate) fn keep_execution_cache_code(&self, cache: ExecutionCache) {
        // the account and storage tables drop (unmap) here, outside the lock
        let code = cache.into_code();
        self.inner.execution_cache.lock().code = Some(code);
    }

    /// Drop `cache` whole, bytecode table included, after one of its hits was stale, and count the
    /// block's rebuild.
    pub(crate) fn drop_stale_execution_cache(&self, cache: Option<ExecutionCache>) {
        drop(cache);
        let mut slot = self.inner.execution_cache.lock();
        slot.code = None;
        slot.rebuilds += 1;
    }

    /// Drop the saved execution cache: the next block starts empty (over its bytecode table).
    ///
    /// Called when a persist fails. Anything that writes account, storage or bytecode state outside
    /// a block build must call it too (see the module docs of `env/exec_cache.rs`).
    pub fn clear_execution_cache(&self) {
        let saved = self.inner.execution_cache.lock().saved.take();
        if let Some(saved) = saved {
            self.keep_execution_cache_code(saved.cache);
        }
    }

    /// The saved cache's anchor and its hit and miss counts, or `None` when no cache is saved.
    pub fn execution_cache_stats(&self) -> Option<(B256, ExecutionCacheStats)> {
        let slot = self.inner.execution_cache.lock();
        slot.saved.as_ref().map(|saved| (saved.hash, saved.cache.stats()))
    }

    /// How many blocks this env built again without the execution cache because a checked hit
    /// differed from the parent state. Zero unless an invariant of `env/exec_cache.rs` failed.
    pub fn execution_cache_rebuilds(&self) -> u64 {
        self.inner.execution_cache.lock().rebuilds
    }

    /// TEST-ONLY: set the entry counts of the next new cache and drop the saved one and the kept
    /// bytecode table.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_execution_cache_capacity_for_test(&self, accounts: usize, storage: usize) {
        let mut slot = self.inner.execution_cache.lock();
        slot.capacity = CacheCapacity { accounts, storage, code: 4, wiped: DEFAULT_WIPED_ACCOUNTS };
        slot.saved = None;
        slot.code = None;
    }

    /// TEST-ONLY: build every later block without an execution cache, as a node without one
    /// would (the baseline of a differential run).
    #[cfg(any(test, feature = "test-utils"))]
    pub fn disable_execution_cache_for_test(&self) {
        let mut slot = self.inner.execution_cache.lock();
        slot.disabled = true;
        slot.saved = None;
        slot.code = None;
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
             storage_misses={} storage_bypasses={} code_hits={} code_misses={} checked_hits={} \
             wiped_accounts={}",
            s.account_hits,
            s.account_misses,
            s.storage_hits,
            s.storage_misses,
            s.storage_bypasses,
            s.code_hits,
            s.code_misses,
            s.checked_hits,
            s.wiped_accounts
        );
    }
}

/// A state provider that answers account, storage and bytecode reads from an [`ExecutionCache`]
/// and passes the rest, and every miss, to `inner` (the parent block's state). Without a cache it
/// passes everything (a block's rebuild after a stale hit).
#[derive(Debug)]
pub(crate) struct CachedStateProvider<'a, S> {
    /// The parent block's state.
    inner: S,
    /// The cache, anchored at the parent.
    cache: Option<&'a ExecutionCache>,
    /// Set when a checked hit differed from `inner`: the block built on this provider must be
    /// built again without the cache.
    served_stale: AtomicBool,
}

impl<'a, S> CachedStateProvider<'a, S> {
    /// Wrap `inner`, the state at `cache`'s anchor.
    pub(crate) const fn new(inner: S, cache: Option<&'a ExecutionCache>) -> Self {
        Self { inner, cache, served_stale: AtomicBool::new(false) }
    }

    /// Whether a checked hit differed from the parent state.
    pub(crate) fn served_stale(&self) -> bool {
        self.served_stale.load(Ordering::Relaxed)
    }
}

/// Whether the hit counted `hit` (its counter's value before it) is re-read from the parent
/// state: one in [`CHECK_ONE_HIT_IN`], every one in shadow mode.
#[inline]
fn is_checked(cache: &ExecutionCache, hit: u64) -> bool {
    #[cfg(any(test, feature = "test-utils"))]
    if cache.shadow {
        return true;
    }
    let _ = cache;
    hit.is_multiple_of(CHECK_ONE_HIT_IN)
}

impl<S: StateProvider> CachedStateProvider<'_, S> {
    /// A checked hit differed from the parent state: log it, count it, and mark the block for a
    /// rebuild without the cache. In shadow mode, panic.
    #[cold]
    fn stale(&self, cache: &ExecutionCache, what: fmt::Arguments<'_>) {
        #[cfg(any(test, feature = "test-utils"))]
        if cache.shadow {
            panic!("execution cache served a stale {what}");
        }
        let _ = cache;
        error!(target: "engine", "execution cache served a stale {what}; the block is built again without the cache");
        RETH_METRICS.exec_cache_stale_hits_total.increment(1);
        self.served_stale.store(true, Ordering::Relaxed);
    }

    /// Re-read a cached account from the parent state. Accounts compare as revm sees them (an
    /// empty code hash and no code hash are one).
    #[cold]
    #[inline(never)]
    fn check_account(
        &self,
        cache: &ExecutionCache,
        address: &Address,
        cached: Option<Account>,
    ) -> ProviderResult<Option<Account>> {
        Counters::bump(&cache.counters.checked_hits);
        let norm = |a: Option<Account>| a.map(|a| (a.nonce, a.balance, a.get_bytecode_hash()));
        let fresh = self.inner.basic_account(address)?;
        if norm(cached) == norm(fresh) {
            return Ok(cached);
        }
        self.stale(cache, format_args!("account {address}: cached {cached:?}, state {fresh:?}"));
        Ok(fresh)
    }

    /// Re-read a cached slot from the parent state, unset as zero.
    #[cold]
    #[inline(never)]
    fn check_storage(
        &self,
        cache: &ExecutionCache,
        address: Address,
        slot: StorageKey,
        cached: StorageValue,
    ) -> ProviderResult<StorageValue> {
        Counters::bump(&cache.counters.checked_hits);
        let fresh = self.inner.storage(address, slot)?.unwrap_or_default();
        if cached != fresh {
            self.stale(
                cache,
                format_args!("slot {address} {slot}: cached {cached}, state {fresh}"),
            );
        }
        Ok(fresh)
    }

    /// Re-read cached code from the parent state.
    #[cold]
    #[inline(never)]
    fn check_code(
        &self,
        cache: &ExecutionCache,
        code_hash: &B256,
        cached: Bytecode,
    ) -> ProviderResult<Option<Bytecode>> {
        Counters::bump(&cache.counters.checked_hits);
        let fresh = self.inner.bytecode_by_hash(code_hash)?;
        if fresh.as_ref().map(|code| code.original_bytes()) == Some(cached.original_bytes()) {
            return Ok(Some(cached));
        }
        let fresh_len = fresh.as_ref().map(|code| code.original_bytes().len());
        self.stale(
            cache,
            format_args!(
                "code {code_hash}: cached {} bytes, state {fresh_len:?} bytes",
                cached.original_bytes().len()
            ),
        );
        Ok(fresh)
    }
}

impl<S: StateProvider> AccountReader for CachedStateProvider<'_, S> {
    fn basic_account(&self, address: &Address) -> ProviderResult<Option<Account>> {
        let Some(cache) = self.cache else { return self.inner.basic_account(address) };
        if let Some(account) = cache.accounts.get(address) {
            let hit = Counters::bump(&cache.counters.account_hits);
            if is_checked(cache, hit) {
                return self.check_account(cache, address, account);
            }
            return Ok(account);
        }
        Counters::bump(&cache.counters.account_misses);
        let account = self.inner.basic_account(address)?;
        cache.accounts.insert(*address, account);
        Ok(account)
    }
}

impl<S: StateProvider> StateProvider for CachedStateProvider<'_, S> {
    fn storage(
        &self,
        account: Address,
        storage_key: StorageKey,
    ) -> ProviderResult<Option<StorageValue>> {
        let Some(cache) = self.cache else { return self.inner.storage(account, storage_key) };
        if cache.is_wiped(&account) {
            Counters::bump(&cache.counters.storage_bypasses);
            return self.inner.storage(account, storage_key);
        }
        if let Some(mut value) = cache.storage.get(&(account, storage_key)) {
            let hit = Counters::bump(&cache.counters.storage_hits);
            if is_checked(cache, hit) {
                value = self.check_storage(cache, account, storage_key, value)?;
            }
            // an unset slot and a slot set to zero read alike: `None` for both
            return Ok(Some(value).filter(|v| !v.is_zero()));
        }
        Counters::bump(&cache.counters.storage_misses);
        let value = self.inner.storage(account, storage_key)?;
        cache.storage.insert((account, storage_key), value.unwrap_or_default());
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
        let Some(cache) = self.cache else { return self.inner.bytecode_by_hash(code_hash) };
        if let Some(code) = cache.code.get(code_hash) {
            let hit = Counters::bump(&cache.counters.code_hits);
            if is_checked(cache, hit) {
                return self.check_code(cache, code_hash, code);
            }
            return Ok(Some(code));
        }
        Counters::bump(&cache.counters.code_misses);
        let code = self.inner.bytecode_by_hash(code_hash)?;
        if let Some(code) = &code {
            cache.code.insert(*code_hash, code.clone());
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
