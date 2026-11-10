//! Tests of the cross-block execution cache: `insert_state` on hand-made bundles, the hasher, and
//! the cache on real blocks built by `RethEnv::build_block_from_batch_payload`. The shadow check
//! is on in these tests (`shadow_from_env`), so every hit is also compared with the parent state.

use super::*;
use crate::{
    payload::TNPayload,
    test_utils::{consensus_output_for_tests_at, TransactionFactory},
    ExecutedBlock, NewCanonicalChain, OutputTrieOverlay, RethChainSpec,
};
use alloy::primitives::{keccak256, U256};
use reth_revm::{
    db::{states::StorageSlot, AccountStatus, BundleAccount},
    state::AccountInfo,
};
use std::sync::Arc;
use tempfile::TempDir;
use tn_types::{
    test_genesis_at, Bytes, GenesisAccount, SealedHeader, TaskManager, MIN_PROTOCOL_BASE_FEE,
};

// ---------------------------------------------------------------------------------------------
// insert_state on hand-made bundles

fn info(nonce: u64, balance: u64) -> AccountInfo {
    AccountInfo { nonce, balance: U256::from(balance), ..Default::default() }
}

fn tiny() -> ExecutionCache {
    ExecutionCache::new(CacheCapacity { accounts: 64, storage: 64, code: 4 })
}

fn bundle(accounts: Vec<(Address, BundleAccount)>) -> BundleState {
    BundleState { state: accounts.into_iter().collect(), ..Default::default() }
}

fn slots(values: &[(u64, u64, u64)]) -> reth_revm::db::states::StorageWithOriginalValues {
    values
        .iter()
        .map(|(k, old, new)| {
            (U256::from(*k), StorageSlot::new_changed(U256::from(*old), U256::from(*new)))
        })
        .collect()
}

fn key(slot: u64) -> StorageKey {
    StorageKey::from(U256::from(slot))
}

/// A changed account moves to its new info and present slot values; a loaded-only account is
/// left alone.
#[test]
fn insert_state_applies_changed_accounts_and_slots() {
    let mut cache = tiny();
    let (a, b) = (Address::repeat_byte(1), Address::repeat_byte(2));
    cache.accounts.insert(a, Some(Account::from(info(0, 10))));
    cache.storage.insert((a, key(1)), U256::from(5));
    cache.accounts.insert(b, Some(Account::from(info(3, 3))));

    let changed = BundleAccount::new(
        Some(info(0, 10)),
        Some(info(1, 9)),
        slots(&[(1, 5, 6), (2, 0, 7)]),
        AccountStatus::Changed,
    );
    let loaded =
        BundleAccount::new(Some(info(3, 3)), Some(info(3, 3)), slots(&[]), AccountStatus::Loaded);
    cache.insert_state(&bundle(vec![(a, changed), (b, loaded)])).expect("bundle applies");

    assert_eq!(cache.accounts.get(&a), Some(Some(Account::from(info(1, 9)))));
    assert_eq!(cache.storage.get(&(a, key(1))), Some(U256::from(6)));
    assert_eq!(cache.storage.get(&(a, key(2))), Some(U256::from(7)));
    assert_eq!(cache.accounts.get(&b), Some(Some(Account::from(info(3, 3)))));
}

/// An account that existed at the anchor and was destroyed (here destroyed and re-created) had
/// its storage wiped: the cache cannot drop one account's slots, so it refuses the bundle.
#[test]
fn insert_state_refuses_a_wiped_account_that_existed() {
    let mut cache = tiny();
    let a = Address::repeat_byte(1);
    cache.storage.insert((a, key(1)), U256::from(5));
    for status in
        [AccountStatus::Destroyed, AccountStatus::DestroyedChanged, AccountStatus::DestroyedAgain]
    {
        let wiped =
            BundleAccount::new(Some(info(1, 0)), Some(info(1, 0)), slots(&[(2, 0, 9)]), status);
        assert_eq!(
            cache.insert_state(&bundle(vec![(a, wiped)])),
            Err(InsertStateError::DestroyedExisting(a)),
            "{status:?}"
        );
    }
}

/// An account created, destroyed and re-created within the block (it did not exist at the
/// anchor) loses its account entry and its listed slots; other entries stay.
#[test]
fn insert_state_drops_an_account_created_and_destroyed_in_the_block() {
    let mut cache = tiny();
    let (a, b) = (Address::repeat_byte(1), Address::repeat_byte(2));
    // what the block's reads of `a` could have left: it did not exist at the anchor
    cache.accounts.insert(a, None);
    cache.storage.insert((a, key(1)), U256::ZERO);
    cache.storage.insert((b, key(1)), U256::from(4));

    let recreated = BundleAccount::new(
        None,
        Some(info(1, 1)),
        slots(&[(1, 0, 7)]),
        AccountStatus::DestroyedChanged,
    );
    cache.insert_state(&bundle(vec![(a, recreated)])).expect("bundle applies");

    assert_eq!(cache.accounts.get(&a), None, "the re-created account reads from the parent state");
    assert_eq!(
        cache.storage.get(&(a, key(1))),
        None,
        "its listed slot reads from the parent state"
    );
    assert_eq!(cache.storage.get(&(b, key(1))), Some(U256::from(4)));
}

/// A modified, not destroyed account without info is unrepresentable: refused.
#[test]
fn insert_state_refuses_a_modified_account_without_info() {
    let mut cache = tiny();
    let a = Address::repeat_byte(1);
    let broken = BundleAccount::new(Some(info(0, 1)), None, slots(&[]), AccountStatus::Changed);
    assert_eq!(
        cache.insert_state(&bundle(vec![(a, broken)])),
        Err(InsertStateError::ModifiedWithoutInfo(a))
    );
}

/// New contracts enter the code cache.
#[test]
fn insert_state_caches_new_bytecode() {
    let mut cache = tiny();
    let code = reth_revm::bytecode::Bytecode::new_raw(Bytes::from_static(&[0x60, 0x00, 0x00]));
    let hash = code.hash_slow();
    let state = BundleState {
        contracts: [(hash, code.clone())].into_iter().collect(),
        ..Default::default()
    };
    cache.insert_state(&state).expect("bundle applies");
    assert_eq!(cache.code.get(&hash).map(|c| c.original_bytes()), Some(code.original_bytes()));
}

/// Eviction in a cache smaller than its key set never serves a wrong value: each get returns the
/// key's last value or nothing.
#[test]
fn tiny_cache_evicts_without_serving_another_keys_value() {
    let cache = ExecutionCache::new(CacheCapacity { accounts: 4, storage: 4, code: 4 });
    let address = |i: u64| Address::left_padding_from(&i.to_be_bytes());
    for round in 0..3u64 {
        for i in 0..64u64 {
            cache.storage.insert((address(i), key(i)), U256::from(i * 1000 + round));
            cache.accounts.insert(address(i), Some(Account::from(info(round, i))));
        }
        for i in 0..64u64 {
            if let Some(value) = cache.storage.get(&(address(i), key(i))) {
                assert_eq!(value, U256::from(i * 1000 + round));
            }
            if let Some(account) = cache.accounts.get(&address(i)) {
                assert_eq!(account, Some(Account::from(info(round, i))));
            }
        }
    }
}

/// Every table's bucket takes [`BUCKET_BYTES`], the figure the default capacities' memory is
/// stated in.
#[test]
fn buckets_take_bucket_bytes() {
    use fixed_cache::Bucket;
    assert_eq!(size_of::<Bucket<((Address, StorageKey), StorageValue)>>(), BUCKET_BYTES);
    assert_eq!(size_of::<Bucket<(Address, Option<Account>)>>(), BUCKET_BYTES);
    assert_eq!(size_of::<Bucket<(B256, Bytecode)>>(), BUCKET_BYTES);
}

// ---------------------------------------------------------------------------------------------
// the hasher

/// A hasher that always hashes to 0: the worst case for fixed-cache's tags.
#[derive(Debug, Clone, Default)]
struct ZeroHash;

impl BuildHasher for ZeroHash {
    type Hasher = ZeroHasher;
    fn build_hasher(&self) -> ZeroHasher {
        ZeroHasher
    }
}

#[derive(Debug)]
struct ZeroHasher;

impl Hasher for ZeroHasher {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, _bytes: &[u8]) {}
}

/// fixed-cache tags a live entry of a value without `Drop` by the hash's high bits alone, the
/// tag of an empty bucket: an all-zero key whose hash has zero high bits reads the empty bucket's
/// zeroed bytes as an entry. `TopBitHash` sets a high bit, so the empty cache reads nothing.
#[test]
fn top_bit_hash_keeps_an_empty_bucket_from_matching_the_zero_key() {
    let raw: fixed_cache::Cache<Address, Option<Account>, ZeroHash> =
        fixed_cache::Cache::new(4, ZeroHash);
    assert!(
        raw.get(&Address::ZERO).is_some(),
        "the hazard: an empty bucket answers the zero key under a zero hash"
    );
    let raw_slots: fixed_cache::Cache<(Address, StorageKey), StorageValue, ZeroHash> =
        fixed_cache::Cache::new(4, ZeroHash);
    assert_eq!(raw_slots.get(&(Address::ZERO, B256::ZERO)), Some(U256::ZERO));

    let fixed: fixed_cache::Cache<Address, Option<Account>, TopBitHash<ZeroHash>> =
        fixed_cache::Cache::new(4, TopBitHash(ZeroHash));
    assert_eq!(fixed.get(&Address::ZERO), None);
    let fixed_slots: fixed_cache::Cache<(Address, StorageKey), StorageValue, TopBitHash<ZeroHash>> =
        fixed_cache::Cache::new(4, TopBitHash(ZeroHash));
    assert_eq!(fixed_slots.get(&(Address::ZERO, B256::ZERO)), None);
    fixed_slots.insert((Address::ZERO, B256::ZERO), U256::from(3));
    assert_eq!(fixed_slots.get(&(Address::ZERO, B256::ZERO)), Some(U256::from(3)));

    let cache = tiny();
    assert_eq!(cache.accounts.get(&Address::ZERO), None);
    assert_eq!(cache.storage.get(&(Address::ZERO, B256::ZERO)), None);
}

// ---------------------------------------------------------------------------------------------
// the cache on real blocks

/// Fixed genesis and block timestamps: reproducible block hashes across envs.
const GENESIS_TIMESTAMP: u64 = 1_700_000_000;

/// `slot0 += 1` on every call.
const COUNTER: &[u8] = &[0x60, 0x00, 0x54, 0x60, 0x01, 0x01, 0x60, 0x00, 0x55, 0x00];
const COUNTER_ADDRESS: Address = Address::repeat_byte(0xc0);

/// The child's runtime code: `slot2 = slot0 + slot1`.
const CHILD_RUNTIME: [u8; 11] = [0x60, 0x00, 0x54, 0x60, 0x01, 0x54, 0x01, 0x60, 0x02, 0x55, 0x00];

/// The child's init code. Created with no value it writes `slot1 = 5` and self-destructs (in the
/// creating transaction, so it is deleted at its end); with a value it writes `slot0 = 7` and
/// deploys `CHILD_RUNTIME`.
fn child_init() -> Vec<u8> {
    let mut code = vec![
        0x34, 0x60, 0x0b, 0x57, // CALLVALUE PUSH1 live JUMPI
        0x60, 0x05, 0x60, 0x01, 0x55, 0x33, 0xff, // slot1 = 5; SELFDESTRUCT(caller)
        0x5b, 0x60, 0x07, 0x60, 0x00, 0x55, // live: slot0 = 7
        0x60, 0x0b, 0x60, 0x1d, 0x60, 0x00, 0x39, // CODECOPY(0, 0x1d, 11)
        0x60, 0x0b, 0x60, 0x00, 0xf3, // RETURN(0, 11)
    ];
    assert_eq!(code.len(), 0x1d);
    code.extend_from_slice(&CHILD_RUNTIME);
    code
}

/// The factory's runtime code: `CREATE2(callvalue, child_init, salt 0)`, the new address (or 0)
/// in `slot0`.
fn factory_code() -> Vec<u8> {
    let init = child_init();
    let mut code = vec![
        0x60,
        init.len() as u8,
        0x60,
        0x13,
        0x60,
        0x00,
        0x39, // CODECOPY(0, 0x13, len)
        0x60,
        0x00,
        0x60,
        init.len() as u8,
        0x60,
        0x00,
        0x34,
        0xf5, // CREATE2(value, 0, len, 0)
        0x60,
        0x00,
        0x55,
        0x00, // slot0 = address; STOP
    ];
    assert_eq!(code.len(), 0x13);
    code.extend_from_slice(&init);
    code
}
const FACTORY_ADDRESS: Address = Address::repeat_byte(0xf0);
/// An empty genesis account (no nonce, balance, code or storage: TN's genesis refuses storage
/// without code): touching it deletes it (EIP-161).
const EMPTY_ADDRESS: Address = Address::repeat_byte(0xe0);

/// One env on a fixed genesis with the counter and the factory, chaining blocks in memory as the
/// engine does within one consensus output.
struct TestChain {
    env: RethEnv,
    chain: Arc<RethChainSpec>,
    head: SealedHeader,
    index: u64,
    sender: TransactionFactory,
    _tmp: TempDir,
    _task_manager: TaskManager,
}

impl TestChain {
    fn new() -> eyre::Result<Self> {
        let genesis = test_genesis_at(GENESIS_TIMESTAMP).extend_accounts([
            (
                COUNTER_ADDRESS,
                GenesisAccount::default().with_code(Some(Bytes::from_static(COUNTER))),
            ),
            (
                FACTORY_ADDRESS,
                GenesisAccount::default().with_code(Some(Bytes::from(factory_code()))),
            ),
            (EMPTY_ADDRESS, GenesisAccount::default()),
        ]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp = TempDir::new()?;
        let task_manager = TaskManager::new("exec cache test");
        let env = RethEnv::new_for_temp_chain(chain.clone(), tmp.path(), &task_manager, None)?;
        let head = chain.sealed_genesis_header();
        Ok(Self {
            env,
            chain,
            head,
            index: 0,
            sender: TransactionFactory::new(),
            _tmp: tmp,
            _task_manager: task_manager,
        })
    }

    fn tx(&mut self, to: Address, value: u64, gas_limit: u64) -> Vec<u8> {
        self.sender.create_eip1559_encoded(
            self.chain.clone(),
            Some(gas_limit),
            MIN_PROTOCOL_BASE_FEE.into(),
            Some(to),
            U256::from(value),
            Bytes::new(),
        )
    }

    fn call(&mut self, to: Address) -> Vec<u8> {
        self.tx(to, 0, 1_000_000)
    }

    /// Build a block on the head and chain it in memory (not persisted).
    fn build(
        &mut self,
        txs: Vec<Vec<u8>>,
        overlay: &mut OutputTrieOverlay,
    ) -> eyre::Result<ExecutedBlock> {
        let index = self.index + 1;
        let output =
            consensus_output_for_tests_at(index as u32, 0, index, false, GENESIS_TIMESTAMP + index);
        let seed = B256::from(U256::from(index));
        let payload = TNPayload::new_for_test_with(
            self.head.clone(),
            &output,
            Address::repeat_byte(0xbe),
            seed,
            seed,
        );
        let block = self.env.build_block_from_batch_payload(payload, &txs, overlay)?;
        self.index = index;
        let header = block.recovered_block.clone_sealed_header();
        let cims = self.env.canonical_in_memory_state();
        cims.update_chain(NewCanonicalChain::Commit { new: vec![block.clone()] });
        cims.set_canonical_head(header.clone());
        self.head = header;
        Ok(block)
    }

    /// Build one consensus output (one block per entry) and persist it.
    fn output(&mut self, blocks: Vec<Vec<Vec<u8>>>) -> eyre::Result<Vec<B256>> {
        let mut overlay = OutputTrieOverlay::new();
        let mut built = Vec::new();
        for txs in blocks {
            built.push(self.build(txs, &mut overlay)?);
        }
        let hashes = built.iter().map(|b| b.recovered_block.hash()).collect();
        self.env.finish_executing_output(built, None)?;
        self.env.finalize_block(self.head.clone())?;
        Ok(hashes)
    }

    fn slot(&self, address: Address, slot: u64) -> eyre::Result<U256> {
        let state = self.env.state_by_block_hash(self.head.hash())?;
        Ok(state.storage(address, key(slot))?.unwrap_or_default())
    }

    fn stats(&self) -> ExecutionCacheStats {
        let (anchor, stats) = self.env.execution_cache_stats().expect("a saved cache");
        assert_eq!(anchor, self.head.hash(), "the cache is anchored at the last built block");
        stats
    }
}

/// Block k writes a slot and block k+1 of the same output (not persisted between them) reads
/// it from the cache.
#[tokio::test]
async fn block_reads_what_an_earlier_block_of_its_output_wrote() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    let mut overlay = OutputTrieOverlay::new();
    let tx = chain.call(COUNTER_ADDRESS);
    let first = chain.build(vec![tx], &mut overlay)?;
    let after_first = chain.stats();
    let tx = chain.call(COUNTER_ADDRESS);
    let second = chain.build(vec![tx], &mut overlay)?;
    let after_second = chain.stats();

    assert!(after_second.storage_hits > after_first.storage_hits, "slot0 came from the cache");
    assert!(after_second.account_hits > after_first.account_hits, "the sender came from the cache");
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(2));
    chain.env.finish_executing_output(vec![first, second], None)?;
    Ok(())
}

/// Output N reads and writes a slot; output N+1 reads N's value from the cache.
#[tokio::test]
async fn next_output_reads_the_previous_outputs_value() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    let after_n = chain.stats();
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    let after_next = chain.stats();

    assert!(after_next.storage_hits > after_n.storage_hits, "slot0 came from the cache");
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(2));
    Ok(())
}

/// An account created with storage and destroyed in one transaction, then created again in the
/// next one of the same block: later blocks read the new incarnation's slots, and the wiped one as
/// zero, through the cache. (revm folds the first incarnation away: a missing account created and
/// destroyed in one transaction stays `LoadedNotExisting`, so the bundle shows the second one as
/// a new account.)
#[tokio::test]
async fn storage_wipe_and_recreation_read_correctly() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    let child = FACTORY_ADDRESS.create2(B256::ZERO, keccak256(child_init()));
    let destroy = chain.tx(FACTORY_ADDRESS, 0, 1_000_000);
    let create = chain.tx(FACTORY_ADDRESS, 1, 1_000_000);
    chain.output(vec![vec![destroy, create]])?;
    assert_eq!(chain.slot(FACTORY_ADDRESS, 0)?, U256::from_be_slice(child.as_slice()));
    assert_eq!(chain.slot(child, 0)?, U256::from(7));
    assert_eq!(chain.slot(child, 1)?, U256::ZERO, "the destroyed incarnation's slot is wiped");

    // two calls: the first fills slot0 and slot1 on misses, the second reads them as hits
    let first = chain.call(child);
    chain.output(vec![vec![first]])?;
    let before = chain.stats();
    let second = chain.call(child);
    chain.output(vec![vec![second]])?;
    assert!(chain.stats().storage_hits >= before.storage_hits + 2, "slot0 and slot1 were hits");
    assert_eq!(chain.slot(child, 2)?, U256::from(7), "slot2 = 7 + 0");
    Ok(())
}

/// An account that existed at the anchor and is destroyed within a block (an empty account,
/// deleted by EIP-161 when a transaction touches it: the one way since Cancun) drops the whole
/// cache, and later blocks read it as gone.
#[tokio::test]
async fn wiped_existing_account_drops_the_cache() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    assert!(chain
        .env
        .state_by_block_hash(chain.head.hash())?
        .basic_account(&EMPTY_ADDRESS)?
        .is_some());
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    assert!(chain.env.execution_cache_stats().is_some());

    let touch = chain.tx(EMPTY_ADDRESS, 0, 100_000);
    chain.output(vec![vec![touch]])?;
    assert!(chain.env.execution_cache_stats().is_none(), "the wipe dropped the cache");
    let state = chain.env.state_by_block_hash(chain.head.hash())?;
    assert_eq!(state.basic_account(&EMPTY_ADDRESS)?, None, "deleted");

    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(2));
    Ok(())
}

/// A block build that fails after executing a transaction (filling the cache) leaves no saved
/// cache, and the next build starts empty.
#[tokio::test]
async fn failed_block_build_drops_the_cache() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    assert!(chain.env.execution_cache_stats().is_some());

    // a gas limit above the block's fails the attempt (not an InvalidTx skip)
    let fill = chain.call(COUNTER_ADDRESS);
    let too_big = chain.tx(COUNTER_ADDRESS, 0, chain.head.gas_limit + 1);
    let _ = chain
        .build(vec![fill, too_big], &mut OutputTrieOverlay::new())
        .expect_err("the build fails");
    assert!(chain.env.execution_cache_stats().is_none(), "the failed build dropped the cache");

    chain.sender.set_nonce(1);
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    let fresh = chain.stats();
    assert_eq!((fresh.account_hits, fresh.storage_hits), (0, 0), "a new, empty cache");
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(2));
    Ok(())
}

/// A bundle `insert_state` refuses drops the cache instead of saving it.
#[tokio::test]
async fn refused_bundle_drops_the_cache() -> eyre::Result<()> {
    let chain = TestChain::new()?;
    let parent = chain.head.hash();
    let cache = chain.env.check_out_execution_cache(parent);
    chain.env.check_in_execution_cache(cache, B256::repeat_byte(1), &BundleState::default());
    assert!(chain.env.execution_cache_stats().is_some());

    let cache = chain.env.check_out_execution_cache(B256::repeat_byte(1));
    let a = Address::repeat_byte(1);
    let wiped = BundleAccount::new(Some(info(1, 0)), None, slots(&[]), AccountStatus::Destroyed);
    chain.env.check_in_execution_cache(cache, B256::repeat_byte(2), &bundle(vec![(a, wiped)]));
    assert!(chain.env.execution_cache_stats().is_none());
    Ok(())
}

/// A failed persist clears the saved cache.
#[tokio::test]
async fn failed_persist_drops_the_cache() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    let tx = chain.call(COUNTER_ADDRESS);
    let block = chain.build(vec![tx], &mut OutputTrieOverlay::new())?;
    assert!(chain.env.execution_cache_stats().is_some());
    chain.env.inject_persist_provider_faults(1);
    chain.env.persist_executed_output(&[block]).expect_err("injected fault");
    assert!(chain.env.execution_cache_stats().is_none());
    Ok(())
}

/// A parent other than the anchor gets an empty cache; the anchor's own child gets the saved one.
#[tokio::test]
async fn parent_other_than_the_anchor_starts_empty() -> eyre::Result<()> {
    let chain = TestChain::new()?;
    let (anchor, other) = (B256::repeat_byte(1), B256::repeat_byte(2));
    let cache = chain.env.check_out_execution_cache(chain.head.hash());
    let a = Address::repeat_byte(9);
    let created = BundleAccount::new(
        None,
        Some(info(1, 1)),
        slots(&[(1, 0, 3)]),
        AccountStatus::InMemoryChange,
    );
    chain.env.check_in_execution_cache(cache, anchor, &bundle(vec![(a, created.clone())]));

    let cache = chain.env.check_out_execution_cache(other);
    assert_eq!(cache.accounts.get(&a), None, "another parent: empty");
    assert_eq!(cache.storage.get(&(a, key(1))), None);
    assert!(chain.env.execution_cache_stats().is_none(), "the mismatched cache was dropped");

    chain.env.check_in_execution_cache(cache, anchor, &bundle(vec![(a, created)]));
    let cache = chain.env.check_out_execution_cache(anchor);
    assert_eq!(cache.accounts.get(&a), Some(Some(Account::from(info(1, 1)))), "the anchor: saved");
    assert_eq!(cache.storage.get(&(a, key(1))), Some(U256::from(3)));
    Ok(())
}

/// Building on a sibling of the anchor (same parent as the anchor block) starts empty.
#[tokio::test]
async fn sibling_block_starts_empty() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    let genesis = chain.head.clone();
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    assert!(chain.stats().storage_hits > 0);

    // rebuild on genesis: the anchor (block 2) is not the parent
    chain.head = genesis;
    chain.sender.set_nonce(0);
    let tx = chain.call(COUNTER_ADDRESS);
    chain.build(vec![tx], &mut OutputTrieOverlay::new())?;
    let fresh = chain.stats();
    assert_eq!((fresh.account_hits, fresh.storage_hits), (0, 0));
    Ok(())
}

/// The same blocks on a cache of 4 entries per table (constant eviction) and on the default one
/// produce the same block hashes, so the same state roots.
#[tokio::test]
async fn tiny_cache_builds_the_same_chain() -> eyre::Result<()> {
    let recipients: Vec<Address> = (1..=6u8).map(|i| Address::repeat_byte(0x10 + i)).collect();
    let run = |accounts: usize, storage: usize| -> eyre::Result<Vec<B256>> {
        let mut chain = TestChain::new()?;
        chain.env.set_execution_cache_capacity_for_test(accounts, storage);
        let mut hashes = Vec::new();
        for output in 0..3usize {
            let mut blocks = Vec::new();
            for block in 0..2usize {
                let to = recipients[(output * 2 + block) % recipients.len()];
                let pay = chain.tx(to, 1 + block as u64, 100_000);
                let count = chain.call(COUNTER_ADDRESS);
                blocks.push(vec![pay, count]);
            }
            hashes.extend(chain.output(blocks)?);
        }
        assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(6));
        Ok(hashes)
    };
    let default = CacheCapacity::default();
    assert_eq!(run(4, 4)?, run(default.accounts, default.storage)?);
    Ok(())
}
