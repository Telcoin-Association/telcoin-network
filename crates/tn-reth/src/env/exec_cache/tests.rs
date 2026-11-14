//! Tests of the cross-block execution cache: `insert_state` on hand-made bundles, the hasher, and
//! the cache on real blocks built by `RethEnv::build_block_from_batch_payload`. The shadow check
//! is on in these tests (`shadow_from_env`), so every hit is also compared with the parent state;
//! only `stale_checked_hit_rebuilds_the_block_without_the_cache` turns it off for its one cache.

use super::*;
use crate::{
    payload::TNPayload,
    test_utils::{
        consensus_output_for_tests_at, test_genesis_with_consensus_registry, TransactionFactory,
    },
    ExecutedBlock, NewCanonicalChain, OutputTrieOverlay, RethChainSpec, StorageLayout,
};
use alloy::{
    consensus::{SignableTransaction as _, TxEip7702},
    eips::eip7702::Authorization,
    primitives::{keccak256, U256},
    signers::SignerSync as _,
};
use rand::{rngs::StdRng, SeedableRng as _};
use reth_revm::{
    db::{states::StorageSlot, AccountStatus, BundleAccount},
    state::AccountInfo,
};
use std::{collections::BTreeMap, sync::Arc};
use tempfile::TempDir;
use tn_types::{
    test_genesis_at, Bytes, Encodable2718 as _, Genesis, GenesisAccount, SealedHeader, TaskManager,
    TransactionSigned, MIN_PROTOCOL_BASE_FEE,
};

// ---------------------------------------------------------------------------------------------
// insert_state on hand-made bundles

fn info(nonce: u64, balance: u64) -> AccountInfo {
    AccountInfo { nonce, balance: U256::from(balance), ..Default::default() }
}

/// A cache for the hand-made bundles. The tables are direct-mapped with a per-process hash seed,
/// so two keys share a bucket (and the later evicts the earlier) with chance 1/entries: 2^16
/// entries make that about 1 run in 65k, where 64 failed about 1 run in 64. Pages are touched
/// only as entries fill.
fn tiny() -> ExecutionCache {
    let entries = 1 << 16;
    ExecutionCache::with_code(
        CacheCapacity {
            accounts: entries,
            storage: entries,
            code: 4,
            wiped: DEFAULT_WIPED_ACCOUNTS,
        },
        None,
        &[],
    )
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

/// An account that existed at the anchor and was destroyed (or destroyed and re-created) had its
/// storage wiped: it joins the wiped set and loses its account entry, the block's slots of it are
/// not cached, and neither are its slots in later blocks. Other accounts keep their entries.
#[test]
fn insert_state_wipes_an_account_that_existed() {
    for status in
        [AccountStatus::Destroyed, AccountStatus::DestroyedChanged, AccountStatus::DestroyedAgain]
    {
        let mut cache = tiny();
        let (a, b) = (Address::repeat_byte(1), Address::repeat_byte(2));
        cache.accounts.insert(a, Some(Account::from(info(1, 0))));
        cache.storage.insert((a, key(1)), U256::from(5));
        cache.storage.insert((b, key(1)), U256::from(4));
        let wiped =
            BundleAccount::new(Some(info(1, 0)), Some(info(1, 0)), slots(&[(2, 0, 9)]), status);
        cache.insert_state(&bundle(vec![(a, wiped)])).expect("bundle applies");
        assert!(cache.is_wiped(&a), "{status:?}");
        assert!(!cache.is_wiped(&b));
        assert_eq!(cache.accounts.get(&a), None, "{status:?}: read from the parent state");
        assert_eq!(cache.storage.get(&(a, key(2))), None, "{status:?}: a wiped slot is not cached");
        assert_eq!(cache.storage.get(&(b, key(1))), Some(U256::from(4)));

        let changed = BundleAccount::new(
            Some(info(1, 0)),
            Some(info(2, 0)),
            slots(&[(3, 0, 8)]),
            AccountStatus::Changed,
        );
        cache.insert_state(&bundle(vec![(a, changed)])).expect("bundle applies");
        assert_eq!(cache.accounts.get(&a), Some(Some(Account::from(info(2, 0)))));
        assert_eq!(cache.storage.get(&(a, key(3))), None, "{status:?}: still wiped");
        assert_eq!(cache.stats().wiped_accounts, 1);
    }
}

/// The wiped set holds at most its limit: one more wipe refuses the bundle (the caller drops the
/// cache), while a wipe of an address already in it is no new entry.
#[test]
fn insert_state_refuses_a_wipe_past_the_limit() {
    // a seeded account (a codeless genesis account with storage) does not count toward the cap
    let mut cache = ExecutionCache::with_code(
        CacheCapacity { accounts: 1 << 16, storage: 1 << 16, code: 4, wiped: 2 },
        None,
        &[Address::repeat_byte(9)],
    );
    assert!(cache.is_wiped(&Address::repeat_byte(9)), "seeded");
    let wipe = |n: u8| {
        let destroyed =
            BundleAccount::new(Some(info(0, 1)), None, slots(&[]), AccountStatus::Destroyed);
        (Address::repeat_byte(n), destroyed)
    };
    cache.insert_state(&bundle(vec![wipe(1), wipe(2)])).expect("two fit");
    cache.insert_state(&bundle(vec![wipe(1)])).expect("already wiped");
    cache.insert_state(&bundle(vec![wipe(9)])).expect("the seed is already wiped");
    assert_eq!(
        cache.insert_state(&bundle(vec![wipe(3)])),
        Err(InsertStateError::WipedSetFull(Address::repeat_byte(3)))
    );
    assert_eq!(cache.stats().wiped_accounts, 3);
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
    let cache = ExecutionCache::with_code(
        CacheCapacity { accounts: 4, storage: 4, code: 4, wiped: DEFAULT_WIPED_ACCOUNTS },
        None,
        &[],
    );
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
/// An empty genesis account (no nonce, balance, code or storage): touching it deletes it
/// (EIP-161). Storage layout v2's genesis refuses storage without code; v1's accepts it.
const EMPTY_ADDRESS: Address = Address::repeat_byte(0xe0);
/// A second empty genesis account.
const EMPTY_ADDRESS_2: Address = Address::repeat_byte(0xe2);
/// A codeless genesis account with slot 1 set: storage layout v1 only.
const CODELESS_WITH_STORAGE: Address = Address::repeat_byte(0xe1);
/// [`CHILD_RUNTIME`] at a genesis address, for an EIP-7702 delegation.
const READER_ADDRESS: Address = Address::repeat_byte(0xd0);

/// The factory's child address (`CREATE2` with salt 0 over `child_init`).
fn child_address() -> Address {
    FACTORY_ADDRESS.create2(B256::ZERO, keccak256(child_init()))
}

/// The test genesis at [`GENESIS_TIMESTAMP`] with the counter, the factory and the empty accounts.
fn genesis() -> Genesis {
    with_test_accounts(test_genesis_at(GENESIS_TIMESTAMP))
}

/// [`genesis`] with a `ConsensusRegistry` of four validators, so blocks can close epochs. Builds
/// the registry through a temporary env (needs a tokio runtime).
fn registry_genesis() -> Genesis {
    let mut genesis = test_genesis_with_consensus_registry(4);
    genesis.timestamp = GENESIS_TIMESTAMP;
    with_test_accounts(genesis)
}

fn with_test_accounts(genesis: Genesis) -> Genesis {
    genesis.extend_accounts([
        (COUNTER_ADDRESS, GenesisAccount::default().with_code(Some(Bytes::from_static(COUNTER)))),
        (FACTORY_ADDRESS, GenesisAccount::default().with_code(Some(Bytes::from(factory_code())))),
        (EMPTY_ADDRESS, GenesisAccount::default()),
        (EMPTY_ADDRESS_2, GenesisAccount::default()),
    ])
}

/// The authority of the EIP-7702 tests: an account with no genesis entry.
fn authority() -> TransactionFactory {
    TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(7702))
}

/// One env on a fixed genesis with the counter and the factory, chaining blocks in memory as the
/// engine does within one consensus output.
struct TestChain {
    env: RethEnv,
    chain: Arc<RethChainSpec>,
    head: SealedHeader,
    index: u64,
    /// The epoch of the next block.
    epoch: u32,
    /// Whether the next block closes its epoch.
    close_next: bool,
    sender: TransactionFactory,
    _tmp: TempDir,
    _task_manager: TaskManager,
}

impl TestChain {
    fn new() -> eyre::Result<Self> {
        Self::on(genesis(), None)
    }

    /// A chain on `genesis`, its datadir in storage `layout` (the build's default when `None`).
    fn on(genesis: Genesis, layout: Option<StorageLayout>) -> eyre::Result<Self> {
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp = TempDir::new()?;
        let task_manager = TaskManager::new("exec cache test");
        let env = match layout {
            Some(layout) => RethEnv::new_for_temp_chain_with_layout(
                chain.clone(),
                tmp.path(),
                &task_manager,
                layout,
            )?,
            None => RethEnv::new_for_temp_chain(chain.clone(), tmp.path(), &task_manager, None)?,
        };
        let head = chain.sealed_genesis_header();
        Ok(Self {
            env,
            chain,
            head,
            index: 0,
            epoch: 0,
            close_next: false,
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

    /// An EIP-7702 transaction from the sender calling `authority`, with one authorization by
    /// `authority` (at its nonce `auth_nonce`) delegating it to `code` (`Address::ZERO` clears
    /// the delegation).
    fn set_code(
        &mut self,
        authority: &TransactionFactory,
        auth_nonce: u64,
        code: Address,
    ) -> eyre::Result<Vec<u8>> {
        let chain_id = self.chain.chain.id();
        let auth =
            Authorization { chain_id: U256::from(chain_id), address: code, nonce: auth_nonce };
        let signature = authority.get_default_signer()?.sign_hash_sync(&auth.signature_hash())?;
        let tx = TxEip7702 {
            chain_id,
            nonce: self.sender.nonce(),
            gas_limit: 1_000_000,
            max_fee_per_gas: MIN_PROTOCOL_BASE_FEE.into(),
            max_priority_fee_per_gas: 0,
            to: authority.address(),
            value: U256::ZERO,
            access_list: Default::default(),
            authorization_list: vec![auth.into_signed(signature)],
            input: Bytes::new(),
        };
        let signature = self.sender.get_default_signer()?.sign_hash_sync(&tx.signature_hash())?;
        self.sender.inc_nonce();
        Ok(TransactionSigned::new_unhashed(tx.into(), signature).encoded_2718())
    }

    /// Build a block on the head without chaining it: the next block builds on the same head.
    fn build_detached(
        &mut self,
        txs: Vec<Vec<u8>>,
        overlay: &mut OutputTrieOverlay,
    ) -> eyre::Result<ExecutedBlock> {
        let index = self.index + 1;
        let output = consensus_output_for_tests_at(
            index as u32,
            self.epoch,
            index,
            self.close_next,
            GENESIS_TIMESTAMP + index,
        );
        let seed = B256::from(U256::from(index));
        let payload = TNPayload::new_for_test_with(
            self.head.clone(),
            &output,
            Address::repeat_byte(0xbe),
            seed,
            seed,
        );
        Ok(self.env.build_block_from_batch_payload(payload, &txs, overlay)?)
    }

    /// Build a block on the head and chain it in memory (not persisted).
    fn build(
        &mut self,
        txs: Vec<Vec<u8>>,
        overlay: &mut OutputTrieOverlay,
    ) -> eyre::Result<ExecutedBlock> {
        let block = self.build_detached(txs, overlay)?;
        self.index += 1;
        if std::mem::take(&mut self.close_next) {
            self.epoch += 1;
        }
        let header = block.recovered_block.clone_sealed_header();
        let cims = self.env.canonical_in_memory_state();
        cims.update_chain(NewCanonicalChain::Commit { new: vec![block.clone()] });
        cims.set_canonical_head(header.clone());
        self.head = header;
        Ok(block)
    }

    /// Build one consensus output (one block per entry) and persist it.
    fn output_blocks(&mut self, blocks: Vec<Vec<Vec<u8>>>) -> eyre::Result<Vec<ExecutedBlock>> {
        let mut overlay = OutputTrieOverlay::new();
        let mut built = Vec::new();
        for txs in blocks {
            built.push(self.build(txs, &mut overlay)?);
        }
        self.env.finish_executing_output(built.clone(), None)?;
        self.env.finalize_block(self.head.clone())?;
        Ok(built)
    }

    /// [`Self::output_blocks`], returning the block hashes.
    fn output(&mut self, blocks: Vec<Vec<Vec<u8>>>) -> eyre::Result<Vec<B256>> {
        Ok(self.output_blocks(blocks)?.iter().map(|b| b.recovered_block.hash()).collect())
    }

    /// One output of one empty block that closes the current epoch.
    fn close_epoch(&mut self) -> eyre::Result<Vec<B256>> {
        self.close_next = true;
        self.output(vec![Vec::new()])
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

    fn account(&self, address: Address) -> eyre::Result<Option<Account>> {
        Ok(self.env.state_by_block_hash(self.head.hash())?.basic_account(&address)?)
    }

    /// Whether the env holds a dropped cache's bytecode table for its next cache.
    fn kept_code(&self) -> bool {
        self.env.inner.execution_cache.lock().code.is_some()
    }
}

/// Run `plan` on a chain with the cache off (the slot `TN_EXEC_CACHE=0` makes) and on one with
/// it, over the same `genesis`; both must build the same blocks (a block hash commits to the
/// block's state root). Returns the cached chain.
fn cached_and_uncached(
    genesis: &Genesis,
    plan: impl Fn(&mut TestChain) -> eyre::Result<Vec<B256>>,
) -> eyre::Result<TestChain> {
    cached_and_uncached_on(genesis, None, plan)
}

/// [`cached_and_uncached`] with both datadirs in storage `layout` (the build's default when
/// `None`).
fn cached_and_uncached_on(
    genesis: &Genesis,
    layout: Option<StorageLayout>,
    plan: impl Fn(&mut TestChain) -> eyre::Result<Vec<B256>>,
) -> eyre::Result<TestChain> {
    let mut uncached = TestChain::on(genesis.clone(), layout)?;
    uncached.env.disable_execution_cache_for_test();
    let expected = plan(&mut uncached)?;
    assert!(uncached.env.execution_cache_stats().is_none(), "the baseline ran without a cache");
    let mut cached = TestChain::on(genesis.clone(), layout)?;
    assert_eq!(plan(&mut cached)?, expected, "the cache changed a block");
    Ok(cached)
}

/// `TN_EXEC_CACHE`: unset or `1` keeps the cache; `0` turns it off, and so does any other value
/// (the cache-less build is the reference). An env switched off checks out no cache, so every read
/// goes to the parent state.
#[tokio::test]
async fn exec_cache_switch_turns_the_cache_off() -> eyre::Result<()> {
    for (value, disabled) in [
        (None, false),
        (Some("1"), false),
        (Some("0"), true),
        (Some("off"), true),
        (Some(""), true),
    ] {
        assert_eq!(ExecutionCacheSlot::from_env_value(value).disabled, disabled, "{value:?}");
    }
    let mut chain = TestChain::new()?;
    chain.env.disable_execution_cache_for_test();
    assert!(chain.env.check_out_execution_cache(chain.head.hash()).is_none());
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    assert!(chain.env.execution_cache_stats().is_none(), "no cache was saved");
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(1));
    Ok(())
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
/// deleted by EIP-161 when a transaction touches it) joins the wiped set; the cache survives and
/// later blocks read the account as gone.
#[tokio::test]
async fn wiped_existing_account_keeps_the_cache() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    assert!(chain.account(EMPTY_ADDRESS)?.is_some());
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;

    let touch = chain.tx(EMPTY_ADDRESS, 0, 100_000);
    let count = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![touch, count]])?;
    let after = chain.stats();
    assert_eq!(after.wiped_accounts, 1, "the cache survived the wipe");
    assert!(after.storage_hits > 0, "slot0 came from the cache in the wiping block");
    assert_eq!(chain.account(EMPTY_ADDRESS)?, None, "deleted");

    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    assert!(chain.stats().storage_hits > after.storage_hits);
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(3));
    Ok(())
}

/// The cheap forced wipe: fund a codeless address, then CREATE2 over it with init code that
/// self-destructs in its creation transaction. The cache survives with the address wiped; the
/// account is gone; created again with code, its slots read through the bypass, right.
#[tokio::test]
async fn create2_over_a_funded_address_keeps_the_cache() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    let child = child_address();
    let fund = chain.tx(child, 1, 100_000);
    let count = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![fund, count]])?;
    assert_eq!(chain.account(child)?.map(|a| a.balance), Some(U256::from(1)));

    let destroy = chain.tx(FACTORY_ADDRESS, 0, 1_000_000);
    let count = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![destroy, count]])?;
    let wiped = chain.stats();
    assert_eq!(wiped.wiped_accounts, 1, "the cache survived the wipe");
    assert_eq!(chain.slot(FACTORY_ADDRESS, 0)?, U256::from_be_slice(child.as_slice()));
    assert_eq!(chain.account(child)?, None, "created over and self-destructed: deleted");

    let create = chain.tx(FACTORY_ADDRESS, 1, 1_000_000);
    chain.output(vec![vec![create]])?;
    let before = chain.stats();
    let call = chain.call(child);
    chain.output(vec![vec![call]])?;
    let after = chain.stats();
    assert_eq!(chain.slot(child, 2)?, U256::from(7), "slot2 = 7 + 0");
    assert!(after.storage_bypasses >= before.storage_bypasses + 2, "slot0 and slot1 bypassed");
    assert!(after.account_hits > before.account_hits, "the cache still serves");
    Ok(())
}

/// The hazard the wiped set exists for, on storage v1 (whose genesis keeps storage on a codeless
/// account): an entry the cache holds for a slot of an account that is then wiped. The entry
/// stays in its bucket, but a read of the slot through the cache reads the state. (Such a genesis
/// account is in every new cache's wiped set from the start, "Codeless genesis storage"; the
/// wipe keeps it there.)
#[tokio::test]
async fn wiped_account_slots_read_the_state_not_the_cache() -> eyre::Result<()> {
    let genesis = genesis().extend_accounts([(
        CODELESS_WITH_STORAGE,
        GenesisAccount::default().with_storage(Some(BTreeMap::from([(
            B256::from(U256::from(1)),
            B256::from(U256::from(5)),
        )]))),
    )]);
    let mut chain = TestChain::on(genesis, Some(StorageLayout::V1))?;
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    assert_eq!(chain.slot(CODELESS_WITH_STORAGE, 1)?, U256::from(5));
    assert_eq!(chain.stats().wiped_accounts, 1, "seeded");
    // what a read of it would have left: its value at the anchor
    {
        let slot = chain.env.inner.execution_cache.lock();
        let saved = slot.saved.as_ref().expect("a saved cache");
        saved.cache.storage.insert((CODELESS_WITH_STORAGE, key(1)), U256::from(5));
    }

    let touch = chain.tx(CODELESS_WITH_STORAGE, 0, 100_000);
    chain.output(vec![vec![touch]])?;
    assert_eq!(chain.stats().wiped_accounts, 1);
    assert_eq!(chain.slot(CODELESS_WITH_STORAGE, 1)?, U256::ZERO, "the wipe cleared it");

    let cache = chain.env.check_out_execution_cache(chain.head.hash()).expect("a cache");
    assert_eq!(
        cache.storage.get(&(CODELESS_WITH_STORAGE, key(1))),
        Some(U256::from(5)),
        "the stale entry is still in its bucket"
    );
    let state = chain.env.state_by_block_hash(chain.head.hash())?;
    let provider = CachedStateProvider::new(&state, Some(&cache));
    assert_eq!(provider.storage(CODELESS_WITH_STORAGE, key(1))?, None);
    assert_eq!(cache.stats().storage_bypasses, 1);
    Ok(())
}

/// A wipe past the wiped set's limit drops the cache but keeps its code table: the next block
/// starts with empty account and storage tables and hits the code.
#[tokio::test]
async fn wiped_set_overflow_drops_the_cache_but_keeps_the_code() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    chain.env.inner.execution_cache.lock().capacity.wiped = 1;
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    let touch = chain.tx(EMPTY_ADDRESS, 0, 100_000);
    chain.output(vec![vec![touch]])?;
    assert_eq!(chain.stats().wiped_accounts, 1);

    let touch = chain.tx(EMPTY_ADDRESS_2, 0, 100_000);
    chain.output(vec![vec![touch]])?;
    assert!(chain.env.execution_cache_stats().is_none(), "the second wipe dropped the cache");
    assert!(chain.kept_code());
    assert_eq!(chain.account(EMPTY_ADDRESS_2)?, None);

    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    let fresh = chain.stats();
    assert_eq!((fresh.account_hits, fresh.storage_hits, fresh.wiped_accounts), (0, 0, 0));
    assert!(fresh.code_hits > 0, "the counter's code came from the kept table");
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(2));
    Ok(())
}

/// Transactions dropped as `InvalidTx` (a duplicate, an unfunded sender) leave their validation
/// reads in the cache; later blocks read the same accounts, fund the sender and run its
/// transaction, all through the cache.
#[tokio::test]
async fn invalid_tx_drops_then_reads_of_their_accounts() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    let mut poor = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(9));
    let valid = chain.call(COUNTER_ADDRESS);
    let unfunded = poor.create_eip1559_encoded(
        chain.chain.clone(),
        Some(100_000),
        MIN_PROTOCOL_BASE_FEE.into(),
        Some(COUNTER_ADDRESS),
        U256::ZERO,
        Bytes::new(),
    );
    let blocks = chain.output_blocks(vec![vec![valid.clone(), valid, unfunded.clone()]])?;
    assert_eq!(blocks[0].recovered_block.body().transactions.len(), 1, "two dropped");

    let fund = chain.tx(poor.address(), 1_000_000_000_000_000_000, 100_000);
    chain.output(vec![vec![fund]])?;
    let before = chain.stats();
    let blocks = chain.output_blocks(vec![vec![unfunded]])?;
    assert_eq!(blocks[0].recovered_block.body().transactions.len(), 1, "now funded");
    assert!(chain.stats().account_hits > before.account_hits);
    assert_eq!(chain.account(poor.address())?.map(|a| a.nonce), Some(1));
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(2));
    Ok(())
}

/// A block built and discarded (the engine's rollback of an unpersisted output), then a different
/// block on the same parent: the discarded block's cache is not used for it.
#[tokio::test]
async fn discarded_block_then_another_on_the_same_parent() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;

    let count = chain.call(COUNTER_ADDRESS);
    let count_again = chain.call(COUNTER_ADDRESS);
    let discarded =
        chain.build_detached(vec![count, count_again], &mut OutputTrieOverlay::new())?;
    let (anchor, _) = chain.env.execution_cache_stats().expect("saved");
    assert_eq!(anchor, discarded.recovered_block.hash());

    chain.sender.set_nonce(1);
    let pay = chain.tx(Address::repeat_byte(0x21), 5, 100_000);
    let count = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![pay, count]])?;
    let fresh = chain.stats();
    assert_eq!((fresh.account_hits, fresh.storage_hits), (0, 0), "a new cache on the parent");
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(2), "1 + 1, not the discarded 3");

    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    assert!(chain.stats().storage_hits > 0);
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(3));
    Ok(())
}

/// A failed persist clears the cache (keeping its code); the retry persists the same blocks, and
/// the chain continues on a new cache.
#[tokio::test]
async fn persist_fail_then_retry_then_continue() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;

    let mut overlay = OutputTrieOverlay::new();
    let tx = chain.call(COUNTER_ADDRESS);
    let first = chain.build(vec![tx], &mut overlay)?;
    let tx = chain.call(COUNTER_ADDRESS);
    let second = chain.build(vec![tx], &mut overlay)?;
    chain.env.inject_persist_provider_faults(1);
    chain.env.persist_executed_output(&[first.clone(), second.clone()]).expect_err("injected");
    assert!(chain.env.execution_cache_stats().is_none());
    assert!(chain.kept_code());

    chain.env.finish_executing_output(vec![first, second], None)?;
    chain.env.finalize_block(chain.head.clone())?;
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    let fresh = chain.stats();
    assert_eq!((fresh.account_hits, fresh.storage_hits), (0, 0));
    assert!(fresh.code_hits > 0, "the code table survived the drop");
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    assert!(chain.stats().storage_hits > 0);
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(5));
    Ok(())
}

/// The test hook for a stale hit: a wrong value planted in the saved cache, on the slot the next
/// block reads first, with the shadow check off for that cache and its storage hit count reset so
/// the read is a checked one. The build sees the stale hit, drops the cache (code included) and
/// returns the block a node without the cache builds.
#[tokio::test]
async fn stale_checked_hit_rebuilds_the_block_without_the_cache() -> eyre::Result<()> {
    let mut chain = TestChain::new()?;
    let mut twin = TestChain::new()?;
    twin.env.disable_execution_cache_for_test();
    for c in [&mut chain, &mut twin] {
        let tx = c.call(COUNTER_ADDRESS);
        c.output(vec![vec![tx]])?;
    }
    {
        let mut slot = chain.env.inner.execution_cache.lock();
        let cache = &mut slot.saved.as_mut().expect("a saved cache").cache;
        cache.shadow = false;
        cache.counters.storage_hits.store(0, Ordering::Relaxed);
        cache.storage.insert((COUNTER_ADDRESS, key(0)), U256::from(41));
    }

    for _ in 0..2 {
        let tx = chain.call(COUNTER_ADDRESS);
        let built = chain.output(vec![vec![tx]])?;
        let tx = twin.call(COUNTER_ADDRESS);
        assert_eq!(built, twin.output(vec![vec![tx]])?, "the uncached block");
        assert_eq!(chain.env.execution_cache_rebuilds(), 1, "one rebuild");
    }
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(3));
    Ok(())
}

/// Epoch-close blocks (system calls writing `ConsensusRegistry` slots) build through a warm cache
/// as without one, and a later close reads the earlier one's writes from the cache.
#[tokio::test]
async fn epoch_close_blocks_build_through_the_cache() -> eyre::Result<()> {
    let mut chain = cached_and_uncached(&registry_genesis(), |chain| {
        let tx = chain.call(COUNTER_ADDRESS);
        let mut hashes = chain.output(vec![vec![tx]])?;
        hashes.extend(chain.close_epoch()?);
        let tx = chain.call(COUNTER_ADDRESS);
        hashes.extend(chain.output(vec![vec![tx]])?);
        hashes.extend(chain.close_epoch()?);
        Ok(hashes)
    })?;
    let before = chain.stats();
    chain.close_epoch()?;
    assert!(chain.stats().storage_hits > before.storage_hits, "registry slots came from the cache");
    Ok(())
}

/// adiri builds: a pre-fork close, then the close of epoch `CONSENSUS_REGISTRY_FORK_EPOCH - 1`,
/// whose fork writes (the registry's and `WorkerConfigs`' code swapped, the registry's storage
/// migrated, all committed through `State`) land on a cache holding the pre-fork entries; the next
/// close runs the swapped code from the cache. The same blocks as without the cache.
#[cfg(feature = "adiri")]
#[tokio::test]
async fn fork_boundary_close_builds_through_the_cache() -> eyre::Result<()> {
    let plan = |chain: &mut TestChain| -> eyre::Result<Vec<B256>> {
        chain.epoch = tn_types::forks::CONSENSUS_REGISTRY_FORK_EPOCH - 2;
        let mut hashes = Vec::new();
        for _ in 0..3 {
            let tx = chain.call(COUNTER_ADDRESS);
            hashes.extend(chain.output(vec![vec![tx]])?);
            hashes.extend(chain.close_epoch()?);
        }
        Ok(hashes)
    };
    let chain = cached_and_uncached(&genesis(), plan)?;
    let stats = chain.stats();
    assert!(stats.code_hits > 0 && stats.storage_hits > 0, "{stats:?}");
    Ok(())
}

/// An EIP-7702 delegation to the counter, calls through it in later blocks and outputs, then the
/// delegation cleared: the same blocks as without the cache, the delegation's code and the
/// authority's slot read from the cache.
#[tokio::test]
async fn eip7702_delegation_reads_through_the_cache() -> eyre::Result<()> {
    let authority = authority();
    let plan = |chain: &mut TestChain| -> eyre::Result<Vec<B256>> {
        let delegate = chain.set_code(&authority, 0, COUNTER_ADDRESS)?;
        let mut hashes = chain.output(vec![vec![delegate]])?;
        let call = chain.call(authority.address());
        let call_again = chain.call(authority.address());
        hashes.extend(chain.output(vec![vec![call], vec![call_again]])?);
        let clear = chain.set_code(&authority, 1, Address::ZERO)?;
        let plain = chain.call(authority.address());
        hashes.extend(chain.output(vec![vec![clear, plain]])?);
        Ok(hashes)
    };
    let chain = cached_and_uncached(&genesis(), plan)?;
    // the delegated call ran the counter in the authority's storage (+2 over two blocks), and
    // set slot0 to 1 in the delegating transaction itself
    assert_eq!(chain.slot(authority.address(), 0)?, U256::from(3));
    let stats = chain.stats();
    assert!(stats.code_hits > 0 && stats.storage_hits > 0, "{stats:?}");
    let code_hash = chain.account(authority.address())?.map(|a| a.get_bytecode_hash());
    assert_eq!(code_hash, Some(keccak256([])), "the delegation is cleared");
    Ok(())
}

/// A storage-v1 genesis account with storage, no code and nonce 0, delegated by its key holder
/// (EIP-7702). revm marks such an account `InMemoryChange` once it changes, so until its output
/// persists reth's memory overlay answers 0 for every slot the output did not write, while the
/// database keeps the genesis values. The cache seeds the account into its wiped set, so its slots
/// read the parent state as a node without the cache does: 0 in the next block of the output (the
/// cache filled 5 from the database in the delegating block), 5 again in the next output (a cache
/// filled from the overlay would hold 0).
#[tokio::test]
async fn codeless_genesis_storage_reads_the_state_after_a_delegation() -> eyre::Result<()> {
    let authority = authority();
    let genesis = genesis().extend_accounts([
        (READER_ADDRESS, GenesisAccount::default().with_code(Some(Bytes::from(CHILD_RUNTIME)))),
        (
            authority.address(),
            GenesisAccount::default().with_storage(Some(BTreeMap::from([(
                B256::from(U256::from(1)),
                B256::from(U256::from(5)),
            )]))),
        ),
    ]);
    let plan = |chain: &mut TestChain| -> eyre::Result<Vec<B256>> {
        // the delegating transaction calls the authority too: slot2 = 0 + 5, read before the
        // account changes
        let delegate = chain.set_code(&authority, 0, READER_ADDRESS)?;
        let read = chain.call(authority.address());
        let mut hashes = chain.output(vec![vec![delegate], vec![read]])?;
        assert_eq!(chain.slot(authority.address(), 2)?, U256::ZERO, "slot2 = 0 + the overlay's 0");
        let read = chain.call(authority.address());
        hashes.extend(chain.output(vec![vec![read]])?);
        Ok(hashes)
    };
    let chain = cached_and_uncached_on(&genesis, Some(StorageLayout::V1), plan)?;
    assert_eq!(chain.slot(authority.address(), 2)?, U256::from(5), "slot2 = 0 + the database's 5");
    let stats = chain.stats();
    assert_eq!(stats.wiped_accounts, 1, "the seeded authority: {stats:?}");
    assert!(stats.storage_bypasses >= 2, "{stats:?}");
    Ok(())
}

/// Many outputs over every path above at once (multi-block outputs, wipes of both kinds, a
/// re-created child, `InvalidTx` drops, an EIP-7702 delegation, epoch closes) build the same
/// block hashes with the cache as without it.
#[tokio::test]
async fn cached_and_uncached_chains_build_the_same_blocks() -> eyre::Result<()> {
    let authority = authority();
    let child = child_address();
    let plan = |chain: &mut TestChain| -> eyre::Result<Vec<B256>> {
        let (r1, r2) = (Address::repeat_byte(0x31), Address::repeat_byte(0x32));
        let b1 = vec![
            chain.tx(r1, 1, 100_000),
            chain.call(COUNTER_ADDRESS),
            chain.tx(child, 1, 100_000),
        ];
        let b2 = vec![chain.call(COUNTER_ADDRESS), chain.tx(r2, 2, 100_000)];
        let mut hashes = chain.output(vec![b1, b2])?;

        let destroy = chain.tx(FACTORY_ADDRESS, 0, 1_000_000);
        let touch = chain.tx(EMPTY_ADDRESS, 0, 100_000);
        let count = chain.call(COUNTER_ADDRESS);
        hashes.extend(chain.output(vec![vec![destroy.clone(), destroy, touch, count]])?);
        hashes.extend(chain.close_epoch()?);

        let create = chain.tx(FACTORY_ADDRESS, 1, 1_000_000);
        let delegate = chain.set_code(&authority, 0, COUNTER_ADDRESS)?;
        let b1 = vec![create, delegate];
        let b2 = vec![chain.call(child), chain.call(authority.address())];
        hashes.extend(chain.output(vec![b1, b2])?);

        let b = vec![
            chain.call(child),
            chain.call(authority.address()),
            chain.set_code(&authority, 1, Address::ZERO)?,
            chain.call(authority.address()),
            chain.call(COUNTER_ADDRESS),
            chain.tx(EMPTY_ADDRESS_2, 0, 100_000),
        ];
        hashes.extend(chain.output(vec![b])?);
        hashes.extend(chain.close_epoch()?);
        let b = vec![chain.call(COUNTER_ADDRESS), chain.call(child), chain.tx(r1, 3, 100_000)];
        hashes.extend(chain.output(vec![b])?);
        Ok(hashes)
    };
    let chain = cached_and_uncached(&registry_genesis(), plan)?;
    let stats = chain.stats();
    assert_eq!(stats.wiped_accounts, 3, "the child and the two empty accounts: {stats:?}");
    assert!(stats.storage_bypasses > 0 && stats.storage_hits > 0 && stats.account_hits > 0);
    assert_eq!(chain.slot(child, 2)?, U256::from(7));
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(5));
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
    assert!(chain.kept_code(), "but not its code table");

    chain.sender.set_nonce(1);
    let tx = chain.call(COUNTER_ADDRESS);
    chain.output(vec![vec![tx]])?;
    let fresh = chain.stats();
    assert_eq!((fresh.account_hits, fresh.storage_hits), (0, 0), "a new, empty cache");
    assert_eq!(chain.slot(COUNTER_ADDRESS, 0)?, U256::from(2));
    Ok(())
}

/// A bundle `insert_state` refuses drops the cache instead of saving it, and keeps its code.
#[tokio::test]
async fn refused_bundle_drops_the_cache() -> eyre::Result<()> {
    let chain = TestChain::new()?;
    let parent = chain.head.hash();
    let cache = chain.env.check_out_execution_cache(parent).expect("a cache");
    chain.env.check_in_execution_cache(cache, B256::repeat_byte(1), &BundleState::default());
    assert!(chain.env.execution_cache_stats().is_some());

    let cache = chain.env.check_out_execution_cache(B256::repeat_byte(1)).expect("a cache");
    let a = Address::repeat_byte(1);
    let broken = BundleAccount::new(Some(info(1, 0)), None, slots(&[]), AccountStatus::Changed);
    chain.env.check_in_execution_cache(cache, B256::repeat_byte(2), &bundle(vec![(a, broken)]));
    assert!(chain.env.execution_cache_stats().is_none());
    assert!(chain.kept_code());
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
    assert!(chain.kept_code());
    Ok(())
}

/// A parent other than the anchor gets an empty cache; the anchor's own child gets the saved one.
#[tokio::test]
async fn parent_other_than_the_anchor_starts_empty() -> eyre::Result<()> {
    let chain = TestChain::new()?;
    let (anchor, other) = (B256::repeat_byte(1), B256::repeat_byte(2));
    let cache = chain.env.check_out_execution_cache(chain.head.hash()).expect("a cache");
    let a = Address::repeat_byte(9);
    let created = BundleAccount::new(
        None,
        Some(info(1, 1)),
        slots(&[(1, 0, 3)]),
        AccountStatus::InMemoryChange,
    );
    chain.env.check_in_execution_cache(cache, anchor, &bundle(vec![(a, created.clone())]));

    let cache = chain.env.check_out_execution_cache(other).expect("a cache");
    assert_eq!(cache.accounts.get(&a), None, "another parent: empty");
    assert_eq!(cache.storage.get(&(a, key(1))), None);
    assert!(chain.env.execution_cache_stats().is_none(), "the mismatched cache was dropped");

    chain.env.check_in_execution_cache(cache, anchor, &bundle(vec![(a, created)]));
    let cache = chain.env.check_out_execution_cache(anchor).expect("a cache");
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
