//! Shared test infrastructure for full-pipeline Telcoin precompile property tests.
//!
//! Unlike `tel_precompile_helpers.rs` which tests the precompile via a lightweight in-memory EVM,
//! this module exercises the full TN execution pipeline: transaction signing, EIP-2718 encoding,
//! signature recovery, block building, state persistence, and finalization.

use alloy::sol_types::SolCall;
use reth_provider::{
    AccountReader as _, BlockHashReader as _, BytecodeReader as _, StateProvider as _,
};
use reth_revm::primitives::KECCAK_EMPTY;
use secp256k1::rand::{rngs::StdRng, SeedableRng as _};
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
};
use tempfile::TempDir;
use tn_config::GOVERNANCE_SAFE_ADDRESS;
#[cfg(not(feature = "faucet"))]
use tn_reth::mintCall;
use tn_reth::{
    error::TnRethResult,
    payload::TNPayload,
    recover_raw_transaction,
    system_calls::PRECOMPILE_GENESIS_BYTECODE,
    test_utils::{precompile_test_utils::GENESIS_SUPPLY, TransactionFactory},
    totalSupplyCall, ExecutedBlock, NewCanonicalChain, OutputBuildScope, OutputTrieOverlay,
    RethChainSpec, RethEnv, TELCOIN_PRECOMPILE_ADDRESS,
};
use tn_types::{
    test_genesis, Address, Batch, BlsSignature, Bytes, Certificate, CertifiedBatch,
    CommittedSubDag, ConsensusHeader, ConsensusOutput, GenesisAccount, Recovered, ReputationScores,
    SealedHeader, SignatureVerificationState, TaskManager, TransactionSigned, B256,
    MIN_PROTOCOL_BASE_FEE, U256,
};

// --- Concurrency limiter ---

/// Limits concurrent MDBX database environments to avoid exhausting OS memory-mapping limits
/// when `cargo test` runs all pipeline tests in parallel.
static DB_LIMIT: (Mutex<u32>, Condvar) = (Mutex::new(0), Condvar::new());
const MAX_CONCURRENT_DBS: u32 = 4;

struct DbPermit;

impl DbPermit {
    fn acquire() -> Self {
        let (lock, cvar) = &DB_LIMIT;
        let mut count = lock.lock().unwrap();
        while *count >= MAX_CONCURRENT_DBS {
            count = cvar.wait(count).unwrap();
        }
        *count += 1;
        DbPermit
    }
}

impl Drop for DbPermit {
    fn drop(&mut self) {
        let (lock, cvar) = &DB_LIMIT;
        *lock.lock().unwrap() -= 1;
        cvar.notify_one();
    }
}

// --- Constants ---

// GENESIS_SUPPLY imported from tn_reth::test_utils::precompile_test_utils

/// Minimal EVM contract that forwards any call (with value) to TELCOIN_PRECOMPILE_ADDRESS.
/// Deployed at GOVERNANCE_SAFE_ADDRESS so the precompile sees caller == governance.
///
/// Disassembly:
///   CALLDATASIZE PUSH1 0 PUSH1 0 CALLDATACOPY     // copy calldata to memory[0..]
///   PUSH1 0 PUSH1 0 CALLDATASIZE PUSH1 0 CALLVALUE // stack: value, 0, cdsize, 0, 0
///   PUSH20 <TELCOIN_PRECOMPILE_ADDRESS>             // target address 0x7e1
///   GAS CALL                                        // CALL(gas, addr, val, 0, cdsize, 0, 0)
///   RETURNDATASIZE PUSH1 0 PUSH1 0 RETURNDATACOPY  // copy return data to memory
///   ISZERO PUSH1 0x33 JUMPI                         // jump to revert on CALL failure
///   RETURNDATASIZE PUSH1 0 RETURN                   // success: return data
///   JUMPDEST RETURNDATASIZE PUSH1 0 REVERT          // failure: revert with return data
pub(crate) const GOVERNANCE_FORWARDER_BYTECODE: &[u8] = &[
    0x36, 0x60, 0x00, 0x60, 0x00, 0x37, // CALLDATASIZE PUSH1 0 PUSH1 0 CALLDATACOPY
    0x60, 0x00, 0x60, 0x00, 0x36, 0x60, 0x00,
    0x34, // PUSH1 0 PUSH1 0 CALLDATASIZE PUSH1 0 CALLVALUE
    0x73, // PUSH20
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x07, 0xe1, // TELCOIN_PRECOMPILE_ADDRESS
    0x5a, 0xf1, // GAS CALL
    0x3d, 0x60, 0x00, 0x60, 0x00, 0x3e, // RETURNDATASIZE PUSH1 0 PUSH1 0 RETURNDATACOPY
    0x15, 0x60, 0x33, 0x57, // ISZERO PUSH1 0x33 JUMPI
    0x3d, 0x60, 0x00, 0xf3, // RETURNDATASIZE PUSH1 0 RETURN
    0x5b, 0x3d, 0x60, 0x00, 0xfd, // JUMPDEST RETURNDATASIZE PUSH1 0 REVERT
];

// --- PipelineTestEnv ---

/// Full-pipeline test environment for TEL precompile property tests.
///
/// Mirrors the execution path used by `test_close_epochs` in `lib.rs`:
/// `TransactionFactory` → signed tx → `build_block_from_batch_payload` →
/// `finish_executing_output` → `finalize_block`.
pub(crate) struct PipelineTestEnv {
    pub(crate) reth_env: RethEnv,
    pub(crate) chain: Arc<RethChainSpec>,
    /// EOA that sends txs to the governance forwarder contract.
    pub(crate) governance_factory: TransactionFactory,
    /// Regular user EOA.
    pub(crate) user_factory: TransactionFactory,
    /// Transfer recipient EOA.
    #[cfg(feature = "faucet")]
    pub(crate) recipient_factory: TransactionFactory,
    /// Current canonical header (updated after each block).
    pub(crate) canonical_header: SealedHeader,
    /// Monotonically increasing block timestamp.
    pub(crate) block_timestamp: u64,
    /// Timestamp of the genesis block, the base for fixture timestamps.
    #[cfg(not(feature = "faucet"))]
    genesis_timestamp: u64,
    /// Monotonically increasing subdag index for consensus output.
    subdag_index: u64,
    _db_permit: DbPermit,
    _tmp_dir: TempDir,
    _task_manager: TaskManager,
    _runtime: tokio::runtime::Runtime,
}

impl PipelineTestEnv {
    /// Create a new pipeline test environment with 3 funded EOAs and a governance forwarder.
    pub(crate) fn new() -> Self {
        let large_balance = U256::from(10).pow(U256::from(18)) * U256::from(1_000_000_000u64); // 1B TEL
        let genesis_supply_wei = U256::from(GENESIS_SUPPLY) * U256::from(10).pow(U256::from(18));
        let precompile_balance = U256::from(10).pow(U256::from(18)) * U256::from(1000u64);

        Self::new_with_custom_state(
            genesis_supply_wei,
            precompile_balance,
            large_balance,
            large_balance,
            large_balance,
            large_balance,
        )
    }

    /// Create a new pipeline test environment with customizable genesis state.
    ///
    /// Parameters:
    /// - `total_supply`: value stored in slot 100 (already in wei)
    /// - `precompile_balance`: native balance of TELCOIN_PRECOMPILE_ADDRESS
    /// - `governance_safe_balance`: native balance of GOVERNANCE_SAFE_ADDRESS (forwarder)
    /// - `governance_eoa_balance`: native balance of governance factory EOA
    /// - `user_balance`: native balance of user factory EOA
    /// - `_recipient_balance`: native balance of recipient factory EOA (only used under `feature =
    ///   "faucet"`)
    pub(crate) fn new_with_custom_state(
        total_supply: U256,
        precompile_balance: U256,
        governance_safe_balance: U256,
        governance_eoa_balance: U256,
        user_balance: U256,
        _recipient_balance: U256,
    ) -> Self {
        let db_permit = DbPermit::acquire();

        let mut governance_factory =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(100));
        let mut user_factory =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(200));
        #[cfg(feature = "faucet")]
        let mut recipient_factory =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(300));

        // Build genesis with funded accounts + governance forwarder + precompile storage
        #[allow(unused_mut)]
        let mut accounts: Vec<(Address, GenesisAccount)> = vec![
            // Fund factory EOAs
            (
                governance_factory.address(),
                GenesisAccount::default().with_balance(governance_eoa_balance),
            ),
            (user_factory.address(), GenesisAccount::default().with_balance(user_balance)),
            // Deploy forwarder contract at GOVERNANCE_SAFE_ADDRESS
            (
                GOVERNANCE_SAFE_ADDRESS,
                GenesisAccount::default()
                    .with_balance(governance_safe_balance)
                    .with_code(Some(Bytes::from_static(GOVERNANCE_FORWARDER_BYTECODE))),
            ),
            // Precompile account with balance and total supply storage.
            //
            // `extend_accounts` replaces the canonical `0x7e1` entry inherited from
            // `test_genesis()` rather than merging into it, so the genesis code has to be restated
            // here. Without it the account is code-less, and once its balance reaches zero (a full
            // burn of the pool) it is empty by EIP-158 and gets cleared at the end of the block
            // that touched it - taking the total supply slot with it.
            (
                TELCOIN_PRECOMPILE_ADDRESS,
                GenesisAccount::default()
                    .with_balance(precompile_balance)
                    .with_code(Some(Bytes::from_static(PRECOMPILE_GENESIS_BYTECODE)))
                    .with_storage(Some({
                        let mut storage = std::collections::BTreeMap::new();
                        // Slot 100 = totalSupply
                        storage.insert(
                            tn_types::B256::from(U256::from(100)),
                            tn_types::B256::from(total_supply),
                        );
                        storage
                    })),
            ),
        ];
        #[cfg(feature = "faucet")]
        accounts.push((
            recipient_factory.address(),
            GenesisAccount::default().with_balance(_recipient_balance),
        ));
        let genesis = test_genesis().extend_accounts(accounts);

        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().expect("create temp dir");
        let task_manager = TaskManager::new("Pipeline Test");
        // RethEnv requires a Tokio runtime for the static file provider
        let runtime = tokio::runtime::Runtime::new().expect("create tokio runtime");
        let _guard = runtime.enter();
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .expect("create reth env");

        // Get the genesis header as starting canonical header
        let canonical_header = reth_env.canonical_tip();

        // Reset nonces since these are fresh accounts
        governance_factory.set_nonce(0);
        user_factory.set_nonce(0);
        #[cfg(feature = "faucet")]
        recipient_factory.set_nonce(0);

        let genesis_timestamp = canonical_header.timestamp;
        let block_timestamp = genesis_timestamp + 1;

        Self {
            reth_env,
            chain,
            governance_factory,
            user_factory,
            #[cfg(feature = "faucet")]
            recipient_factory,
            canonical_header,
            block_timestamp,
            #[cfg(not(feature = "faucet"))]
            genesis_timestamp,
            subdag_index: 1,
            _db_permit: db_permit,
            _tmp_dir: tmp_dir,
            _task_manager: task_manager,
            _runtime: runtime,
        }
    }

    /// The genesis block's timestamp in seconds.
    ///
    /// Fixtures that pick their own block timestamps offset them from this value so every
    /// block lands after genesis, as it does on a real chain.
    #[cfg(not(feature = "faucet"))]
    pub(crate) fn genesis_timestamp(&self) -> u64 {
        self.genesis_timestamp
    }

    /// Execute a block containing the given encoded transactions.
    ///
    /// Mirrors the 5-step execute-finalize pipeline from `lib.rs:1513-1531`.
    pub(crate) fn execute_block(&mut self, txs: Vec<Vec<u8>>) -> eyre::Result<ExecutedBlock> {
        self.execute_block_at_timestamp(txs, self.block_timestamp)
    }

    /// Execute a block with a specific timestamp (for timelock tests).
    ///
    /// `timestamp` is absolute and must not precede the parent's: from the sub-second timestamp
    /// fork on, execution raises it to the parent's timestamp (`evm_block_timestamp` in
    /// `crates/tn-reth/src/payload.rs`).
    pub(crate) fn execute_block_at_timestamp(
        &mut self,
        txs: Vec<Vec<u8>>,
        timestamp: u64,
    ) -> eyre::Result<ExecutedBlock> {
        self.build_and_persist(timestamp, |reth_env, payload| {
            reth_env.build_block_from_batch_payload(payload, &txs, &mut OutputTrieOverlay::new())
        })
    }

    /// Execute a block from transactions whose signers are already recovered (one batch's entry
    /// of `RethEnv::recover_output_signers`), the way the engine builds blocks, then persist it
    /// like [`Self::execute_block`].
    pub(crate) fn execute_recovered_block(
        &mut self,
        recovered: Vec<Recovered<TransactionSigned>>,
    ) -> eyre::Result<ExecutedBlock> {
        self.build_and_persist(self.block_timestamp, |reth_env, payload| {
            reth_env
                .build_block_from_recovered_payload(
                    payload,
                    recovered,
                    &mut OutputBuildScope::new(),
                )
                .map(|(block, _timings)| block)
        })
    }

    /// Build one block at `timestamp` with `build`, then chain and persist it.
    ///
    /// Each block persists in step 5 before the next build, so `build` passes a fresh empty
    /// overlay or build scope: the database then already holds the parent state, which makes
    /// both exact (#1301).
    fn build_and_persist(
        &mut self,
        timestamp: u64,
        build: impl FnOnce(&RethEnv, TNPayload) -> TnRethResult<ExecutedBlock>,
    ) -> eyre::Result<ExecutedBlock> {
        // 1. Create consensus output with controlled timestamp
        let output =
            consensus_output_for_test(self.subdag_index as u32, 0, self.subdag_index, timestamp);

        // 2. Build TNPayload
        let payload = TNPayload::new_for_test(self.canonical_header.clone(), &output);

        // 3. Build and execute block
        let block = build(&self.reth_env, payload)?;

        // 4. Update canonical in-memory state
        let canonical_header = block.recovered_block.clone_sealed_header();
        let cims = self.reth_env.canonical_in_memory_state();
        cims.update_chain(NewCanonicalChain::Commit { new: vec![block.clone()] });
        cims.set_canonical_head(canonical_header.clone());

        // 5. Persist state
        self.reth_env.finish_executing_output(vec![block.clone()], None)?;
        self.reth_env.finalize_block(canonical_header.clone())?;

        // Update env state
        self.canonical_header = canonical_header;
        self.block_timestamp = timestamp + 1;
        self.subdag_index += 1;

        Ok(block)
    }

    /// Create an encoded EIP-1559 tx targeting TELCOIN_PRECOMPILE_ADDRESS directly.
    pub(crate) fn precompile_tx(
        factory: &mut TransactionFactory,
        chain: &Arc<RethChainSpec>,
        calldata: Vec<u8>,
    ) -> Vec<u8> {
        factory.create_eip1559_encoded(
            chain.clone(),
            Some(1_000_000),
            MIN_PROTOCOL_BASE_FEE.into(),
            Some(TELCOIN_PRECOMPILE_ADDRESS),
            U256::ZERO,
            Bytes::from(calldata),
        )
    }

    /// Create an encoded EIP-1559 tx targeting GOVERNANCE_SAFE_ADDRESS (the forwarder).
    pub(crate) fn governance_tx(&mut self, calldata: Vec<u8>) -> Vec<u8> {
        self.governance_factory.create_eip1559_encoded(
            self.chain.clone(),
            Some(1_000_000),
            MIN_PROTOCOL_BASE_FEE.into(),
            Some(GOVERNANCE_SAFE_ADDRESS),
            U256::ZERO,
            Bytes::from(calldata),
        )
    }

    /// Create an encoded governance mint tx targeting the forwarder.
    #[cfg(not(feature = "faucet"))]
    pub(crate) fn governance_mint_tx(&mut self, amount: U256) -> Vec<u8> {
        self.governance_tx(mintCall { amount }.abi_encode())
    }

    /// Create an encoded user tx targeting the precompile.
    pub(crate) fn user_precompile_tx(&mut self, calldata: Vec<u8>) -> Vec<u8> {
        Self::precompile_tx(&mut self.user_factory, &self.chain, calldata)
    }

    /// Get the native TEL balance of an address via `retrieve_account`.
    pub(crate) fn get_balance(&self, addr: Address) -> U256 {
        self.reth_env
            .retrieve_account(&addr)
            .expect("retrieve account")
            .map(|a| a.balance)
            .unwrap_or(U256::ZERO)
    }

    /// Read totalSupply from the precompile via system call.
    pub(crate) fn get_total_supply(&self) -> U256 {
        let calldata = totalSupplyCall {}.abi_encode();
        self.read_precompile_u256(calldata)
    }

    /// Execute a read-only contract call to the precompile and decode a U256 result.
    fn read_precompile_u256(&self, calldata: Vec<u8>) -> U256 {
        let hash = self.canonical_header.hash();
        let bytes = self
            .reth_env
            .read_contract_at_block(hash, TELCOIN_PRECOMPILE_ADDRESS, Bytes::from(calldata))
            .expect("read contract");
        assert!(bytes.len() >= 32, "output too short for U256");
        U256::from_be_slice(&bytes[..32])
    }

    /// A pipeline environment whose governance EOA is an existing but EMPTY genesis account (no
    /// balance, nonce or code), so a zero-value transfer to it clears it under EIP-161. The
    /// governance EOA cannot pay for transactions here.
    pub(crate) fn new_with_empty_governance_eoa() -> Self {
        let wei = U256::from(10).pow(U256::from(18));
        let large = wei * U256::from(1_000_000_000u64);
        Self::new_with_custom_state(
            U256::from(GENESIS_SUPPLY) * wei,
            wei * U256::from(1000u64),
            large,
            U256::ZERO,
            large,
            large,
        )
    }

    /// Every entry of `scope`'s read cache that differs from the state at `block`, the parent
    /// state of the output's next block: one description per account, slot, bytecode or block
    /// hash, sorted. Empty when the cache is a subset of that state (the scope's invariant).
    ///
    /// Values are compared the way the cache serves them: account info as `(balance, nonce,
    /// code hash)` with neither code nor a storage-access hint (`account_id`), a missing slot as
    /// zero and missing bytecode as empty, as reth's state provider database reads them.
    pub(crate) fn cache_mismatches(
        &self,
        scope: &OutputBuildScope,
        block: &ExecutedBlock,
    ) -> eyre::Result<Vec<String>> {
        let provider = self.reth_env.state_by_block_hash_for_test(block.recovered_block.hash())?;
        let reads = scope.reads();
        let mut mismatches = Vec::new();
        for (address, cached) in &reads.accounts {
            // a provider read carries no code (revm loads it by hash) and no `account_id`
            let state = provider.basic_account(address)?.map(|account| {
                let code_hash = account.bytecode_hash.unwrap_or(KECCAK_EMPTY);
                (account.balance, account.nonce, code_hash, true, true)
            });
            let info = cached.info.as_ref().map(|info| {
                let (no_code, no_id) = (info.code.is_none(), info.account_id.is_none());
                (info.balance, info.nonce, info.code_hash, no_code, no_id)
            });
            if info != state {
                mismatches.push(format!("account {address}: cached {info:?}, state {state:?}"));
            }
            for (slot, value) in &cached.storage {
                let state = provider.storage(*address, B256::from(*slot))?.unwrap_or_default();
                if *value != state {
                    mismatches
                        .push(format!("{address} slot {slot}: cached {value}, state {state}"));
                }
            }
        }
        for (hash, code) in &reads.contracts {
            let state = provider.bytecode_by_hash(hash)?.unwrap_or_default().0;
            if *code != state {
                mismatches.push(format!("bytecode {hash} differs"));
            }
        }
        for (number, hash) in &reads.block_hashes {
            let state = provider.block_hash(*number)?.unwrap_or_default();
            if *hash != state {
                mismatches.push(format!("block hash {number}: cached {hash}, state {state}"));
            }
        }
        mismatches.sort();
        Ok(mismatches)
    }

    /// Build the next block of an UNPERSISTED output twice from one pinned payload, then chain it
    /// in memory only, as the engine does between the blocks of one consensus output.
    ///
    /// The first build goes through `build_block_from_batch_payload` with `overlay`, which starts
    /// from an empty read cache, so every state read reaches the parent state provider. The second
    /// goes through `build_block_from_recovered_payload` with `scope`, the output's shared build
    /// scope, whose read cache carries the earlier blocks' reads. Both carry the output's trie
    /// overlay. Returns `(fresh, shared)`; the fresh block is the one chained.
    pub(crate) fn build_unpersisted_fresh_and_shared(
        &mut self,
        txs: &Vec<Vec<u8>>,
        overlay: &mut OutputTrieOverlay,
        scope: &mut OutputBuildScope,
    ) -> eyre::Result<(ExecutedBlock, ExecutedBlock)> {
        let parent = self.canonical_header.clone();
        let index = self.subdag_index;
        let output = consensus_output_for_test(index as u32, 0, index, self.block_timestamp);
        // `new_for_test` randomizes the beneficiary, batch digest and mix hash: pin them so both
        // builds start from the same payload (one beneficiary for every block of the output)
        let payload = || {
            TNPayload::new_for_test_with(
                parent.clone(),
                &output,
                Address::with_last_byte(0xbe),
                B256::with_last_byte(index as u8),
                B256::with_last_byte(0x80 | index as u8),
            )
        };
        let fresh = self.reth_env.build_block_from_batch_payload(payload(), txs, overlay)?;
        let recovered = txs
            .iter()
            .map(|tx| recover_raw_transaction(tx))
            .collect::<TnRethResult<Vec<Recovered<TransactionSigned>>>>()?;
        let (shared, _timings) =
            self.reth_env.build_block_from_recovered_payload(payload(), recovered, scope)?;

        let header = fresh.recovered_block.clone_sealed_header();
        let cims = self.reth_env.canonical_in_memory_state();
        cims.update_chain(NewCanonicalChain::Commit { new: vec![fresh.clone()] });
        cims.set_canonical_head(header.clone());
        self.canonical_header = header;
        self.block_timestamp += 1;
        self.subdag_index += 1;
        Ok((fresh, shared))
    }

    /// Check if a transaction in the most recent block succeeded.
    /// Looks at the receipt status for the given tx index.
    pub(crate) fn tx_succeeded(&self, block: &ExecutedBlock, tx_index: usize) -> bool {
        block.execution_output.result.receipts[tx_index].success
    }

    /// Create an encoded EIP-1559 native value transfer (no calldata).
    #[cfg(feature = "faucet")]
    pub(crate) fn native_transfer_tx(
        factory: &mut TransactionFactory,
        chain: &Arc<RethChainSpec>,
        to: Address,
        amount: U256,
    ) -> Vec<u8> {
        factory.create_eip1559_encoded(
            chain.clone(),
            Some(1_000_000),
            MIN_PROTOCOL_BASE_FEE.into(),
            Some(to),
            amount,
            Bytes::new(),
        )
    }
}

// --- Read-cache differential helpers ---

/// The fields in which two builds of one block differ, among those consensus and the database
/// observe: block hash, state root, receipts root, gas used, receipts and bundle state. Empty when
/// the builds are identical.
pub(crate) fn block_differences(a: &ExecutedBlock, b: &ExecutedBlock) -> Vec<&'static str> {
    let (header_a, header_b) =
        (a.recovered_block.clone_sealed_header(), b.recovered_block.clone_sealed_header());
    [
        ("block hash", header_a.hash() == header_b.hash()),
        ("state root", header_a.state_root == header_b.state_root),
        ("receipts root", header_a.receipts_root == header_b.receipts_root),
        ("gas used", header_a.gas_used == header_b.gas_used),
        ("receipts", a.execution_output.result.receipts == b.execution_output.result.receipts),
        ("bundle state", a.execution_output.state == b.execution_output.state),
    ]
    .into_iter()
    .filter(|(_, same)| !same)
    .map(|(field, _)| field)
    .collect()
}

/// Creation code that deploys `runtime` (at most 255 bytes) as is.
pub(crate) fn deploy_code(runtime: &[u8]) -> Bytes {
    let len = u8::try_from(runtime.len()).expect("runtime of at most 255 bytes");
    // codecopy(0, 0x0a, len); return(0, len): the runtime starts after these 10 bytes
    let mut code = vec![0x60, len, 0x60, 0x0a, 0x5f, 0x39, 0x60, len, 0x5f, 0xf3];
    code.extend_from_slice(runtime);
    code.into()
}

/// Runtime of the CREATE2 factory: `create2(callvalue, calldata, salt 0)`, reverting when the
/// create fails, so the child's address depends only on its init code (the calldata).
pub(crate) const FACTORY_RUNTIME: [u8; 17] = [
    0x36, 0x5f, 0x5f, 0x37, // calldatacopy(0, 0, calldatasize)
    0x5f, 0x36, 0x5f, 0x34, 0xf5, // create2(callvalue, 0, calldatasize, 0)
    0x60, 0x0f, 0x57, // jumpi(0x0f, address)
    0x5f, 0x5f, 0xfd, // revert(0, 0)
    0x5b, 0x00, // jumpdest; stop
];

/// Runtime of the factory's child: `sstore(2, calldataload(0) + sload(1))`, so a call reads the
/// slot the child's init code wrote and writes a second one.
pub(crate) const CHILD_RUNTIME: [u8; 10] = [
    0x60, 0x01, 0x54, // sload(1)
    0x5f, 0x35, 0x01, // + calldataload(0)
    0x60, 0x02, 0x55, 0x00, // sstore(2, ..); stop
];

/// Init code of the factory's child `variant`. Without value it self-destructs at once (created
/// and destroyed in one transaction); with value it stores `sstore(1, callvalue)` and deploys
/// [`CHILD_RUNTIME`]. Equal init code means an equal CREATE2 address, so a child can be destroyed
/// and recreated at one address; the trailing `variant` byte (never executed) gives each variant
/// its own address.
pub(crate) fn child_init_code(variant: u8) -> Vec<u8> {
    let mut code = vec![
        0x34, 0x15, 0x60, 0x13, 0x57, // jumpi(0x13, iszero(callvalue))
        0x34, 0x60, 0x01, 0x55, // sstore(1, callvalue)
        0x60, 0x0a, 0x60, 0x16, 0x5f, 0x39, // codecopy(0, 0x16, 10)
        0x60, 0x0a, 0x5f, 0xf3, // return(0, 10)
        0x5b, 0x33, 0xff, // 0x13: jumpdest; selfdestruct(caller)
    ];
    code.extend_from_slice(&CHILD_RUNTIME);
    code.push(variant);
    code
}

// --- Consensus output helper ---

/// Create a `ConsensusOutput` with a controlled timestamp.
///
/// Adapted from `lib.rs:1478-1510`.
pub(crate) fn consensus_output_for_test(
    round: u32,
    epoch: u32,
    subdag_index: u64,
    timestamp: u64,
) -> ConsensusOutput {
    let mut leader = Certificate::default();
    leader.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
        BlsSignature::default(),
    ));
    // Set the timestamp on the leader's header (used by CommittedSubDag for block timestamp)
    leader.update_header_created_at_for_test(timestamp);
    leader.update_header_round_for_test(round);
    leader.update_header_epoch_for_test(epoch);
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = None;
    let sub_dag = CommittedSubDag::new(
        vec![Certificate::default(), leader.clone()],
        leader,
        subdag_index,
        reputation_scores,
        previous_sub_dag,
        tn_types::EpochSeedChainValue::genesis_placeholder(),
    );
    ConsensusOutput::new(
        sub_dag,
        ConsensusHeader::default().digest(),
        subdag_index,
        false,
        VecDeque::new(),
        Vec::new(),
    )
}

/// Wrap `consensus_output_for_test`'s sub-DAG in an output with one certificate per entry of
/// `certificates`, each carrying one batch per entry of its own (each the batch's encoded
/// transactions), with the digests in flattened order (certificate, then batch).
///
/// For signer recovery only: the batches carry default beneficiaries, base fees and epochs, and
/// certificate `i` is attributed to `Address::with_last_byte(i)`.
pub(crate) fn consensus_output_with_batches(
    subdag_index: u64,
    timestamp: u64,
    certificates: Vec<Vec<Vec<Vec<u8>>>>,
) -> ConsensusOutput {
    let base = consensus_output_for_test(subdag_index as u32, 0, subdag_index, timestamp);
    let certified: Vec<CertifiedBatch> = certificates
        .into_iter()
        .enumerate()
        .map(|(i, batches)| CertifiedBatch {
            address: Address::with_last_byte(i as u8),
            batches: batches
                .into_iter()
                .map(|transactions| Batch { transactions, ..Default::default() })
                .collect(),
        })
        .collect();
    let digests =
        certified.iter().flat_map(|cert| cert.batches.iter()).map(Batch::digest).collect();
    ConsensusOutput::new(
        base.sub_dag().clone(),
        ConsensusHeader::default().digest(),
        subdag_index,
        false,
        digests,
        certified,
    )
}

/// A built block retains no cumulative ancestor trie overlay (#1266).
///
/// No consumer on the node's path reads `anchored_trie_input`: persistence and
/// canonical-chain notifications use the per-block sorted deltas, and state roots and
/// RPC proofs over unpersisted blocks assemble their trie input from those same deltas.
/// Building the cumulative overlay cost `O(N^2 * M)` copy work and transient memory per
/// consensus output, because reth's parent-reuse fast path deep-copies it for every
/// block whose parent bundle is still alive.
#[test]
fn test_built_block_retains_no_cumulative_trie_overlay() -> eyre::Result<()> {
    let mut env = PipelineTestEnv::new();
    let first = env.execute_block(Vec::new())?;
    let second = env.execute_block(Vec::new())?;
    assert!(first.trie_data_handle().wait_cloned().anchored_trie_input.is_none());
    assert!(second.trie_data_handle().wait_cloned().anchored_trie_input.is_none());
    Ok(())
}
