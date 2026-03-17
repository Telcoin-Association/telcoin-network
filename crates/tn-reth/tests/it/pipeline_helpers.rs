//! Shared test infrastructure for full-pipeline Telcoin precompile property tests.
//!
//! Unlike `tel_precompile_helpers.rs` which tests the precompile via a lightweight in-memory EVM,
//! this module exercises the full TN execution pipeline: transaction signing, EIP-2718 encoding,
//! signature recovery, block building, state persistence, and finalization.

use alloy::sol_types::SolCall;
use reth_revm::context::result::{ExecutionResult, Output};
use secp256k1::rand::{rngs::StdRng, SeedableRng as _};
use std::{collections::VecDeque, sync::Arc};
use tempfile::TempDir;
use tn_reth::{
    allowanceCall, balanceOfCall, totalSupplyCall, ExecutedBlock, NewCanonicalChain, RethChainSpec,
    RethEnv, TELCOIN_PRECOMPILE_ADDRESS,
};
use tn_types::{
    test_genesis, Address, BlsSignature, Bytes, Certificate, CommittedSubDag,
    ConsensusHeader, ConsensusOutput, GenesisAccount, ReputationScores, SealedHeader,
    SignatureVerificationState, TaskManager, U256, MIN_PROTOCOL_BASE_FEE,
};

use tn_reth::test_utils::TransactionFactory;

// --- Constants ---

/// Genesis total supply: 100 billion (before 10^18 scaling).
pub(crate) const GENESIS_SUPPLY: u128 = 100_000_000_000;

/// Timelock duration: 7 days in seconds.
pub(crate) const TIMELOCK_DURATION: u64 = 7 * 24 * 60 * 60; // 604800s

/// The governance safe address that the precompile checks for authorization.
pub(crate) const GOVERNANCE: Address = tn_config::GOVERNANCE_SAFE_ADDRESS;

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
    0x60, 0x00, 0x60, 0x00, 0x36, 0x60, 0x00, 0x34, // PUSH1 0 PUSH1 0 CALLDATASIZE PUSH1 0 CALLVALUE
    0x73, // PUSH20
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0xe1, // TELCOIN_PRECOMPILE_ADDRESS
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
    pub(crate) recipient_factory: TransactionFactory,
    /// Current canonical header (updated after each block).
    pub(crate) canonical_header: SealedHeader,
    /// Monotonically increasing block timestamp.
    block_timestamp: u64,
    /// Monotonically increasing subdag index for consensus output.
    subdag_index: u64,
    _tmp_dir: TempDir,
    _task_manager: TaskManager,
    _runtime: tokio::runtime::Runtime,
}

impl PipelineTestEnv {
    /// Create a new pipeline test environment with 3 funded EOAs and a governance forwarder.
    pub(crate) fn new() -> Self {
        let mut governance_factory =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(100));
        let mut user_factory =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(200));
        let mut recipient_factory =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(300));

        let large_balance =
            U256::from(10).pow(U256::from(18)) * U256::from(1_000_000_000u64); // 1B TEL
        let genesis_supply_wei =
            U256::from(GENESIS_SUPPLY) * U256::from(10).pow(U256::from(18));

        // Build genesis with funded accounts + governance forwarder + precompile storage
        let genesis = test_genesis().extend_accounts(vec![
            // Fund all 3 factory EOAs
            (governance_factory.address(), GenesisAccount::default().with_balance(large_balance)),
            (user_factory.address(), GenesisAccount::default().with_balance(large_balance)),
            (recipient_factory.address(), GenesisAccount::default().with_balance(large_balance)),
            // Deploy forwarder contract at GOVERNANCE_SAFE_ADDRESS
            (
                GOVERNANCE,
                GenesisAccount::default()
                    .with_balance(large_balance)
                    .with_code(Some(Bytes::from_static(GOVERNANCE_FORWARDER_BYTECODE))),
            ),
            // Precompile account with balance and total supply storage
            (
                TELCOIN_PRECOMPILE_ADDRESS,
                GenesisAccount::default()
                    .with_balance(U256::from(10).pow(U256::from(18)) * U256::from(1000u64))
                    .with_storage(Some({
                        let mut storage = std::collections::BTreeMap::new();
                        // Slot 100 = totalSupply
                        storage.insert(
                            tn_types::B256::from(U256::from(100)),
                            tn_types::B256::from(genesis_supply_wei),
                        );
                        storage
                    })),
            ),
        ]);

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
        recipient_factory.set_nonce(0);

        let block_timestamp = canonical_header.timestamp + 1;

        Self {
            reth_env,
            chain,
            governance_factory,
            user_factory,
            recipient_factory,
            canonical_header,
            block_timestamp,
            subdag_index: 1,
            _tmp_dir: tmp_dir,
            _task_manager: task_manager,
            _runtime: runtime,
        }
    }

    /// Execute a block containing the given encoded transactions.
    ///
    /// Mirrors the 5-step execute-finalize pipeline from `lib.rs:1513-1531`.
    pub(crate) fn execute_block(
        &mut self,
        txs: Vec<Vec<u8>>,
    ) -> eyre::Result<ExecutedBlock> {
        self.execute_block_at_timestamp(txs, self.block_timestamp)
    }

    /// Execute a block with a specific timestamp (for timelock tests).
    pub(crate) fn execute_block_at_timestamp(
        &mut self,
        txs: Vec<Vec<u8>>,
        timestamp: u64,
    ) -> eyre::Result<ExecutedBlock> {
        // 1. Create consensus output with controlled timestamp
        let output = consensus_output_for_test(
            self.subdag_index as u32,
            0,
            self.subdag_index,
            timestamp,
        );

        // 2. Build TNPayload
        let payload = tn_reth::payload::TNPayload::new_for_test(
            self.canonical_header.clone(),
            &output,
        );

        // 3. Build and execute block
        let anchor_hash = self.canonical_header.hash();
        let block = self.reth_env.build_block_from_batch_payload(
            payload,
            &txs,
            anchor_hash,
            &[],
        )?;

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
            Some(GOVERNANCE),
            U256::ZERO,
            Bytes::from(calldata),
        )
    }

    /// Create an encoded user tx targeting the precompile.
    pub(crate) fn user_precompile_tx(&mut self, calldata: Vec<u8>) -> Vec<u8> {
        Self::precompile_tx(&mut self.user_factory, &self.chain, calldata)
    }

    /// Create an encoded recipient tx targeting the precompile.
    pub(crate) fn recipient_precompile_tx(&mut self, calldata: Vec<u8>) -> Vec<u8> {
        Self::precompile_tx(&mut self.recipient_factory, &self.chain, calldata)
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

    /// Read allowance(owner, spender) from the precompile via system call.
    pub(crate) fn get_allowance(&self, owner: Address, spender: Address) -> U256 {
        let calldata = allowanceCall { owner, spender }.abi_encode();
        self.read_precompile_u256(calldata)
    }

    /// Read balanceOf(account) from the precompile via system call.
    pub(crate) fn get_precompile_balance(&self, account: Address) -> U256 {
        let calldata = balanceOfCall { account }.abi_encode();
        self.read_precompile_u256(calldata)
    }

    /// Execute a read-only system call to the precompile and decode a U256 result.
    fn read_precompile_u256(&self, calldata: Vec<u8>) -> U256 {
        let hash = self.canonical_header.hash();
        let result = self
            .reth_env
            .read_contract_state(hash, TELCOIN_PRECOMPILE_ADDRESS, Bytes::from(calldata))
            .expect("read_contract_state");
        match result.result {
            ExecutionResult::Success { output, .. } => match output {
                Output::Call(bytes) => {
                    assert!(bytes.len() >= 32, "output too short for U256");
                    U256::from_be_slice(&bytes[..32])
                }
                _ => panic!("unexpected output type"),
            },
            other => panic!("expected Success, got {other:?}"),
        }
    }

    /// Check if a transaction in the most recent block succeeded.
    /// Looks at the receipt status for the given tx index.
    pub(crate) fn tx_succeeded(&self, block: &ExecutedBlock, tx_index: usize) -> bool {
        block.execution_output.result.receipts[tx_index].success
    }
}

// --- Consensus output helper ---

/// Create a `ConsensusOutput` with a controlled timestamp.
///
/// Adapted from `lib.rs:1478-1510`.
fn consensus_output_for_test(
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
    leader.header_mut_for_test().created_at = timestamp;
    leader.header.round = round;
    leader.header.epoch = epoch;
    let reputation_scores = ReputationScores::default();
    let previous_sub_dag = None;
    let sub_dag = Arc::new(CommittedSubDag::new(
        vec![leader.clone(), Certificate::default()],
        leader,
        subdag_index,
        reputation_scores,
        previous_sub_dag,
    ));
    ConsensusOutput::new(
        sub_dag,
        ConsensusHeader::default().digest(),
        subdag_index,
        false,
        VecDeque::new(),
        Vec::new(),
    )
}
