//! This should allow for easier upgrades.
//! It still re-exports some stuff and a few places use Reth directly but eventually
//! it all should go through this crate.

#![doc(
    html_logo_url = "https://www.telco.in/logos/TEL.svg",
    html_favicon_url = "https://www.telco.in/logos/TEL.svg",
    issue_tracker_base_url = "https://github.com/telcoin-association/telcoin-network/issues/"
)]
#![warn(
    missing_debug_implementations,
    missing_docs,
    unreachable_pub,
    rustdoc::all,
    unused_crate_dependencies
)]
#![deny(unused_must_use, rust_2018_idioms)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

// Used in tests
#[cfg(test)]
mod clippy {
    use proptest as _;
    use tn_reth as _;
}

use alloy::primitives::ChainId;
use reth_chainspec::EthChainSpec;
use std::sync::{Arc, OnceLock};
use system_calls::SYSTEM_ADDRESS;
use tn_config::GOVERNANCE_SAFE_ADDRESS;
use tn_types::{Address, BlockBody, Genesis, SealedBlock, SealedHeader};

// Reth stuff we are just re-exporting.  Need to reduce this over time.
pub use alloy::primitives::FixedBytes;
pub use reth::{
    chainspec::chain_value_parser, dirs::MaybePlatformPath, payload::BlobSidecars,
    rpc::builder::RpcServerHandle,
};
pub use reth_chain_state::{
    CanonicalInMemoryState, DeferredTrieData, ExecutedBlock, NewCanonicalChain,
};
pub use reth_chainspec::ChainSpec as RethChainSpec;
pub use reth_cli_util::{parse_duration_from_secs, parse_socket_address};
pub use reth_db::{
    mdbx::{open_db_read_only, DatabaseArguments, Error as RethMdbxError},
    static_file::iter_static_files,
    Database as RethDatabaseT, DatabaseEnv, Tables,
};
pub use reth_errors::{ProviderError, RethError};
pub use reth_node_core::{
    args::{ColorMode, LogArgs},
    node_config::DEFAULT_PERSISTENCE_THRESHOLD,
};
pub use reth_primitives_traits::crypto::secp256k1::sign_message;
pub use reth_provider::{
    providers::StaticFileProvider, CanonStateNotificationStream, ChangedAccount,
};
pub use reth_rpc_eth_types::EthApiError;
pub use reth_tracing::{FileWorkerGuard, Layers};
pub use reth_transaction_pool::{
    error::{InvalidPoolTransactionError, PoolError, PoolTransactionError},
    identifier::SenderIdentifiers,
    BestTransactions, EthPooledTransaction, TransactionPool as TransactionPoolT,
};

mod cli;
pub mod dirs;
pub mod payload;
pub mod traits;
pub mod txn_pool;
pub use txn_pool::*;
mod env;
pub mod error;
mod evm;
pub mod forward;
mod metrics;
pub mod rpc_server_args;
pub mod snapshot;
pub mod system_calls;
mod types;
pub mod worker;
pub use cli::{
    init_txpool_defaults, RethCommand, RethConfig, TN_TXPOOL_MAX_ACCOUNT_SLOTS_PER_SENDER,
};
pub use env::*;
#[cfg(feature = "faucet")]
pub use evm::faucet_mint_role_slot;
#[cfg(not(feature = "faucet"))]
pub use evm::TIMELOCK_DURATION;
pub use evm::{
    add_bls_precompile, add_telcoin_precompile, burnCall, calculate_gas_penalty, claimCall,
    grantMintRoleCall, hasMintRoleCall, mintCall, revokeMintRoleCall, totalSupplyCall,
    BLS_G1_PRECOMPILE_ADDRESS, TELCOIN_PRECOMPILE_ADDRESS,
};
pub use forward::WorkerRpcForwarder;
pub use metrics::report_db_metrics;
pub use types::*;

#[cfg(any(feature = "test-utils", test))]
pub mod test_utils;

/// This will contain the address to receive base fees.  It is set per chain and
/// will not change.  Implemented as a static OnceLock to work around the Reth lib interface.
static BASEFEE_ADDRESS: OnceLock<Address> = OnceLock::new();

/// Return the chains basefee address if set.
/// Note the basefee address is set once for the chain and will not change (outside of a hard fork).
pub fn basefee_address() -> Address {
    *BASEFEE_ADDRESS.get().unwrap_or(&GOVERNANCE_SAFE_ADDRESS)
}

/// Wrapper for Reth ChainSpec, just a layer of abstraction.
#[derive(Clone, Debug)]
pub struct ChainSpec(Arc<RethChainSpec>);

impl ChainSpec {
    /// Return the contained Reth ChainSpec.
    pub(crate) fn reth_chain_spec(&self) -> RethChainSpec {
        (*self.0).clone()
    }

    /// Return a reference to the ChainSpec's genesis.
    pub fn genesis(&self) -> &Genesis {
        self.0.genesis()
    }

    /// Return the sealed header for genesis.
    pub fn sealed_genesis_header(&self) -> SealedHeader {
        self.0.sealed_genesis_header()
    }

    /// Return the sealed header for genesis.
    pub fn sealed_genesis_block(&self) -> SealedBlock {
        let header = self.sealed_genesis_header();
        let body = BlockBody {
            transactions: vec![],
            ommers: vec![],
            withdrawals: Some(Default::default()),
        };

        SealedBlock::from_sealed_parts(header, body)
    }

    /// Return the chain id.
    pub fn chain_id(&self) -> ChainId {
        self.0.chain_id()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;
    use crate::{
        payload::TNPayload,
        system_calls::ConsensusRegistry::ValidatorStatus,
        test_utils::{
            execute_payload_and_update_canonical_chain, governance_burn_tx,
            governance_owner_factory, test_genesis_with_consensus_registry, TransactionFactory,
        },
    };
    use alloy::primitives::utils::parse_ether;
    use rand::{rngs::StdRng, SeedableRng as _};
    use tempfile::TempDir;
    use tn_types::{
        generate_proof_of_possession_bls_for_test, keccak256, test_genesis, BlsKeypair,
        BlsSignature, Certificate, CommittedSubDag, ConsensusHeader, ConsensusOutput,
        Encodable2718 as _, NodeP2pInfo, ReputationScores, SignatureVerificationState,
    };

    /// Helper function for creating a consensus output for tests.
    fn consensus_output_for_tests(
        round: u32,
        epoch: u32,
        subdag_index: u64,
        close_epoch: bool,
    ) -> ConsensusOutput {
        let mut leader = Certificate::default();
        // set signature for deterministic test results
        leader.set_signature_verification_state(SignatureVerificationState::VerifiedDirectly(
            BlsSignature::default(),
        ));
        leader.update_header_created_at_for_test(tn_types::now());
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
        );
        ConsensusOutput::new(
            sub_dag,
            ConsensusHeader::default().digest(),
            subdag_index,
            close_epoch,
            VecDeque::new(),
            Vec::new(),
        )
    }

    /// Exercise the tx/receipt/feed read API against a persisted three-block chain:
    /// block 1 holds two transfers, block 2 is empty, block 3 holds one transfer.
    #[tokio::test]
    async fn test_read_api_tx_receipts_and_feed() -> eyre::Result<()> {
        let chain: Arc<RethChainSpec> = Arc::new(test_genesis().into());
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        let mut factory = TransactionFactory::new();
        let recipient = Address::from_slice(&[0xcc; 20]);
        let value = U256::from(1_000_000);
        let tx1 =
            factory.create_eip1559(chain.clone(), None, 100, Some(recipient), value, Bytes::new());
        let tx2 =
            factory.create_eip1559(chain.clone(), None, 100, Some(recipient), value, Bytes::new());
        let tx3 =
            factory.create_eip1559(chain.clone(), None, 100, Some(recipient), value, Bytes::new());

        // block 1: two transfers
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![tx1.encoded_2718(), tx2.encoded_2718()],
        )?;
        let block1_header = block1.recovered_block.clone_sealed_header();

        // block 2: empty
        let consensus_output = consensus_output_for_tests(2, 0, 2, false);
        let payload = TNPayload::new_for_test(block1_header.clone(), &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let block2_header = block2.recovered_block.clone_sealed_header();

        // block 3: one transfer
        let consensus_output = consensus_output_for_tests(2, 0, 3, false);
        let payload = TNPayload::new_for_test(block2_header, &consensus_output);
        let block3 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![tx3.encoded_2718()],
        )?;
        let block3_header = block3.recovered_block.clone_sealed_header();

        // tx-by-hash roundtrip for the second transaction in block 1
        let tx2_hash = *tx2.hash();
        let (recovered, meta) = reth_env
            .transaction_by_hash_with_meta(tx2_hash)?
            .expect("mined transaction found by hash");
        assert_eq!(recovered.signer(), factory.address());
        assert_eq!(*recovered.hash(), tx2_hash);
        assert_eq!(meta.block_number, 1);
        assert_eq!(meta.index, 1);
        assert_eq!(meta.block_hash, block1_header.hash());
        assert_eq!(meta.timestamp, block1_header.timestamp);
        // unknown hash is Ok(None), not an error
        assert!(reth_env.transaction_by_hash_with_meta(TxHash::random())?.is_none());

        // receipts by hash: cumulative gas is block-wide, per-tx gas is the delta
        let receipt = reth_env.receipt_by_hash(tx2_hash)?.expect("receipt for mined tx");
        assert!(receipt.success);
        let (receipt, gas_used) = reth_env
            .receipt_by_hash_with_gas_used(tx2_hash)?
            .expect("receipt with gas for mined tx");
        assert_eq!(gas_used, 21_000);
        assert_eq!(receipt.cumulative_gas_used, 42_000);
        // first-in-block transaction: per-tx gas equals its cumulative gas
        let (receipt, gas_used) = reth_env
            .receipt_by_hash_with_gas_used(*tx3.hash())?
            .expect("receipt with gas for mined tx");
        assert_eq!(gas_used, 21_000);
        assert_eq!(receipt.cumulative_gas_used, 21_000);

        // receipts by block: number- and hash-based reads agree
        let by_number =
            reth_env.receipts_by_block(BlockHashOrNumber::Number(1))?.expect("block 1 receipts");
        let by_hash = reth_env
            .receipts_by_block(BlockHashOrNumber::Hash(block1_header.hash()))?
            .expect("block 1 receipts by hash");
        assert_eq!(by_number.len(), 2);
        assert_eq!(by_number, by_hash);
        // known-but-empty block returns Some(empty); unknown block returns None
        assert_eq!(reth_env.receipts_by_block(BlockHashOrNumber::Number(2))?, Some(vec![]));
        assert!(reth_env.receipts_by_block(BlockHashOrNumber::Number(99))?.is_none());

        // chain-wide feed: three transactions total
        assert_eq!(reth_env.total_transactions()?, 3);
        let feed = reth_env.transactions_by_tx_range_with_meta(0..=2)?;
        assert_eq!(feed.len(), 3);
        let tx_numbers: Vec<_> = feed.iter().map(|entry| entry.tx_number).collect();
        assert_eq!(tx_numbers, vec![0, 1, 2]);
        for (entry, tx) in feed.iter().zip([&tx1, &tx2, &tx3]) {
            assert_eq!(*entry.transaction.hash(), *tx.hash());
            assert_eq!(entry.transaction.signer(), factory.address());
        }
        // entries 0-1 come from block 1
        assert_eq!(feed[0].block_number, 1);
        assert_eq!(feed[0].index, 0);
        assert_eq!(feed[0].block_hash, block1_header.hash());
        assert_eq!(feed[1].block_number, 1);
        assert_eq!(feed[1].index, 1);
        // entry 2 comes from block 3 — the empty block 2 is skipped entirely
        assert_eq!(feed[2].block_number, 3);
        assert_eq!(feed[2].index, 0);
        assert_eq!(feed[2].block_hash, block3_header.hash());
        assert_eq!(feed[2].timestamp, block3_header.timestamp);

        // ranges past the newest transaction clamp to what exists
        assert_eq!(reth_env.transactions_by_tx_range_with_meta(0..=99)?.len(), 3);
        // empty range yields an empty vec
        assert!(reth_env.transactions_by_tx_range_with_meta(RangeInclusive::new(1, 0))?.is_empty());

        // newest-first page of two: read a descending window and reverse it
        let total = reth_env.total_transactions()?;
        let mut page = reth_env.transactions_by_tx_range_with_meta(total - 2..=total - 1)?;
        page.reverse();
        let hashes: Vec<_> = page.iter().map(|entry| *entry.transaction.hash()).collect();
        assert_eq!(hashes, vec![*tx3.hash(), *tx2.hash()]);

        Ok(())
    }

    /// Minimal runtime bytecode: `PUSH1 42, PUSH1 0, MSTORE, PUSH1 32, PUSH1 0, RETURN` —
    /// returns `uint256(42)` for any calldata.
    const RETURN_42: &[u8] = &[0x60, 0x2a, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3];
    /// Minimal runtime bytecode: `PUSH1 0, PUSH1 0, REVERT` — reverts with empty output.
    const ALWAYS_REVERT: &[u8] = &[0x60, 0x00, 0x60, 0x00, 0xfd];

    /// Exercise account/code reads and the generic read-only contract call against two
    /// bytecode fixtures deployed in genesis.
    #[tokio::test]
    async fn test_read_api_account_code_and_contract_read() -> eyre::Result<()> {
        let return_42_addr = Address::from_slice(&[0xaa; 20]);
        let always_revert_addr = Address::from_slice(&[0xbb; 20]);
        let genesis = test_genesis().extend_accounts([
            (
                return_42_addr,
                GenesisAccount::default().with_code(Some(Bytes::from_static(RETURN_42))),
            ),
            (
                always_revert_addr,
                GenesisAccount::default().with_code(Some(Bytes::from_static(ALWAYS_REVERT))),
            ),
        ]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new()?;
        let task_manager = TaskManager::new("Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;
        let genesis_hash = chain.sealed_genesis_header().hash();

        let mut factory = TransactionFactory::new();
        let genesis_balance = reth_env
            .retrieve_account(&factory.address())?
            .expect("factory funded in genesis")
            .balance;

        // execute one transfer so the factory's nonce and balance move
        let recipient = Address::from_slice(&[0xcc; 20]);
        let transfer = factory.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(recipient),
            U256::from(1_000_000),
            Bytes::new(),
        );
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![transfer])?;

        // account state reflects the executed transfer
        let factory_account =
            reth_env.retrieve_account(&factory.address())?.expect("factory account exists");
        assert_eq!(factory_account.nonce, 1);
        assert!(factory_account.balance < genesis_balance);

        // genesis contract account carries the bytecode hash; unknown account is None
        let contract_account =
            reth_env.retrieve_account(&return_42_addr)?.expect("genesis contract account exists");
        assert_eq!(contract_account.bytecode_hash, Some(keccak256(RETURN_42)));
        assert!(reth_env.retrieve_account(&Address::from_slice(&[0xdd; 20]))?.is_none());

        // eth_getCode semantics: byte-exact for contracts, None for EOAs and unknowns
        assert_eq!(reth_env.account_code(&return_42_addr)?, Some(Bytes::from_static(RETURN_42)));
        assert!(reth_env.account_code(&factory.address())?.is_none());
        assert!(reth_env.account_code(&Address::from_slice(&[0xdd; 20]))?.is_none());

        // read-only contract call at the canonical tip
        let output = reth_env
            .read_contract(return_42_addr, Bytes::default())
            .expect("read-only call succeeds");
        assert_eq!(output.len(), 32);
        assert_eq!(U256::from_be_slice(&output), U256::from(42));

        // on-chain revert surfaces as EvmReadError::Revert with the raw output bytes
        let err = reth_env
            .read_contract(always_revert_addr, Bytes::default())
            .expect_err("reverting call must error");
        match err {
            EvmReadError::Revert { output, reason } => {
                assert!(output.is_empty());
                // alloy's `decode_revert_reason` treats any valid-UTF-8 output as a raw-string
                // reason (Vyper convention), so empty revert data yields `Some("")` rather than
                // `None`; assert there is no *meaningful* reason either way
                assert!(reason.as_deref().unwrap_or_default().is_empty());
            }
            other => panic!("expected revert error, got: {other:?}"),
        }

        // historical pinning: the same read against the genesis block's state
        let output = reth_env
            .read_contract_at_block(genesis_hash, return_42_addr, Bytes::default())
            .expect("read-only call at genesis succeeds");
        assert_eq!(U256::from_be_slice(&output), U256::from(42));

        Ok(())
    }

    /// In-protocol `ConsensusRegistry` fork over the PRE-fork testnet registry.
    ///
    /// `test_genesis()` embeds the committed testnet `genesis.yaml`, whose `ConsensusRegistry`
    /// account carries the pre-fork runtime code and validator storage with NO per-status sets —
    /// the exact on-chain shape the fork upgrades in place. The epoch-closing block that
    /// concludes `FORK_EPOCH - 1` swaps in the new runtime and runs `migrateValidatorSets()`
    /// FIRST, then the rewards + conclude calls run on the new code over the byte-identical
    /// preserved storage.
    ///
    /// Asserts: the pre-fork code does not answer the new-ABI eligible-count call; post-fork the
    /// new code is live and the migration populated a non-empty eligible set; a preserved BLS
    /// pubkey survives the swap as 96-byte compressed; and the fork block's `state_root` is
    /// identical across two independent executions (determinism — every node re-derives the
    /// same root).
    ///
    /// NOTE: the fixture is the LIVE pre-fork deployment, pinned by
    /// `tn_types::forks::CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH` and its tn-types pin test. If
    /// `chain-configs/testnet/genesis.yaml` is ever regenerated from the current (post-fork)
    /// artifact, the `pre.is_err()` probe below fails first — that means the fixture no longer
    /// mirrors the chain this fork targets; reassess the fork plan rather than updating the
    /// probes.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_consensus_registry_fork_swaps_code_and_migrates() -> eyre::Result<()> {
        // pre-fork fixture: old registry code + validator storage, no per-status sets
        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let genesis_header = chain.sealed_genesis_header();

        // fork fires when the concluding epoch + 1 == FORK_EPOCH
        let concluding_epoch = tn_types::forks::CONSENSUS_REGISTRY_FORK_EPOCH - 1;

        // one payload, cloned across both executions, so the determinism check compares
        // byte-identical inputs (`new_for_test` otherwise randomizes
        // beneficiary/mix_hash/digest per call)
        let output = consensus_output_for_tests(2, concluding_epoch, 1, true);
        let payload = TNPayload::new_for_test(genesis_header.clone(), &output);

        // --- env 1: pre-fork state must NOT answer the new-ABI eligible-count call (old code) ---
        let tmp1 = TempDir::new().unwrap();
        let tm1 = TaskManager::new("fork test env1");
        let env1 = RethEnv::new_for_temp_chain(chain.clone(), tmp1.path(), &tm1, None).unwrap();
        {
            let state = StateProviderDatabase::new(env1.latest()?);
            let mut cached = CachedReads::default();
            let mut db = State::builder()
                .with_database(cached.as_db_mut(state))
                .with_bundle_update()
                .build();
            let mut evm = env1
                .inner
                .evm_config
                .evm_factory()
                .create_evm(&mut db, env1.inner.evm_config.evm_env(genesis_header.header())?);
            let pre = env1.call_consensus_registry::<_, U256>(
                &mut evm,
                ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
            );
            assert!(
                pre.is_err(),
                "pre-fork registry (old code) must not expose getEligibleValidatorCount"
            );
        }

        // --- produce the fork boundary block on the production path ---
        let block = execute_payload_and_update_canonical_chain(&env1, payload.clone(), vec![])?;
        let header = block.recovered_block.clone_sealed_header();
        let produced_state_root = header.state_root;

        // --- post-fork: new code is live and the sets are migrated ---
        {
            let state = StateProviderDatabase::new(env1.latest()?);
            let mut cached = CachedReads::default();
            let mut db = State::builder()
                .with_database(cached.as_db_mut(state))
                .with_bundle_update()
                .build();
            let mut evm = env1
                .inner
                .evm_config
                .evm_factory()
                .create_evm(&mut db, env1.inner.evm_config.evm_env(header.header())?);

            let eligible = env1
                .call_consensus_registry::<_, U256>(
                    &mut evm,
                    ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
                )
                .expect("post-fork getEligibleValidatorCount must succeed on the swapped-in code");
            assert!(eligible > U256::ZERO, "migration must populate a non-zero eligible count");

            // getValidators(Active) now returns address[] (new ABI) from the migrated set
            let active = env1
                .call_consensus_registry::<_, Vec<Address>>(
                    &mut evm,
                    ConsensusRegistry::getValidatorsCall { status: ValidatorStatus::Active.into() }
                        .abi_encode()
                        .into(),
                )
                .expect("getValidators(Active) must succeed on new code");
            assert!(!active.is_empty(), "migrated Active set must be non-empty");

            // stored BLS pubkey survives the code swap untouched: still 96-byte compressed
            let bls = env1
                .call_consensus_registry::<_, Bytes>(
                    &mut evm,
                    ConsensusRegistry::getBlsPubkeyCall { validatorAddress: active[0] }
                        .abi_encode()
                        .into(),
                )
                .expect("getBlsPubkey must succeed");
            assert_eq!(bls.len(), 96, "preserved BLS pubkey must remain 96-byte compressed");
        }

        // --- determinism: an independent execution of the identical block yields the same root ---
        let tmp2 = TempDir::new().unwrap();
        let tm2 = TaskManager::new("fork test env2");
        let env2 = RethEnv::new_for_temp_chain(chain.clone(), tmp2.path(), &tm2, None).unwrap();
        let block2 = execute_payload_and_update_canonical_chain(&env2, payload, vec![])?;
        assert_eq!(
            block2.recovered_block.clone_sealed_header().state_root,
            produced_state_root,
            "fork block state_root must be identical across independent executions"
        );

        Ok(())
    }

    /// Pre-fork epoch conclusion over the LIVE adiri registry code must speak the legacy ABI.
    ///
    /// `test_genesis()` embeds the committed testnet `genesis.yaml` — the registry account
    /// carries the pre-fork runtime code (pinned by `CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH`)
    /// whose `getValidators(uint8)` returns `ValidatorInfo[]` and which has no
    /// `getValidatorsInfo`. Concluding a NORMAL (non-fork-boundary) epoch on this state
    /// exercises the code-hash gate in `read_committee_eligible_pool`: without the gate the
    /// close reverts fatally (the post-fork ABI is absent on-chain) — the exact path a fresh
    /// node onboarding to adiri or a full resync executes for every pre-fork epoch boundary.
    ///
    /// Asserts: the closing block executes; the registry code hash is untouched (no swap —
    /// epoch 3 is far from the fork boundary); the post-fork-only eligible-count call still
    /// fails; and the block's `state_root` is identical across two independent executions.
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_pre_fork_epoch_close_uses_legacy_registry_read() -> eyre::Result<()> {
        // pre-fork fixture: the committed adiri genesis (old registry code + validator storage)
        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let genesis_header = chain.sealed_genesis_header();

        // a normal epoch close, far from the fork boundary (`u32::MAX - 1`), so no swap can fire
        let concluding_epoch = 3;
        let output = consensus_output_for_tests(2, concluding_epoch, 1, true);
        let payload = TNPayload::new_for_test(genesis_header.clone(), &output);

        let tmp1 = TempDir::new().unwrap();
        let tm1 = TaskManager::new("legacy read test env1");
        let env1 = RethEnv::new_for_temp_chain(chain.clone(), tmp1.path(), &tm1, None).unwrap();

        // --- pre-probes: readable failures if the committed genesis fixture is regenerated ---
        {
            let state = StateProviderDatabase::new(env1.latest()?);
            let mut cached = CachedReads::default();
            let mut db = State::builder()
                .with_database(cached.as_db_mut(state))
                .with_bundle_update()
                .build();
            let mut evm = env1
                .inner
                .evm_config
                .evm_factory()
                .create_evm(&mut db, env1.inner.evm_config.evm_env(genesis_header.header())?);

            // legacy ABI: getValidators(Active) folds the committee-eligible pool into one
            // ValidatorInfo[] response
            let pool = env1
                .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
                    &mut evm,
                    ConsensusRegistry::getValidatorsCall { status: ValidatorStatus::Active.into() }
                        .abi_encode()
                        .into(),
                )
                .expect("pre-fork registry must answer the legacy getValidators(Active) read");
            assert!(!pool.is_empty(), "legacy eligible pool must be non-empty");

            let committee_size = env1
                .call_consensus_registry::<_, u16>(
                    &mut evm,
                    ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
                )
                .expect("pre-fork registry must answer getNextCommitteeSize");
            assert!(
                committee_size as usize <= pool.len(),
                "genesis fixture must hold enough eligible validators ({}) for the next \
                 committee ({committee_size}) — was chain-configs/testnet/genesis.yaml \
                 regenerated?",
                pool.len(),
            );
        }

        // --- the epoch-closing block executes via the legacy read (without the gate: fatal) ---
        let block = execute_payload_and_update_canonical_chain(&env1, payload.clone(), vec![])?;
        let header = block.recovered_block.clone_sealed_header();
        let produced_state_root = header.state_root;

        // --- post: still the pre-fork code — no swap fired, post-fork ABI still absent ---
        {
            use reth_provider::StateProvider as _;
            let code = env1
                .latest()?
                .account_code(&CONSENSUS_REGISTRY_ADDRESS)?
                .expect("registry account must have code");
            assert_eq!(
                code.0.hash_slow(),
                tn_types::forks::CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH,
                "a normal pre-fork epoch close must not swap the registry code"
            );

            let state = StateProviderDatabase::new(env1.latest()?);
            let mut cached = CachedReads::default();
            let mut db = State::builder()
                .with_database(cached.as_db_mut(state))
                .with_bundle_update()
                .build();
            let mut evm = env1
                .inner
                .evm_config
                .evm_factory()
                .create_evm(&mut db, env1.inner.evm_config.evm_env(header.header())?);
            let eligible = env1.call_consensus_registry::<_, U256>(
                &mut evm,
                ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
            );
            assert!(
                eligible.is_err(),
                "post-fork-only getEligibleValidatorCount must still fail on the pre-fork code"
            );
        }

        // --- determinism: an independent execution of the identical block yields the same root ---
        let tmp2 = TempDir::new().unwrap();
        let tm2 = TaskManager::new("legacy read test env2");
        let env2 = RethEnv::new_for_temp_chain(chain.clone(), tmp2.path(), &tm2, None).unwrap();
        let block2 = execute_payload_and_update_canonical_chain(&env2, payload, vec![])?;
        assert_eq!(
            block2.recovered_block.clone_sealed_header().state_root,
            produced_state_root,
            "pre-fork epoch-close state_root must be identical across independent executions"
        );

        Ok(())
    }

    /// The `ConsensusRegistry` fork must fail closed over an unexpected pre-fork deployment.
    ///
    /// The swap + `migrateValidatorSets()` assume the exact storage layout of the pinned
    /// pre-fork registry code. Here the genesis fixture's registry account is overwritten with
    /// the post-fork artifact bytes (any hash other than
    /// `CONSENSUS_REGISTRY_PRE_FORK_CODE_HASH` — a stand-in for an unknown deployment), and the
    /// fork-boundary block must abort with the fail-closed gate error instead of silently
    /// migrating over an unverified layout. (Without the gate this block would execute: the
    /// migration is idempotent on the new code, making this test the discriminating check.)
    #[cfg(feature = "adiri")]
    #[tokio::test]
    async fn test_consensus_registry_fork_fails_closed_on_unexpected_code() -> eyre::Result<()> {
        // overwrite the registry's code (keeping balance + storage) with the post-fork artifact
        let mut genesis = tn_types::test_genesis();
        let v2_value = RethEnv::fetch_value_from_json_str(
            CONSENSUS_REGISTRY_JSON,
            Some("deployedBytecode.object"),
        )?;
        let v2_code: Bytes =
            hex::decode(v2_value.as_str().expect("deployedBytecode.object is a string"))?.into();
        genesis
            .alloc
            .get_mut(&CONSENSUS_REGISTRY_ADDRESS)
            .expect("testnet genesis must allocate the ConsensusRegistry account")
            .code = Some(v2_code);

        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let genesis_header = chain.sealed_genesis_header();

        // drive the fork boundary: the concluding epoch + 1 == CONSENSUS_REGISTRY_FORK_EPOCH
        let concluding_epoch = tn_types::forks::CONSENSUS_REGISTRY_FORK_EPOCH - 1;
        let output = consensus_output_for_tests(2, concluding_epoch, 1, true);
        let payload = TNPayload::new_for_test(genesis_header.clone(), &output);

        let tmp = TempDir::new().unwrap();
        let tm = TaskManager::new("fail closed test");
        let env = RethEnv::new_for_temp_chain(chain.clone(), tmp.path(), &tm, None).unwrap();
        let err = execute_payload_and_update_canonical_chain(&env, payload, vec![])
            .expect_err("fork over an unexpected registry deployment must abort the block");
        assert!(
            format!("{err:#}").contains("failing closed"),
            "abort must come from the fail-closed code-hash gate, got: {err:#}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_close_epochs() -> eyre::Result<()> {
        let validator_1 = Address::from_slice(&[0x11; 20]);
        let validator_3 = Address::from_slice(&[0x33; 20]);
        let validator_4 = Address::from_slice(&[0x44; 20]);
        let validator_5 = Address::from_slice(&[0x55; 20]);

        // create validator wallet for staking later
        let mut new_validator_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));

        // create validator wallet for exiting later
        let mut validator_2_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(2));
        let validator_2_address = validator_2_eoa.address();

        // create initial validators for testing
        let all_validators = [
            validator_1,
            validator_2_address,
            validator_3,
            validator_4,
            validator_5,
            new_validator_eoa.address(),
        ];

        // create validator info objects for each address
        let mut validators: Vec<_> = all_validators
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                // use deterministic seed
                let mut rng = StdRng::seed_from_u64(i as u64);
                let bls = BlsKeypair::generate(&mut rng);
                let bls_pubkey = bls.public();
                let pop = generate_proof_of_possession_bls_for_test(&bls, addr)
                    .expect("pop generation failed");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls_pubkey,
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: pop,
                }
            })
            .collect();

        debug!(target: "engine", "created validators for consensus registry {:#?}", validators);

        let epoch_duration = 60 * 60 * 24; // 24hrs
        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
            minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
            epochIssuance: U256::from(parse_ether("20_000_000").unwrap())
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: epoch_duration,
        };

        // create genesis with funded governance safe
        let mut governance_multisig =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        let governance = governance_multisig.address();
        let tmp_genesis = tn_types::test_genesis().extend_accounts([
            (
                governance,
                GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)), // 50mil TEL
            ),
            (
                new_validator_eoa.address(),
                GenesisAccount::default()
                    .with_balance(initial_stake_config.stakeAmount.saturating_mul(U256::from(2))), // double stake
            ),
            (
                validator_2_address,
                GenesisAccount::default()
                    .with_balance(initial_stake_config.stakeAmount.saturating_mul(U256::from(2))), // double stake
            ),
        ]);

        // remove last validator so only 5 form the initial committees
        let new_validator = validators.pop().expect("six validators");

        // update genesis with consensus registry storage
        let genesis = RethEnv::create_consensus_registry_genesis_accounts(
            validators.clone(),
            tmp_genesis,
            initial_stake_config.clone(),
            governance,
            vec![(0u8, 30_000_000u64)],
        )?;

        // update genesis again to include stake for new validator
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let calldata =
            ConsensusRegistry::mintCall { validatorAddress: new_validator.execution_address }
                .abi_encode()
                .into();
        let mint_nft = governance_multisig.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            calldata,
        );
        let proof = ConsensusRegistry::ProofOfPossession {
            signature: new_validator.proof_of_possession.to_bytes().into(),
        };
        let calldata = ConsensusRegistry::stakeCall {
            blsPubkey: new_validator.bls_public_key.to_bytes().into(),
            proofOfPossession: proof,
        }
        .abi_encode()
        .into();
        let stake_tx = new_validator_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            initial_stake_config.stakeAmount,
            calldata,
        );
        let calldata = ConsensusRegistry::activateCall {}.abi_encode().into();
        let activate_tx = new_validator_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            calldata,
        );

        // create new env with initialized consensus registry for tests
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();
        let mut expected_epoch = 0;
        let expected_committee = validators.iter().map(|v| v.execution_address).collect();
        let mut expected_epoch_info = ConsensusRegistry::EpochInfo {
            committee: expected_committee,
            blockHeight: 0,
            epochId: 0,
            epochDuration: epoch_duration,
            epochIssuance: initial_stake_config.epochIssuance,
            stakeVersion: 0,
        };

        // assert epoch state is correct
        let EpochState { epoch, epoch_info, validators: committee, bls_pubkeys, epoch_start } =
            reth_env.epoch_state_from_canonical_tip()?;
        debug!(target:"evm", ?epoch, ?epoch_info, ?committee, ?epoch, "original epoch state from canonical tip in genesis");
        assert_eq!(epoch, expected_epoch);
        assert_eq!(epoch_start, chain.genesis_timestamp());
        assert_eq!(epoch_info, expected_epoch_info);

        // assert committee matches validator args for constructor
        for v in &validators {
            let idx = committee
                .iter()
                .position(|info| info.validatorAddress == v.execution_address)
                .expect("validator on-chain");
            assert_eq!(bls_pubkeys[idx].as_ref(), v.bls_public_key.to_bytes());
            let on_chain = &committee[idx];
            assert_eq!(on_chain.activationEpoch, epoch);
            assert_eq!(on_chain.exitEpoch, 0);
            assert!(!on_chain.isRetired);
            assert_eq!(on_chain.stakeVersion, 0);
        }

        // close epoch with deterministic signature as source of randomness
        // and execute the first block with txs for new validator to stake
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![mint_nft, stake_tx, activate_tx],
        )?;
        let canonical_header = block1.recovered_block.clone_sealed_header();

        // now close the first epoch
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 2, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block2.recovered_block.clone_sealed_header();

        // now close the second epoch so the new validator is active
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 3, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block3 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block3.recovered_block.clone_sealed_header();

        // read new epoch state
        let EpochState { epoch, epoch_info, validators: committee, bls_pubkeys, epoch_start } =
            reth_env.epoch_state_from_canonical_tip()?;
        debug!(target: "evm", ?epoch, ?epoch_info, ?committee, ?epoch, "new epoch state from canonical tip");
        // assert epoch info updated
        expected_epoch_info.blockHeight = 4;
        expected_epoch_info.epochId = expected_epoch as u32;
        assert_eq!(expected_epoch, epoch);
        assert_eq!(epoch_start, canonical_header.timestamp);
        assert_eq!(epoch_info, expected_epoch_info);

        // create evm to read custom contract call
        let state = StateProviderDatabase::new(reth_env.latest()?);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();
        let mut tn_evm = reth_env
            .evm_config()
            .evm_factory()
            .create_evm(&mut db, reth_env.evm_config().evm_env(canonical_header.header())?);

        // read new committee (always 2 epochs ahead)
        let calldata = ConsensusRegistry::getEpochInfoCall { epoch: epoch + 1 }.abi_encode().into();
        let new_epoch_info = reth_env
            .call_consensus_registry::<_, ConsensusRegistry::EpochInfo>(&mut tn_evm, calldata)?;

        // ensure validators in increasing order by address
        let expected_new_committee = vec![
            validator_1,
            validator_3,
            validator_4,
            validator_2_address,
            new_validator.execution_address,
        ];

        let expected = ConsensusRegistry::EpochInfo {
            committee: expected_new_committee,
            blockHeight: 0,
            epochId: (expected_epoch + 1) as u32,
            // epoch duration set at the start
            epochDuration: Default::default(),
            // values should remain the same
            epochIssuance: Default::default(),
            stakeVersion: 0,
        };

        debug!(target: "engine", "new epoch info:{:#?}", new_epoch_info);
        assert_eq!(new_epoch_info, expected);

        // assert new committee matches validator args for constructor
        // this should be the case for the first 3 epochs
        for v in &validators {
            let idx = committee
                .iter()
                .position(|info| info.validatorAddress == v.execution_address)
                .expect("validator on-chain");
            assert_eq!(bls_pubkeys[idx].as_ref(), v.bls_public_key.to_bytes());
            let on_chain = &committee[idx];
            assert_eq!(on_chain.activationEpoch, 0);
            assert_eq!(on_chain.exitEpoch, 0);
            assert!(!on_chain.isRetired);
            assert_eq!(on_chain.stakeVersion, 0);
        }

        // submit validator 2 exit request
        let calldata = ConsensusRegistry::beginExitCall {}.abi_encode().into();
        let begin_exit_tx = validator_2_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            calldata,
        );
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 4, false);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block4 =
            execute_payload_and_update_canonical_chain(&reth_env, payload, vec![begin_exit_tx])?;
        let canonical_header = block4.recovered_block.clone_sealed_header();

        // close epoch
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 5, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block5 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block5.recovered_block.clone_sealed_header();

        // create evm to read latest state
        let state = StateProviderDatabase::new(reth_env.latest()?);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();
        let mut tn_evm = reth_env
            .evm_config()
            .evm_factory()
            .create_evm(&mut db, reth_env.evm_config().evm_env(canonical_header.header())?);

        // assert validator 2 is pending exit
        let calldata =
            ConsensusRegistry::getValidatorCall { validatorAddress: validator_2_address }
                .abi_encode()
                .into();
        let validator_2_info = reth_env
            .call_consensus_registry::<_, ConsensusRegistry::ValidatorInfo>(
                &mut tn_evm,
                calldata,
            )?;
        debug!(target: "engine", ?validator_2_info, "getting validator 2 info");
        assert_eq!(validator_2_info.currentStatus, ValidatorStatus::PendingExit);

        // With the per-status sets, `getValidatorsInfo(Active)` returns strictly-active validators;
        // the committee-eligible pool is the union of Active/PendingActivation/PendingExit, so the
        // pending-exit validator is queried separately rather than partitioned out of `Active`.
        let active_validators = reth_env
            .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
                &mut tn_evm,
                ConsensusRegistry::getValidatorsInfoCall { status: ValidatorStatus::Active.into() }
                    .abi_encode()
                    .into(),
            )?;
        assert_eq!(active_validators.len(), 5);

        // validator 2 should be the single pending-exit validator
        let pending_exit = reth_env
            .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
                &mut tn_evm,
                ConsensusRegistry::getValidatorsInfoCall {
                    status: ValidatorStatus::PendingExit.into(),
                }
                .abi_encode()
                .into(),
            )?;
        assert_eq!(pending_exit.len(), 1);
        assert_eq!(
            pending_exit.first().expect("one pending validator").validatorAddress,
            validator_2_address
        );

        // close epoch again to exit validator
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 6, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block6 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block6.recovered_block.clone_sealed_header();
        // close epoch again
        expected_epoch += 1;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 7, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block7 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block7.recovered_block.clone_sealed_header();

        // create evm to read latest state
        let state = StateProviderDatabase::new(reth_env.latest()?);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database(cached_reads.as_db_mut(state))
            .with_bundle_update()
            .build();
        let mut tn_evm = reth_env
            .evm_config()
            .evm_factory()
            .create_evm(&mut db, reth_env.evm_config().evm_env(canonical_header.header())?);

        // assert validator 2 is pending exit
        let calldata =
            ConsensusRegistry::getValidatorCall { validatorAddress: validator_2_address }
                .abi_encode()
                .into();
        let validator_2_info = reth_env
            .call_consensus_registry::<_, ConsensusRegistry::ValidatorInfo>(
                &mut tn_evm,
                calldata,
            )?;
        debug!(target: "engine", ?validator_2_info, "getting validator 2 info");
        assert_eq!(validator_2_info.currentStatus, ValidatorStatus::Exited);

        // read all active validators from consensus registry
        let calldata =
            ConsensusRegistry::getValidatorsInfoCall { status: ValidatorStatus::Active.into() }
                .abi_encode()
                .into();
        let eligible_validators = reth_env
            .call_consensus_registry::<_, Vec<ConsensusRegistry::ValidatorInfo>>(
                &mut tn_evm,
                calldata,
            )?;

        assert_eq!(eligible_validators.len(), 5);

        // ensure validator 2 has fully exited
        let (pending_exit, active_validators): (Vec<_>, Vec<_>) = eligible_validators
            .into_iter()
            .partition(|v| v.currentStatus == ValidatorStatus::PendingExit.into());

        assert_eq!(pending_exit.len(), 0);
        assert_eq!(active_validators.len(), 5);
        for v in active_validators {
            assert!(v.validatorAddress != validator_2_address);
        }

        Ok(())
    }

    /// Guards the committee backfill path in `block.rs::shuffle_new_committee`: when there are
    /// fewer strictly-active validators than the committee size, the shuffle must backfill from
    /// the `PendingExit` pool so the next committee still reaches the required size. Five
    /// genesis validators form a committee of 5; two begin exiting, leaving 3 active + 2
    /// pending-exit. Since `committeeSize (5) > active (3)`, every subsequent committee must
    /// include the two exiting validators - otherwise `concludeEpoch` would revert on its
    /// committee-size check.
    #[tokio::test]
    async fn test_committee_backfill_from_pending_exit() -> eyre::Result<()> {
        // the two validators that begin exiting need EOAs to sign their `beginExit` txns
        let mut exit_a_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(101));
        let exit_a = exit_a_eoa.address();
        let mut exit_b_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(102));
        let exit_b = exit_b_eoa.address();

        let all_validators = [
            Address::from_slice(&[0x11; 20]),
            Address::from_slice(&[0x33; 20]),
            Address::from_slice(&[0x44; 20]),
            exit_a,
            exit_b,
        ];
        let validators: Vec<_> = all_validators
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let mut rng = StdRng::seed_from_u64(i as u64);
                let bls = BlsKeypair::generate(&mut rng);
                let pop = generate_proof_of_possession_bls_for_test(&bls, addr)
                    .expect("pop generation failed");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls.public(),
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: pop,
                }
            })
            .collect();

        let epoch_duration = 60 * 60 * 24;
        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
            minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
            epochIssuance: U256::from(parse_ether("20_000_000").unwrap())
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: epoch_duration,
        };

        let governance_multisig =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        let governance = governance_multisig.address();
        let tmp_genesis = tn_types::test_genesis().extend_accounts([
            (
                governance,
                GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
            ),
            (exit_a, GenesisAccount::default().with_balance(U256::from(parse_ether("1_000")?))),
            (exit_b, GenesisAccount::default().with_balance(U256::from(parse_ether("1_000")?))),
        ]);

        let genesis = RethEnv::create_consensus_registry_genesis_accounts(
            validators.clone(),
            tmp_genesis,
            initial_stake_config.clone(),
            governance,
            vec![(0u8, 30_000_000u64)],
        )?;
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());

        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Backfill Test Task Manager");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();

        // sanity: genesis committee is the full set of 5 validators
        let EpochState { epoch, validators: committee, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 0);
        assert_eq!(committee.len(), 5);

        // two validators begin exiting (Active -> PendingExit)
        let begin_exit_a = exit_a_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            ConsensusRegistry::beginExitCall {}.abi_encode().into(),
        );
        let begin_exit_b = exit_b_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            ConsensusRegistry::beginExitCall {}.abi_encode().into(),
        );

        // execute the exits in the first block (no epoch close yet)
        let mut expected_epoch = 0u32;
        let consensus_output = consensus_output_for_tests(2, expected_epoch, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![begin_exit_a, begin_exit_b],
        )?;
        let mut canonical_header = block1.recovered_block.clone_sealed_header();

        // close several epochs so the post-exit committees (computed 2 epochs ahead by the shuffle)
        // become current. If the backfill is broken, `concludeEpoch` reverts on the size check.
        for round in 2..=6u64 {
            expected_epoch += 1;
            let consensus_output = consensus_output_for_tests(2, expected_epoch, round, true);
            let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
            let block = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
            canonical_header = block.recovered_block.clone_sealed_header();

            // the committee must stay full at every close: active(3) < committeeSize(5) forces the
            // shuffle to backfill from the pending-exit pool each epoch
            let EpochState { validators: committee, .. } =
                reth_env.epoch_state_from_canonical_tip()?;
            assert_eq!(committee.len(), 5, "committee stays full via pending-exit backfill");
        }

        // with active(3) < committeeSize(5), the backfill must keep every committee full and
        // include the two pending-exit validators
        let EpochState { validators: committee, .. } = reth_env.epoch_state_from_canonical_tip()?;
        let committee_addrs: Vec<Address> = committee.iter().map(|v| v.validatorAddress).collect();
        assert_eq!(committee_addrs.len(), 5, "committee stays full via pending-exit backfill");
        assert!(committee_addrs.contains(&exit_a), "pending-exit validator A backfilled");
        assert!(committee_addrs.contains(&exit_b), "pending-exit validator B backfilled");

        // the backfilled validators remain PendingExit (still serving, not yet exited)
        for exiting in [exit_a, exit_b] {
            let info = reth_env.get_validator_info(canonical_header.hash(), exiting)?;
            assert_eq!(
                info.currentStatus,
                ConsensusRegistry::ValidatorStatus::PendingExit,
                "backfilled validator stays PendingExit"
            );
        }

        Ok(())
    }

    /// Governance `burn` forcibly ejects a current-committee validator mid-epoch.
    ///
    /// Pins the on-chain behavior the node's epoch-record layer must tolerate: the stored
    /// committee arrays for the current and both future epochs shrink via swap-and-pop (the
    /// last element moves into the ejected slot — order is NOT preserved), the next committee
    /// size auto-decrements to the eligible count, the validator is permanently retired with
    /// its stake confiscated, and the epoch still closes cleanly on-chain (`concludeEpoch` +
    /// `applyIncentives` system calls succeed over the shrunken committee). A direct
    /// `applyIncentives` call afterwards exercises the `isRetired` skip branch: the burned
    /// validator earns nothing while a surviving validator accrues rewards.
    #[tokio::test]
    async fn test_burn_ejects_current_committee_validator_mid_epoch() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Burn Eject Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // genesis committee of 5 for epoch 0
        let EpochState { epoch, validators: committee, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 0);
        assert_eq!(committee.len(), 5);

        // capture pre-burn committee pubkeys for the current and both future epochs
        let pre_burn = (0u32..=2)
            .map(|e| reth_env.bls_pubkeys_for_epoch(e))
            .collect::<eyre::Result<Vec<_>>>()?;

        // eject a middle slot so the swap-and-pop reorder is visible
        let target = committee[1].validatorAddress;
        let target_bls = pre_burn[0][1].clone();

        // pre-burn: full stake outstanding, no rewards
        let (outstanding, initial_stake, rewards) = reth_env
            .read_consensus_registry::<(U256, U256, U256)>(
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: target }
                    .abi_encode()
                    .into(),
            )?;
        assert_eq!(outstanding, initial_stake);
        assert!(rewards.is_zero());

        // block 1: governance burns the validator mid-epoch
        let mut governance = governance_owner_factory();
        let burn_tx = governance_burn_tx(&mut governance, chain.clone(), target);
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![burn_tx])?;
        let canonical_header = block1.recovered_block.clone_sealed_header();

        // the validator is permanently retired with its stake confiscated
        let retired = reth_env.read_consensus_registry::<bool>(
            ConsensusRegistry::isRetiredCall { validatorAddress: target }.abi_encode().into(),
        )?;
        assert!(retired, "burned validator is permanently retired");
        let (outstanding, _initial, rewards) = reth_env
            .read_consensus_registry::<(U256, U256, U256)>(
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: target }
                    .abi_encode()
                    .into(),
            )?;
        assert!(outstanding.is_zero(), "outstanding stake confiscated to issuance");
        assert!(rewards.is_zero());

        // current + both future committees shrink with EXACT swap-and-pop order: the last
        // element moves into the burned slot and the array truncates by one
        for e in 0u32..=2 {
            let post = reth_env.bls_pubkeys_for_epoch(e)?;
            let mut expected = pre_burn[e as usize].clone();
            let idx = expected
                .iter()
                .position(|k| k == &target_bls)
                .expect("burned validator in every pre-burn committee");
            let last = expected.len() - 1;
            expected.swap(idx, last);
            expected.truncate(last);
            assert_eq!(post.len(), 4);
            assert!(!post.contains(&target_bls));
            assert_eq!(post, expected, "swap-and-pop order for epoch {e}");
        }

        // positional zip pin: the address committee and pubkey committee stay index-aligned
        // (the node zips these arrays by position to build its committee)
        for e in 0u32..=2 {
            let infos = reth_env.validators_for_epoch(e)?;
            let keys = reth_env.bls_pubkeys_for_epoch(e)?;
            assert_eq!(infos.len(), keys.len());
            for (info, key) in infos.iter().zip(keys.iter()) {
                let direct =
                    reth_env.get_bls_pubkey(canonical_header.hash(), info.validatorAddress)?;
                assert_eq!(&direct, key, "epoch {e} committee arrays zip positionally");
            }
        }

        // committee size auto-decrements to the eligible count
        let next_size = reth_env.read_consensus_registry::<u16>(
            ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
        )?;
        assert_eq!(next_size, 4);
        let eligible = reth_env.read_consensus_registry::<U256>(
            ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
        )?;
        assert_eq!(eligible, U256::from(4));

        // block 2: close the epoch — the concludeEpoch + applyIncentives system calls must
        // succeed over the shrunken committee (on-chain close survives mid-epoch ejection)
        let consensus_output = consensus_output_for_tests(2, 1, 2, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let canonical_header = block2.recovered_block.clone_sealed_header();

        // post-close: epoch 1 runs the 4-member committee; the newly shuffled committee
        // (two epochs ahead) is also 4 members and excludes the burned validator
        let EpochState { epoch, validators: committee, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 1);
        assert_eq!(committee.len(), 4);
        assert!(committee.iter().all(|v| v.validatorAddress != target));
        let shuffled = reth_env.bls_pubkeys_for_epoch(3)?;
        assert_eq!(shuffled.len(), 4);
        assert!(!shuffled.contains(&target_bls));

        // direct applyIncentives with rewards for the burned + a surviving validator: the
        // isRetired branch skips the burned validator while the survivor accrues rewards
        let alive = committee[0].validatorAddress;
        let mut tn_evm = reth_env.tn_evm(canonical_header.hash())?;
        let calldata = ConsensusRegistry::applyIncentivesCall {
            rewardInfos: vec![
                ConsensusRegistry::RewardInfo {
                    validatorAddress: target,
                    consensusHeaderCount: U256::from(5),
                },
                ConsensusRegistry::RewardInfo {
                    validatorAddress: alive,
                    consensusHeaderCount: U256::from(5),
                },
            ],
        }
        .abi_encode()
        .into();
        let mut res =
            tn_evm.transact_system_call(SYSTEM_ADDRESS, CONSENSUS_REGISTRY_ADDRESS, calldata)?;
        assert!(res.result.is_success(), "applyIncentives succeeds: {:?}", res.result);
        res.state.remove(&SYSTEM_ADDRESS);
        tn_evm.db_mut().commit(res.state);
        let burned_rewards = reth_env.call_consensus_registry::<_, U256>(
            &mut tn_evm,
            ConsensusRegistry::getRewardsCall { validatorAddress: target }.abi_encode().into(),
        )?;
        let alive_rewards = reth_env.call_consensus_registry::<_, U256>(
            &mut tn_evm,
            ConsensusRegistry::getRewardsCall { validatorAddress: alive }.abi_encode().into(),
        )?;
        assert!(burned_rewards.is_zero(), "retired validator skipped by applyIncentives");
        assert!(alive_rewards > U256::ZERO, "surviving validator accrues rewards");

        Ok(())
    }

    /// Governance `burn` of a validator seated only in FUTURE committees leaves the current
    /// committee untouched.
    ///
    /// A sixth validator stakes and activates, so committees shuffled after its activation may
    /// seat it while the current committee predates it. Burning it mid-epoch mutates only the
    /// future committee arrays it occupies (swap-and-pop), leaves the running committee
    /// byte-identical (so the node's epoch-record comparison cannot diverge for future-only
    /// ejection, even without the mid-epoch-ejection tolerance fix), keeps
    /// `nextCommitteeSize` at 5 (eligible count drops 6 -> 5, so no auto-decrement fires),
    /// and the following epochs close cleanly.
    #[tokio::test]
    async fn test_burn_future_only_validator() -> eyre::Result<()> {
        // the sixth validator's EOA signs its own stake + activate txs
        let mut newval_eoa =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));
        let newval_addr = newval_eoa.address();
        // BLS seeds 0..=4 are taken by the 5 genesis validators
        let newval_bls = BlsKeypair::generate(&mut StdRng::seed_from_u64(5));
        let newval_pop = generate_proof_of_possession_bls_for_test(&newval_bls, &newval_addr)
            .expect("pop generation failed");

        let stake_amount = U256::from(parse_ether("1_000_000")?);
        let genesis = test_genesis_with_consensus_registry(5).extend_accounts([(
            newval_addr,
            GenesisAccount::default().with_balance(stake_amount.saturating_mul(U256::from(2))),
        )]);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Future Only Burn Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // block 1 (epoch 0): governance mints the NFT, the new validator stakes and activates
        let mut governance = governance_owner_factory();
        let mint_tx = governance.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            ConsensusRegistry::mintCall { validatorAddress: newval_addr }.abi_encode().into(),
        );
        let stake_tx = newval_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            stake_amount,
            ConsensusRegistry::stakeCall {
                blsPubkey: newval_bls.public().to_bytes().into(),
                proofOfPossession: ConsensusRegistry::ProofOfPossession {
                    signature: newval_pop.to_bytes().into(),
                },
            }
            .abi_encode()
            .into(),
        );
        let activate_tx = newval_eoa.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(CONSENSUS_REGISTRY_ADDRESS),
            U256::ZERO,
            ConsensusRegistry::activateCall {}.abi_encode().into(),
        );
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![mint_tx, stake_tx, activate_tx],
        )?;
        let mut canonical_header = block1.recovered_block.clone_sealed_header();

        // six validators are now committee-eligible (5 active + 1 pending activation)
        let eligible = reth_env.read_consensus_registry::<U256>(
            ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
        )?;
        assert_eq!(eligible, U256::from(6));

        // close epochs until some validator sits in a future committee but not the current
        // one (with 6 eligible validators and 5 seats every shuffled committee excludes
        // exactly one; the shuffle is seed-deterministic so the arrangement is stable)
        let committee_addrs = |e: u32| -> eyre::Result<Vec<Address>> {
            Ok(reth_env.validators_for_epoch(e)?.into_iter().map(|v| v.validatorAddress).collect())
        };
        let mut current_epoch = 0u32;
        let mut subdag_index = 2u64;
        let mut arrangement = None;
        while arrangement.is_none() && current_epoch < 6 {
            current_epoch += 1;
            let consensus_output = consensus_output_for_tests(2, current_epoch, subdag_index, true);
            subdag_index += 1;
            let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
            let block = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
            canonical_header = block.recovered_block.clone_sealed_header();

            let current = committee_addrs(current_epoch)?;
            let future: Vec<Address> = committee_addrs(current_epoch + 1)?
                .into_iter()
                .chain(committee_addrs(current_epoch + 2)?)
                .collect();
            // prefer the never-seated new validator; any future-only member proves the property
            arrangement = if !current.contains(&newval_addr) && future.contains(&newval_addr) {
                Some(newval_addr)
            } else {
                future.iter().copied().find(|v| !current.contains(v))
            };
        }
        let target = arrangement
            .expect("deterministic shuffle seats a validator in a future committee only");
        let w = current_epoch;
        let target_bls = reth_env.get_bls_pubkey(canonical_header.hash(), target)?;

        // pre-burn snapshots of the running + both future committees
        let pre_current = reth_env.bls_pubkeys_for_epoch(w)?;
        let pre_next = reth_env.bls_pubkeys_for_epoch(w + 1)?;
        let pre_subsequent = reth_env.bls_pubkeys_for_epoch(w + 2)?;
        assert!(!pre_current.contains(&target_bls));
        assert!(
            pre_next.contains(&target_bls) || pre_subsequent.contains(&target_bls),
            "target seated in a future committee"
        );
        let pre_next_size = reth_env.read_consensus_registry::<u16>(
            ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
        )?;
        assert_eq!(pre_next_size, 5);

        // burn the future-only validator mid-epoch W
        let burn_tx = governance_burn_tx(&mut governance, chain.clone(), target);
        let consensus_output = consensus_output_for_tests(2, w, subdag_index, false);
        subdag_index += 1;
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![burn_tx])?;
        canonical_header = block.recovered_block.clone_sealed_header();

        // the running committee is byte-identical: future-only ejection cannot perturb the
        // current epoch (so on-chain reads for epoch W match any pre-burn snapshot exactly)
        assert_eq!(reth_env.bls_pubkeys_for_epoch(w)?, pre_current);

        // future committees shrink via swap-and-pop exactly where the target was seated
        for (e, pre) in [(w + 1, &pre_next), (w + 2, &pre_subsequent)] {
            let post = reth_env.bls_pubkeys_for_epoch(e)?;
            if let Some(idx) = pre.iter().position(|k| k == &target_bls) {
                let mut expected = pre.to_vec();
                let last = expected.len() - 1;
                expected.swap(idx, last);
                expected.truncate(last);
                assert_eq!(post, expected, "swap-and-pop order for future epoch {e}");
            } else {
                assert_eq!(&post, pre, "future committee {e} untouched");
            }
            assert!(!post.contains(&target_bls));
        }

        // no auto-decrement: 5 remaining eligible validators still cover committee size 5
        let post_next_size = reth_env.read_consensus_registry::<u16>(
            ConsensusRegistry::getNextCommitteeSizeCall {}.abi_encode().into(),
        )?;
        assert_eq!(post_next_size, 5);
        let eligible = reth_env.read_consensus_registry::<U256>(
            ConsensusRegistry::getEligibleValidatorCountCall {}.abi_encode().into(),
        )?;
        assert_eq!(eligible, U256::from(5));
        let retired = reth_env.read_consensus_registry::<bool>(
            ConsensusRegistry::isRetiredCall { validatorAddress: target }.abi_encode().into(),
        )?;
        assert!(retired);
        let (outstanding, _initial, rewards) = reth_env
            .read_consensus_registry::<(U256, U256, U256)>(
                ConsensusRegistry::getBalanceBreakdownCall { validatorAddress: target }
                    .abi_encode()
                    .into(),
            )?;
        assert!(outstanding.is_zero(), "outstanding stake confiscated to issuance");
        assert!(rewards.is_zero());

        // the epoch closes cleanly and the next two committees seat without the target,
        // including the (possibly shrunken) subsequent committee going current
        for close in [w + 1, w + 2] {
            let consensus_output = consensus_output_for_tests(2, close, subdag_index, true);
            subdag_index += 1;
            let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
            let block = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
            canonical_header = block.recovered_block.clone_sealed_header();

            let EpochState { epoch, validators: committee, .. } =
                reth_env.epoch_state_from_canonical_tip()?;
            assert_eq!(epoch, close);
            assert!(committee.iter().all(|v| v.validatorAddress != target));
        }

        Ok(())
    }

    /// `epoch_state_at_epoch_start` pins every read to the previous epoch's closing block, so a
    /// mid-epoch governance `burn` — which swap-and-pops the CURRENT epoch's stored committee
    /// arrays immediately — moves the canonical-tip view but never the epoch-start view. A node
    /// entering (or re-entering) the epoch before or after the burn derives the identical
    /// committee.
    #[tokio::test]
    async fn epoch_state_at_epoch_start_pins_pre_burn_committee() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Epoch Start Pin Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // block 1: plain epoch-0 block
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let header1 = block1.recovered_block.clone_sealed_header();

        // block 2: close epoch 0 — `concludeEpoch` executes inside this block, making it epoch
        // 0's closing block (the header that rules all of epoch 1)
        let consensus_output = consensus_output_for_tests(2, 1, 2, true);
        let payload = TNPayload::new_for_test(header1, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let closing_header = block2.recovered_block.clone_sealed_header();

        // snapshot A: epoch 1's start state, pinned to epoch 0's closing block
        let (snapshot_a, pin_a) = reth_env.epoch_state_at_epoch_start()?;
        assert_eq!(snapshot_a.epoch, 1);
        assert_eq!(snapshot_a.validators.len(), 5, "full pre-burn committee");
        assert_eq!(snapshot_a.bls_pubkeys.len(), 5);
        assert_eq!(pin_a.hash(), closing_header.hash(), "pin is epoch 0's closing block");

        // block 3: governance burns a committee member mid-epoch-1
        let target = snapshot_a.validators[1].validatorAddress;
        let target_bls = snapshot_a.bls_pubkeys[1].clone();
        let mut governance = governance_owner_factory();
        let burn_tx = governance_burn_tx(&mut governance, chain.clone(), target);
        let consensus_output = consensus_output_for_tests(2, 1, 3, false);
        let payload = TNPayload::new_for_test(closing_header, &consensus_output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![burn_tx])?;

        // the tip view shrinks immediately (swap-and-pop)
        let tip_state = reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(tip_state.epoch, 1);
        assert_eq!(tip_state.validators.len(), 4, "tip committee shrank post-burn");
        assert_eq!(tip_state.bls_pubkeys.len(), 4);
        assert!(tip_state.validators.iter().all(|v| v.validatorAddress != target));
        assert!(!tip_state.bls_pubkeys.contains(&target_bls));

        // the epoch-start view still returns the full pre-burn set, byte-identical to snapshot A
        let (snapshot_b, pin_b) = reth_env.epoch_state_at_epoch_start()?;
        assert_eq!(pin_b.hash(), pin_a.hash(), "pin unchanged by the burn");
        assert_eq!(snapshot_b.epoch, snapshot_a.epoch);
        assert_eq!(snapshot_b.epoch_info, snapshot_a.epoch_info);
        assert_eq!(snapshot_b.epoch_start, snapshot_a.epoch_start);
        assert_eq!(snapshot_b.validators, snapshot_a.validators);
        assert_eq!(snapshot_b.bls_pubkeys, snapshot_a.bls_pubkeys);
        assert!(snapshot_b.validators.iter().any(|v| v.validatorAddress == target));
        assert!(snapshot_b.bls_pubkeys.contains(&target_bls));

        // the boundary-written-once EpochInfo scalars agree between the tip and pinned reads;
        // the committee array embedded in EpochInfo is exactly what the burn mutates, so it is
        // the only field that diverges
        assert_eq!(tip_state.epoch, snapshot_b.epoch);
        assert_eq!(tip_state.epoch_info.blockHeight, snapshot_b.epoch_info.blockHeight);
        assert_eq!(tip_state.epoch_info.epochId, snapshot_b.epoch_info.epochId);
        assert_eq!(tip_state.epoch_info.epochDuration, snapshot_b.epoch_info.epochDuration);
        assert_eq!(tip_state.epoch_info.epochIssuance, snapshot_b.epoch_info.epochIssuance);
        assert_eq!(tip_state.epoch_info.stakeVersion, snapshot_b.epoch_info.stakeVersion);
        assert_eq!(tip_state.epoch_start, snapshot_b.epoch_start);
        assert_eq!(tip_state.epoch_info.committee.len(), 4, "burn mutates EpochInfo's committee");
        assert_eq!(snapshot_b.epoch_info.committee.len(), 5);

        Ok(())
    }

    /// Next-committee prefetches pinned to the epoch-start header read the PRE-burn future
    /// committees. At epoch 0's closing block the registry already reports epoch 1 and serves
    /// `getCommitteeValidators`/`getCommitteeBlsPubkeys` for epochs 2 and 3 (genesis seeds
    /// epochs 0-2; `concludeEpoch` writes epoch 3's committee inside the closing block), so a
    /// node re-entering epoch 1 after a mid-epoch burn derives the same future sets it would
    /// have derived entering before the burn. The tip variants of the same reads observe the
    /// post-burn (shrunken) sets, proving the pin is what makes the difference.
    #[tokio::test]
    async fn pinned_next_committee_prefetch_reads_pre_burn_sets() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Pinned Prefetch Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // block 1: plain epoch-0 block
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let header1 = block1.recovered_block.clone_sealed_header();

        // block 2: close epoch 0 (epoch 0's closing block)
        let consensus_output = consensus_output_for_tests(2, 1, 2, true);
        let payload = TNPayload::new_for_test(header1, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let closing_header = block2.recovered_block.clone_sealed_header();

        // pre-burn snapshots of the epoch 2 and 3 committees (the tip IS the closing block here)
        let pre_v2 = reth_env.validators_for_epoch(2)?;
        let pre_v3 = reth_env.validators_for_epoch(3)?;
        let pre_b2 = reth_env.bls_pubkeys_for_epoch(2)?;
        let pre_b3 = reth_env.bls_pubkeys_for_epoch(3)?;
        assert_eq!(pre_v2.len(), 5);
        assert_eq!(pre_v3.len(), 5);

        // block 3: governance burns a committee member mid-epoch-1 (with exactly 5 eligible
        // validators every committee seats all 5, so the target sits in epochs 1, 2, and 3)
        let EpochState { validators, bls_pubkeys, .. } =
            reth_env.epoch_state_from_canonical_tip()?;
        let target = validators[1].validatorAddress;
        let target_bls = bls_pubkeys[1].clone();
        let mut governance = governance_owner_factory();
        let burn_tx = governance_burn_tx(&mut governance, chain.clone(), target);
        let consensus_output = consensus_output_for_tests(2, 1, 3, false);
        let payload = TNPayload::new_for_test(closing_header.clone(), &consensus_output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![burn_tx])?;

        // pinned prefetches at the epoch-start header: epoch+1 and epoch+2 relative to the
        // running epoch 1 still return the PRE-burn sets
        let (state, pin) = reth_env.epoch_state_at_epoch_start()?;
        assert_eq!(state.epoch, 1);
        assert_eq!(pin.hash(), closing_header.hash());
        let pinned_v2 = reth_env.validators_for_epoch_at_block(2, pin.hash())?;
        let pinned_v3 = reth_env.validators_for_epoch_at_block(3, pin.hash())?;
        assert_eq!(pinned_v2, pre_v2, "epoch 2 committee at the pin is the pre-burn set");
        assert_eq!(pinned_v3, pre_v3, "epoch 3 committee at the pin is the pre-burn set");
        assert!(pinned_v2.iter().any(|v| v.validatorAddress == target));
        assert!(pinned_v3.iter().any(|v| v.validatorAddress == target));
        let pinned_b2 = reth_env.bls_pubkeys_for_epoch_at_block(2, pin.hash())?;
        let pinned_b3 = reth_env.bls_pubkeys_for_epoch_at_block(3, pin.hash())?;
        assert_eq!(pinned_b2, pre_b2, "epoch 2 pubkeys at the pin are the pre-burn set");
        assert_eq!(pinned_b3, pre_b3, "epoch 3 pubkeys at the pin are the pre-burn set");
        assert!(pinned_b2.contains(&target_bls));
        assert!(pinned_b3.contains(&target_bls));

        // the tip variant of the same read shows the post-burn set: the pin is what makes the
        // difference
        let tip_v2 = reth_env.validators_for_epoch(2)?;
        assert_eq!(tip_v2.len(), 4);
        assert!(tip_v2.iter().all(|v| v.validatorAddress != target));

        Ok(())
    }

    /// In epoch 0 the pin is genesis: genesis execution seeds the registry, so genesis state IS
    /// epoch 0's start state. The pinned view matches the canonical-tip view field for field at
    /// genesis and keeps pinning genesis after the chain advances within the epoch.
    #[tokio::test]
    async fn epoch_state_at_epoch_start_epoch_zero_pins_genesis() -> eyre::Result<()> {
        let genesis = test_genesis_with_consensus_registry(5);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Epoch Zero Pin Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // fresh chain still in epoch 0: the pin is the genesis header
        let (state, pin) = reth_env.epoch_state_at_epoch_start()?;
        assert_eq!(pin.number, 0, "epoch 0 pins genesis");
        assert_eq!(pin.hash(), chain.sealed_genesis_header().hash());

        // at genesis the tip and the pin are the same block, so the views agree field for field
        let tip_state = reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(state.epoch, 0);
        assert_eq!(state.epoch, tip_state.epoch);
        assert_eq!(state.epoch_info, tip_state.epoch_info);
        assert_eq!(state.epoch_start, tip_state.epoch_start);
        assert_eq!(state.validators, tip_state.validators);
        assert_eq!(state.bls_pubkeys, tip_state.bls_pubkeys);
        assert_eq!(state.validators.len(), 5);

        // the pin stays genesis after the chain advances within epoch 0
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let (state_after, pin_after) = reth_env.epoch_state_at_epoch_start()?;
        assert_eq!(pin_after.number, 0);
        assert_eq!(state_after.epoch, 0);
        assert_eq!(state_after.validators, state.validators);
        assert_eq!(state_after.bls_pubkeys, state.bls_pubkeys);

        Ok(())
    }

    #[test]
    fn test_parallel_recovery_preserves_order() {
        use rayon::iter::{IntoParallelRefIterator as _, ParallelIterator as _};
        use tn_types::Encodable2718;

        // Create 20 transactions from different random signers so each tx is unique.
        let chain: Arc<RethChainSpec> = Arc::new(tn_types::test_genesis().into());
        let num_txs = 20;
        let mut encoded_txs = Vec::with_capacity(num_txs);
        for i in 0..num_txs {
            let mut factory =
                TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(i as u64));
            let tx = factory.create_eip1559(
                chain.clone(),
                None,
                100_000,
                Some(Address::ZERO),
                U256::from(1),
                Default::default(),
            );
            encoded_txs.push(tx.encoded_2718());
        }

        // Recover sequentially
        let sequential: Vec<_> = encoded_txs
            .iter()
            .map(|tx_bytes| {
                reth_recover_raw_transaction::<TransactionSigned>(tx_bytes)
                    .expect("sequential recovery")
            })
            .collect();

        // Recover in parallel (using rayon, same as production code)
        let parallel: Vec<_> = encoded_txs
            .par_iter()
            .map(|tx_bytes| {
                reth_recover_raw_transaction::<TransactionSigned>(tx_bytes)
                    .expect("parallel recovery")
            })
            .collect();

        // Assert same length
        assert_eq!(sequential.len(), parallel.len());

        // Assert same order by comparing tx hashes and recovered signer addresses
        for (seq, par) in sequential.iter().zip(parallel.iter()) {
            assert_eq!(seq.hash(), par.hash(), "transaction hashes must match in order");
            assert_eq!(seq.signer(), par.signer(), "recovered signers must match in order");
        }
    }

    #[tokio::test]
    async fn test_get_worker_fee_configs() -> eyre::Result<()> {
        // minimal validator set (5 validators)
        let all_validators: Vec<Address> =
            (1..=5).map(|i| Address::from_slice(&[i * 0x11; 20])).collect();

        let validators: Vec<_> = all_validators
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let mut rng = StdRng::seed_from_u64(i as u64);
                let bls = BlsKeypair::generate(&mut rng);
                let bls_pubkey = bls.public();
                let pop = generate_proof_of_possession_bls_for_test(&bls, addr)
                    .expect("pop generation failed");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls_pubkey,
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: pop,
                }
            })
            .collect();

        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
            minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
            epochIssuance: U256::from(parse_ether("20_000_000").unwrap())
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: 28800,
        };

        let governance_multisig =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        let governance = governance_multisig.address();
        let tmp_genesis = tn_types::test_genesis().extend_accounts([(
            governance,
            GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
        )]);

        // deploy with 2 workers: worker 0 = EIP-1559 (strategy 0, target 30M),
        // worker 1 = Static (strategy 1, fee 500)
        let worker_configs = vec![(0u8, 30_000_000u64), (1u8, 500u64)];
        let genesis = RethEnv::create_consensus_registry_genesis_accounts(
            validators.clone(),
            tmp_genesis,
            initial_stake_config.clone(),
            governance,
            worker_configs,
        )?;

        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Worker Fee Config Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();

        // read back worker configs from chain state - the returned length IS the on-chain
        // numWorkers()
        let configs = reth_env.get_worker_fee_configs()?;
        assert_eq!(configs.len(), 2);
        assert_eq!(configs[0], WorkerFeeConfig::Eip1559 { target_gas: 30_000_000 });
        assert_eq!(configs[1], WorkerFeeConfig::Static { fee: 500 });

        // the block-pinned read primitive reports the same count at genesis
        let (num_workers, configs_at_block) =
            reth_env.get_worker_fee_configs_at_block(chain.sealed_genesis_header().hash())?;
        assert_eq!(num_workers, 2);
        assert_eq!(configs_at_block, configs);

        Ok(())
    }

    /// Pins the per-epoch worker-count read rule: the count for epoch E is the `WorkerConfigs`
    /// state at E's first block's parent (= E-1's closing block). Governance submits
    /// `setWorkerConfig` + `setNumWorkers` during epoch 0; the count read at epoch 1's
    /// start-parent block reflects the change, while the count read at epoch 0's start-parent
    /// (genesis) still reports the original single worker.
    #[tokio::test]
    async fn test_worker_count_read_at_epoch_start_parent() -> eyre::Result<()> {
        // minimal validator set (5 validators)
        let all_validators: Vec<Address> =
            (1..=5).map(|i| Address::from_slice(&[i * 0x11; 20])).collect();

        let validators: Vec<_> = all_validators
            .iter()
            .enumerate()
            .map(|(i, addr)| {
                let mut rng = StdRng::seed_from_u64(i as u64);
                let bls = BlsKeypair::generate(&mut rng);
                let bls_pubkey = bls.public();
                let pop = generate_proof_of_possession_bls_for_test(&bls, addr)
                    .expect("pop generation failed");
                NodeInfo {
                    name: format!("validator-{i}"),
                    bls_public_key: *bls_pubkey,
                    p2p_info: NodeP2pInfo::default(),
                    execution_address: *addr,
                    proof_of_possession: pop,
                }
            })
            .collect();

        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").unwrap()),
            minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
            epochIssuance: U256::from(parse_ether("20_000_000").unwrap())
                .checked_div(U256::from(28))
                .expect("u256 div checked"),
            epochDuration: 28800,
        };

        // governance owns the WorkerConfigs contract and signs the config txs
        let mut governance_multisig =
            TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
        let governance = governance_multisig.address();
        let tmp_genesis = tn_types::test_genesis().extend_accounts([(
            governance,
            GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)),
        )]);

        // deploy with a single worker (the canonical existing-chain shape)
        let genesis = RethEnv::create_consensus_registry_genesis_accounts(
            validators.clone(),
            tmp_genesis,
            initial_stake_config.clone(),
            governance,
            vec![(0u8, 30_000_000u64)],
        )?;

        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("Worker Count Epoch Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)
                .unwrap();

        // sanity: epoch 0 starts at blockHeight 0, so its start-parent (saturating) is genesis
        let EpochState { epoch, epoch_info, .. } = reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 0);
        let epoch_zero_read_block = epoch_info.blockHeight.saturating_sub(1);
        assert_eq!(epoch_zero_read_block, 0);

        // governance grows the worker set mid-epoch: config worker 1 (Static 500), then grow
        // the count (setWorkerConfig must precede setNumWorkers per the contract)
        let set_config_tx = governance_multisig.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(WORKER_CONFIGS_ADDRESS),
            U256::ZERO,
            WorkerConfigs::setWorkerConfigCall { workerId: 1, strategy: 1, value: 500, data: 0 }
                .abi_encode()
                .into(),
        );
        let set_num_workers_tx = governance_multisig.create_eip1559_encoded(
            chain.clone(),
            None,
            100,
            Some(WORKER_CONFIGS_ADDRESS),
            U256::ZERO,
            WorkerConfigs::setNumWorkersCall { numWorkers_: 2 }.abi_encode().into(),
        );

        // block 1 (mid-epoch-0): the governance txs land
        let consensus_output = consensus_output_for_tests(2, 0, 1, false);
        let payload = TNPayload::new_for_test(chain.sealed_genesis_header(), &consensus_output);
        let block1 = execute_payload_and_update_canonical_chain(
            &reth_env,
            payload,
            vec![set_config_tx, set_num_workers_tx],
        )?;
        let canonical_header = block1.recovered_block.clone_sealed_header();

        // block 2 closes epoch 0 -> epoch 1's first block will be 3, start-parent = block 2
        let consensus_output = consensus_output_for_tests(2, 1, 2, true);
        let payload = TNPayload::new_for_test(canonical_header, &consensus_output);
        let block2 = execute_payload_and_update_canonical_chain(&reth_env, payload, vec![])?;
        let close_block_hash = block2.recovered_block.clone_sealed_header().hash();

        // the new epoch's info points its start-parent at the closing block
        let EpochState { epoch, epoch_info, .. } = reth_env.epoch_state_from_canonical_tip()?;
        assert_eq!(epoch, 1);
        let epoch_one_read_block = epoch_info.blockHeight.saturating_sub(1);
        let epoch_one_read_header = reth_env
            .sealed_header_by_number(epoch_one_read_block)?
            .expect("epoch 1 start-parent header");
        assert_eq!(epoch_one_read_header.hash(), close_block_hash);

        // epoch 1's count (read at its start-parent) reflects the governance change...
        let (num_workers, configs) =
            reth_env.get_worker_fee_configs_at_block(epoch_one_read_header.hash())?;
        assert_eq!(num_workers, 2);
        assert_eq!(configs[1], WorkerFeeConfig::Static { fee: 500 });

        // ...while epoch 0's count (read at genesis) still reports the original single worker
        let genesis_hash = reth_env
            .sealed_header_by_number(epoch_zero_read_block)?
            .expect("genesis header")
            .hash();
        let (num_workers, _configs) = reth_env.get_worker_fee_configs_at_block(genesis_hash)?;
        assert_eq!(num_workers, 1);

        Ok(())
    }

    /// Classification pin: the pinned-block read paths distinguish node-local provider faults
    /// (the pinned hash/header not resolving on THIS node - peers may read fine) from
    /// chain-global failures (contract absent at the block - identical on every node). The
    /// close-time base-fee compute keys retry-then-halt vs keep-current off this split.
    #[tokio::test]
    async fn test_state_read_classifies_provider_vs_chain_global() -> eyre::Result<()> {
        // healthy provider/database, but with the system contracts stripped from the alloc so
        // the contract reads fail deterministically at every block
        let mut genesis = tn_types::test_genesis();
        genesis.alloc.remove(&WORKER_CONFIGS_ADDRESS);
        genesis.alloc.remove(&CONSENSUS_REGISTRY_ADDRESS);
        let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
        let tmp_dir = TempDir::new().unwrap();
        let task_manager = TaskManager::new("State Read Classification Test");
        let reth_env =
            RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

        // PROVIDER: an unknown/random block hash on a healthy env is a node-local resolution
        // failure (the fault never reaches the contract)
        let err = reth_env
            .get_worker_fee_configs_at_block(B256::random())
            .expect_err("unknown block hash must fail");
        assert!(
            matches!(err, StateReadError::Provider(_)),
            "unknown block hash must classify as Provider, got: {err}"
        );

        // CHAIN-GLOBAL: the hash resolves (genesis) but the WorkerConfigs contract is absent -
        // the call succeeds with empty return data and the decode fails deterministically
        let err = reth_env
            .get_worker_fee_configs_at_block(chain.sealed_genesis_header().hash())
            .expect_err("absent contract must fail");
        assert!(
            matches!(err, StateReadError::ChainGlobal(_)),
            "absent contract must classify as ChainGlobal, got: {err}"
        );

        // the close-time identity read classifies the same way: a header this node cannot
        // resolve state for is Provider...
        let phantom = SealedHeader::new(ExecHeader::default(), B256::random());
        let err = reth_env
            .get_current_epoch_info_at_header(&phantom)
            .expect_err("phantom header must fail");
        assert!(
            matches!(err, StateReadError::Provider(_)),
            "unresolvable header state must classify as Provider, got: {err}"
        );

        // ...while an absent ConsensusRegistry at a resolvable block is ChainGlobal
        let err = reth_env
            .get_current_epoch_info_at_header(&chain.sealed_genesis_header())
            .expect_err("absent registry must fail");
        assert!(
            matches!(err, StateReadError::ChainGlobal(_)),
            "absent registry must classify as ChainGlobal, got: {err}"
        );

        Ok(())
    }

    /// Regression: a `WorkerConfigs` constructor revert must FAIL genesis creation.
    ///
    /// Strategy 2 exceeds the contract's `MAX_STRATEGY` (= 1), so the constructor reverts
    /// `InvalidStrategy`. Before the fix the reverted state was committed anyway, shipping
    /// runtime code with empty storage: `numWorkers() = 0` and `owner() = address(0)` —
    /// permanently unownable and masked downstream by fail-open defaults.
    #[tokio::test]
    async fn genesis_ceremony_rejects_invalid_worker_config_strategy() {
        let err = crate::test_utils::try_test_genesis_with_consensus_registry_and_workers(
            4,
            vec![(2u8, 30_000_000u64)],
        )
        .expect_err("strategy 2 exceeds the contract's MAX_STRATEGY and must fail genesis");
        assert!(
            format!("{err:#}").contains("WorkerConfigs constructor reverted"),
            "error must name the WorkerConfigs revert, got: {err:#}"
        );
    }

    /// Regression: an EMPTY worker config set must FAIL genesis creation (the
    /// `WorkerConfigs` constructor reverts `NumWorkersBelowMinimum`). See
    /// [`genesis_ceremony_rejects_invalid_worker_config_strategy`] for the pre-fix failure mode.
    #[tokio::test]
    async fn genesis_ceremony_rejects_empty_worker_configs() {
        let err =
            crate::test_utils::try_test_genesis_with_consensus_registry_and_workers(4, vec![])
                .expect_err("empty worker configs must fail genesis");
        assert!(
            format!("{err:#}").contains("WorkerConfigs constructor reverted"),
            "error must name the WorkerConfigs revert, got: {err:#}"
        );
    }

    /// Regression: the `ConsensusRegistry` pre-genesis create is guarded by
    /// the same success check. A proof of possession generated for the WRONG address fails the
    /// constructor's BLS precompile verification (`InvalidProofOfPossession`), which must fail
    /// genesis creation instead of committing a half-initialized registry.
    #[tokio::test]
    async fn genesis_ceremony_rejects_invalid_consensus_registry_pop() {
        let validator_address = Address::from_slice(&[0x11; 20]);
        let wrong_address = Address::from_slice(&[0x22; 20]);
        let mut rng = StdRng::seed_from_u64(0);
        let bls = BlsKeypair::generate(&mut rng);
        // sign the proof of possession over the wrong address
        let pop = generate_proof_of_possession_bls_for_test(&bls, &wrong_address)
            .expect("pop generation failed");
        let validator = NodeInfo {
            name: "validator-0".to_string(),
            bls_public_key: *bls.public(),
            p2p_info: NodeP2pInfo::default(),
            execution_address: validator_address,
            proof_of_possession: pop,
        };

        let initial_stake_config = ConsensusRegistry::StakeConfig {
            stakeAmount: U256::from(parse_ether("1_000_000").expect("parse stake amount")),
            minWithdrawAmount: U256::from(parse_ether("1_000").expect("parse min withdraw")),
            epochIssuance: U256::from(parse_ether("25_806").expect("parse epoch issuance")),
            epochDuration: 60 * 60 * 8,
        };

        let err = RethEnv::create_consensus_registry_genesis_accounts(
            vec![validator],
            tn_types::test_genesis(),
            initial_stake_config,
            Address::from_slice(&[0x99; 20]),
            vec![(0u8, 30_000_000u64)],
        )
        .expect_err("invalid proof of possession must fail genesis");
        assert!(
            format!("{err:#}").contains("ConsensusRegistry constructor reverted"),
            "error must name the ConsensusRegistry revert, got: {err:#}"
        );
    }
}
