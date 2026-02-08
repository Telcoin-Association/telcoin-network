//! E2E test: CLI key generation → staking on ConsensusRegistry.
//!
//! Verifies the complete flow:
//! 1. Generate BLS keys and proof of possession via CLI keytool
//! 2. Use export-staking-args --calldata to build the stake transaction
//! 3. Execute mint → stake → activate on a local ConsensusRegistry
//! 4. Query the registry directly to verify the validator is correctly staked

use alloy::{primitives::utils::parse_ether, sol_types::SolCall};
use clap::Parser as _;
use e2e_tests::create_validator_info;
use rand::{rngs::StdRng, SeedableRng};
use std::sync::Arc;
use telcoin_network_cli::keytool::{KeyArgs, KeySubcommand};
use tn_config::{Config, ConfigFmt, ConfigTrait as _, NodeInfo};
use tn_reth::{
    payload::TNPayload,
    system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    NewCanonicalChain, RethChainSpec, RethEnv,
};
use tn_types::{
    generate_proof_of_possession_bls, test_utils::CommandParser, Address, BlsKeypair,
    GenesisAccount, NodeP2pInfo, SealedHeader, TaskManager, B256, MIN_PROTOCOL_BASE_FEE, U256,
};

/// Build a test [TNPayload] directly, avoiding the `#[cfg(test)]`-only `new_for_test`.
fn make_payload(
    parent_header: &SealedHeader,
    epoch: u32,
    round: u32,
    close_epoch: bool,
) -> TNPayload {
    TNPayload {
        parent_header: parent_header.clone(),
        beneficiary: Address::random(),
        nonce: ((epoch as u64) << 32) | (round as u64),
        batch_index: 0,
        timestamp: tn_types::now(),
        batch_digest: B256::random(),
        consensus_header_digest: B256::random(),
        base_fee_per_gas: parent_header.base_fee_per_gas.unwrap_or(MIN_PROTOCOL_BASE_FEE),
        gas_limit: parent_header.gas_limit,
        mix_hash: B256::random(),
        close_epoch: if close_epoch { Some(B256::random()) } else { None },
        worker_id: 0,
    }
}

/// Execute a payload with transactions and commit the block to the canonical chain.
fn execute_and_commit(
    reth_env: &RethEnv,
    payload: TNPayload,
    transactions: Vec<Vec<u8>>,
) -> eyre::Result<SealedHeader> {
    let block = reth_env.build_block_from_batch_payload(payload, &transactions)?;
    let header = block.recovered_block.clone_sealed_header();
    let state = reth_env.canonical_in_memory_state();
    state.update_chain(NewCanonicalChain::Commit { new: vec![block.clone()] });
    state.set_canonical_head(header.clone());
    reth_env.finish_executing_output(vec![block])?;
    reth_env.finalize_block(header.clone())?;
    Ok(header)
}

/// E2E: generate keys via CLI keytool → stake on local ConsensusRegistry → verify committee.
#[tokio::test]
async fn test_cli_keygen_to_stake() -> eyre::Result<()> {
    // ── 1. Create initial validators with deterministic BLS keys ──
    let initial_addresses = [
        Address::from_slice(&[0x11; 20]),
        Address::from_slice(&[0x22; 20]),
        Address::from_slice(&[0x33; 20]),
        Address::from_slice(&[0x44; 20]),
        Address::from_slice(&[0x55; 20]),
    ];

    let initial_validators: Vec<NodeInfo> = initial_addresses
        .iter()
        .enumerate()
        .map(|(i, addr)| {
            let mut rng = StdRng::seed_from_u64(i as u64);
            let bls = BlsKeypair::generate(&mut rng);
            let pop = generate_proof_of_possession_bls(&bls, addr).expect("pop generation failed");
            NodeInfo {
                name: format!("validator-{i}"),
                bls_public_key: *bls.public(),
                p2p_info: NodeP2pInfo::default(),
                execution_address: *addr,
                proof_of_possession: pop,
            }
        })
        .collect();

    // ── 2. Create governance and new-validator EOAs ──
    let mut governance_eoa =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let governance = governance_eoa.address();

    let mut new_validator_eoa =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(42));
    let new_validator_addr = new_validator_eoa.address();

    // ── 3. Generate keys for the new validator via CLI keytool ──
    let key_dir = tempfile::TempDir::new()?;
    create_validator_info(key_dir.path(), &format!("{new_validator_addr:?}"), None)?;

    // Load the CLI-generated node-info.yaml
    let node_info = Config::load_from_path::<NodeInfo>(
        &key_dir.path().join("node-info.yaml"),
        ConfigFmt::YAML,
    )?;

    // Sanity: verify key byte lengths match expected BLS sizes
    assert_eq!(node_info.bls_public_key.to_bytes().len(), 96, "compressed BLS pubkey");
    assert_eq!(node_info.bls_public_key.serialize().len(), 192, "uncompressed BLS pubkey");
    assert_eq!(node_info.proof_of_possession.serialize().len(), 96, "uncompressed PoP sig");

    // ── 4. Build genesis with ConsensusRegistry ──
    let stake_amount = U256::from(parse_ether("1_000_000").unwrap());
    let initial_stake_config = ConsensusRegistry::StakeConfig {
        stakeAmount: stake_amount,
        minWithdrawAmount: U256::from(parse_ether("1_000").unwrap()),
        epochIssuance: U256::from(parse_ether("20_000_000").unwrap())
            .checked_div(U256::from(28))
            .expect("u256 div"),
        epochDuration: 86400,
    };

    let genesis = tn_types::test_genesis().extend_accounts([
        (
            governance,
            GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000").unwrap())),
        ),
        (
            new_validator_addr,
            GenesisAccount::default().with_balance(stake_amount.saturating_mul(U256::from(2))),
        ),
    ]);

    let genesis = RethEnv::create_consensus_registry_genesis_accounts(
        initial_validators.clone(),
        genesis,
        initial_stake_config.clone(),
        governance,
    )?;

    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let tmp_dir = tempfile::TempDir::new()?;
    let task_manager = TaskManager::new("E2E Key Test");
    let reth_env = RethEnv::new_for_temp_chain(chain.clone(), tmp_dir.path(), &task_manager, None)?;

    // Sanity: verify initial epoch state
    let epoch_state = reth_env.epoch_state_from_canonical_tip()?;
    assert_eq!(epoch_state.epoch, 0);
    assert_eq!(epoch_state.validators.len(), 5);

    // ── 5. Build staking transactions using CLI-generated keys ──

    // 5a. Governance mints NFT for new validator
    let mint_tx = governance_eoa.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        ConsensusRegistry::mintCall { validatorAddress: new_validator_addr }.abi_encode().into(),
    );

    // 5b. New validator stakes using CLI-generated calldata from export-staking-args --calldata
    let cmd = CommandParser::<KeyArgs>::parse_from([
        "tn",
        "export-staking-args",
        "--node-info",
        key_dir.path().to_str().expect("valid utf-8 path"),
        "--calldata",
    ]);
    let stake_calldata = match &cmd.args.command {
        KeySubcommand::ExportStakingArgs(args) => args.stake_calldata()?,
        _ => unreachable!(),
    };
    let stake_tx = new_validator_eoa.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        stake_amount,
        stake_calldata.into(),
    );

    // 5c. New validator activates for committee selection
    let activate_tx = new_validator_eoa.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        ConsensusRegistry::activateCall {}.abi_encode().into(),
    );

    // ── 6. Execute staking transactions ──

    let genesis_header = chain.sealed_genesis_header();

    let payload = make_payload(&genesis_header, 0, 2, false);
    let h1 = execute_and_commit(&reth_env, payload, vec![mint_tx, stake_tx, activate_tx])?;

    // ── 7. Query ConsensusRegistry directly to verify the validator is staked ──

    let on_chain = reth_env.get_validator_info(h1.hash(), new_validator_addr)?;

    assert_eq!(
        on_chain.validatorAddress, new_validator_addr,
        "on-chain validator address should match"
    );
    assert_eq!(
        on_chain.currentStatus,
        ConsensusRegistry::ValidatorStatus::PendingActivation,
        "validator should be PendingActivation after stake + activate"
    );
    assert!(!on_chain.isRetired, "validator should not be retired");
    assert_eq!(
        on_chain.blsPubkey.as_ref(),
        node_info.bls_public_key.to_bytes(),
        "on-chain BLS pubkey should match CLI-generated key"
    );

    Ok(())
}
