//! Test the epoch boundary and validator shuffles.

use crate::util::create_validator_info;
use alloy::{
    primitives::utils::parse_ether,
    providers::{Provider, ProviderBuilder},
    sol_types::SolCall,
};
use clap::Parser as _;
use rand::{rngs::StdRng, SeedableRng as _};
use std::{
    panic,
    path::{Path, PathBuf},
    process::{Child, Command},
    sync::Arc,
};
use telcoin_network::genesis::GenesisArgs;
use tn_config::{Config, ConfigFmt, ConfigTrait as _, NodeInfo};
use tn_reth::{
    system_calls::{ConsensusRegistry, CONSENSUS_REGISTRY_ADDRESS},
    test_utils::TransactionFactory,
    RethChainSpec,
};
use tn_types::{
    test_utils::CommandParser, Address, EpochCertificate, EpochRecord, Genesis, GenesisAccount,
    U256,
};
use tokio::time::timeout;
use tracing::{debug, info};

const NEW_VALIDATOR: &str = "new-validator";
const NODE_PASSWORD: &str = "sup3rsecuur";
const INITIAL_STAKE_AMOUNT: &str = "1_000_000";
const MIN_EPOCHS_TO_TEST: usize = 6;
// 3s is too aggressive
const EPOCH_DURATION: u64 = 5;

async fn test_epoch_boundary_inner(
    genesis: Genesis,
    mut governance_wallet: TransactionFactory,
    temp_path: &Path,
    new_validator: &mut TransactionFactory,
) -> eyre::Result<()> {
    // create transactions to make new validator eligible for future epochs
    let chain: Arc<RethChainSpec> = Arc::new(genesis.into());
    let txs = generate_new_validator_txs(temp_path, chain, new_validator, &mut governance_wallet)?;

    // create rpc client for node1 default rpc address
    let rpc_url = "http://127.0.0.1:8545".to_string();
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);

    // wait for node rpc to become available
    timeout(std::time::Duration::from_secs(20), async {
        let mut result = provider.get_chain_id().await;
        while let Err(e) = result {
            debug!(target: "epoch-test", "provider error getting chain id: {e:?}");
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            // make next request
            result = provider.get_chain_id().await;
        }
    })
    .await?;

    // submit txs to: issue NFT, stake, and activate new validator
    for tx in txs {
        let pending = &provider.send_raw_transaction(&tx).await?;
        debug!(target: "epoch-test", "pending tx: {pending:?}");
    }

    // retrieve current committee
    let consensus_registry = ConsensusRegistry::new(CONSENSUS_REGISTRY_ADDRESS, &provider);
    let mut current_epoch_info = consensus_registry.getCurrentEpochInfo().call().await?;

    let mut last_epoch_block_height = current_epoch_info.blockHeight;

    // track the number of times the new validator was in the epoch committee
    let mut new_validator_in_committee_count = 0;

    // sleep for first epoch with 1s offset and begin assertions loop
    tokio::time::sleep(std::time::Duration::from_secs(EPOCH_DURATION + 1)).await;

    let mut last_pause = 100;
    let mut shuffled = false;
    // the new validator has a 1/6 chance of being selected for the new committee
    //
    // if the new validator hasn't been shuffled in by the minimum number of epochs to test,
    // continue looping up to 99% probability that new validator is shuffled into committee
    //
    // probability (if purely random):
    // 1 - (5/6)^n >= 0.99
    // n ~= 25 iterations
    for i in 0..25 {
        let new_epoch_info = consensus_registry.getCurrentEpochInfo().call().await?;
        if new_epoch_info == current_epoch_info && last_pause != i {
            tokio::time::sleep(std::time::Duration::from_secs(EPOCH_DURATION / 2)).await;
            last_pause = i + 1;
            continue;
        }
        last_pause = i;
        assert!(new_epoch_info != current_epoch_info, "Old and new epoch equal on iteration {i}");
        assert!(new_epoch_info.blockHeight > last_epoch_block_height);
        assert_eq!(new_epoch_info.epochDuration as u64, EPOCH_DURATION);

        // count the number of times the new validator is in committee
        if new_epoch_info.committee.contains(&new_validator.address()) {
            new_validator_in_committee_count += 1;
        }

        // if min number of epochs have transitioned, assert new validator has been shuffled in
        // at least once to end the test
        if i > MIN_EPOCHS_TO_TEST && new_validator_in_committee_count > 0 {
            shuffled = true;
            break;
        }

        // store the last seen epoch info that is expected to change every epoch
        last_epoch_block_height = new_epoch_info.blockHeight;
        current_epoch_info = new_epoch_info;

        // sleep for epoch duration
        tokio::time::sleep(std::time::Duration::from_secs(EPOCH_DURATION)).await;
    }

    if shuffled {
        // Do a check to make sure all the nodes have valid (certified) Epoch Records.
        // TODO issue 375, should use tn_latestHeader RPC for this when fixed.
        let latest_epoch = last_pause;
        for p in 8540..=8545 {
            let rpc_url = format!("http://127.0.0.1:{p}");
            let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
            for epoch in 0..=latest_epoch {
                let (epoch_rec, cert): (EpochRecord, EpochCertificate) =
                    provider.raw_request("tn_epochRecord".into(), (epoch,)).await?;
                assert!(epoch_rec.verify_with_cert(&cert), "invalid epoch record!");
            }
        }

        Ok(())
    } else {
        // return error if loop didn't return
        Err(eyre::eyre!("new validator not shuffled into committee!"))
    }
}

fn kill_procs(procs: &Vec<Arc<std::sync::Mutex<Child>>>) {
    // We need to capture the result above and then kill all the procs.
    for proc in procs.iter() {
        let _ = proc.lock().unwrap().kill();
    }
    for proc in procs {
        let _ = proc.lock().unwrap().wait();
    }
}

#[ignore = "only run independently from all other it tests"]
#[tokio::test]
/// Test a new node joining the network and being shuffled into the committee.
async fn test_epoch_boundary() -> eyre::Result<()> {
    tn_types::test_utils::init_test_tracing();
    // create validator and governance wallets for adding new validator later
    let mut new_validator = TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(6));
    let mut committee = vec![
        ("validator-1", Address::from_slice(&[0x11; 20])),
        ("validator-2", Address::from_slice(&[0x22; 20])),
        ("validator-3", Address::from_slice(&[0x33; 20])),
        ("validator-4", Address::from_slice(&[0x44; 20])),
        ("validator-5", Address::from_slice(&[0x55; 20])),
    ];

    // setup genesis
    let temp_dir = tempfile::TempDir::with_prefix("epoch_boundary")?;
    let temp_path = temp_dir.path();

    let governance_wallet =
        TransactionFactory::new_random_from_seed(&mut StdRng::seed_from_u64(33));
    let genesis = create_genesis_for_test(
        temp_path,
        new_validator.address(),
        governance_wallet.address(),
        &committee,
    )?;

    // start nodes (committee + new validator)
    committee.push((NEW_VALIDATOR, new_validator.address()));
    let procs = start_nodes(temp_path, &committee)?;
    let procs: Vec<Arc<std::sync::Mutex<Child>>> =
        procs.into_iter().map(|c| Arc::new(std::sync::Mutex::new(c))).collect();
    let procs_clone = procs.clone();
    // Use a panic hook to make sure we kill the node procs on a panic (assert failure).
    let org_panic = panic::take_hook();
    panic::set_hook(Box::new(move |a| {
        kill_procs(&procs_clone);
        org_panic(a);
    }));

    let r =
        test_epoch_boundary_inner(genesis, governance_wallet, temp_path, &mut new_validator).await;
    kill_procs(&procs);
    r
}

/// Create genesis for this test.
///
/// Funds a new validator and the governance wallet to issue NFTs.
/// This method also configures the initial committee to start the network.
fn create_genesis_for_test(
    temp_path: &Path,
    new_validator: Address,
    governance_wallet: Address,
    committee: &Vec<(&str, Address)>,
) -> eyre::Result<Genesis> {
    // use same passphrase for all nodes
    let passphrase = Some(NODE_PASSWORD.to_string());

    // create validator info for "new" validator to join
    let new_validator_path = temp_path.join(NEW_VALIDATOR);
    create_validator_info(&new_validator_path, &new_validator.to_string(), passphrase.clone())?;

    // fund governance to issue NFT and new validator to stake
    let accounts = vec![
        (
            governance_wallet,
            GenesisAccount::default().with_balance(U256::from(parse_ether("50_000_000")?)), /* 50mil TEL */
        ),
        (
            new_validator,
            GenesisAccount::default().with_balance(U256::from(parse_ether("2_000_000")?)), /* double stake */
        ),
    ];

    let shared_genesis_dir = temp_path.join("shared-genesis");

    // create the initial committee of validators and create genesis
    let genesis = config_committee(
        temp_path,
        &shared_genesis_dir,
        passphrase,
        governance_wallet,
        accounts,
        committee,
    )?;

    // copy genesis for new validator
    std::fs::create_dir_all(new_validator_path.join("genesis"))?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/committee.yaml"),
        new_validator_path.join("genesis/committee.yaml"),
    )?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/genesis.yaml"),
        new_validator_path.join("genesis/genesis.yaml"),
    )?;
    std::fs::copy(
        shared_genesis_dir.join("parameters.yaml"),
        new_validator_path.join("parameters.yaml"),
    )?;

    Ok(genesis)
}

/// Configure the initial committee and fund accounts for network genesis.
///
/// All data is written to file.
fn config_committee(
    temp_path: &Path,
    shared_genesis_dir: &Path,
    passphrase: Option<String>,
    consensus_registry_owner: Address,
    accounts: Vec<(Address, GenesisAccount)>,
    validators: &Vec<(&str, Address)>,
) -> eyre::Result<Genesis> {
    // create shared genesis dir
    let copy_path = shared_genesis_dir.join("genesis/validators");
    std::fs::create_dir_all(&copy_path)?;
    // create validator info and copy to shared genesis dir
    for (v, addr) in validators.iter() {
        let dir = temp_path.join(v);
        // init genesis ceremony to create committee files
        create_validator_info(&dir, &addr.to_string(), passphrase.clone())?;

        // copy to shared genesis dir
        std::fs::copy(dir.join("node-info.yaml"), copy_path.join(format!("{v}.yaml")))?;
    }

    // configuration for ConesnsusRegistry to pass through CLI
    let min_withdrawal = "1_000";
    let epoch_rewards = "1000";

    info!(target: "epoch-test", "creating committee!");

    // create committee from shared genesis dir
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from([
        "tn",
        "--basefee-address",
        "0x9999999999999999999999999999999999999999",
        "--consensus-registry-owner",
        &consensus_registry_owner.to_string(),
        "--initial-stake-per-validator",
        INITIAL_STAKE_AMOUNT,
        "--min-withdraw-amount",
        min_withdrawal,
        "--epoch-block-rewards",
        epoch_rewards,
        "--epoch-duration-in-secs",
        &EPOCH_DURATION.to_string(),
        "--dev-funded-account",
        "test-source",
        "--max-header-delay-ms",
        "1000",
        "--min-header-delay-ms",
        "500",
    ]);
    create_committee_command.args.execute(shared_genesis_dir.to_path_buf())?;

    // update genesis with funded accounts
    let data_dir = shared_genesis_dir.join("genesis/genesis.yaml");
    let genesis: Genesis = Config::load_from_path(&data_dir, ConfigFmt::YAML)?;
    let genesis = genesis.extend_accounts(accounts);
    Config::write_to_path(&data_dir, &genesis, ConfigFmt::YAML)?;

    // distribute updated genesis to all validators
    for (v, _addr) in validators.iter() {
        let dir = temp_path.join(v);
        std::fs::create_dir_all(dir.join("genesis"))?;
        // copy genesis files back to validator dirs
        std::fs::copy(
            shared_genesis_dir.join("genesis/committee.yaml"),
            dir.join("genesis/committee.yaml"),
        )?;
        std::fs::copy(
            shared_genesis_dir.join("genesis/genesis.yaml"),
            dir.join("genesis/genesis.yaml"),
        )?;
        std::fs::copy(shared_genesis_dir.join("parameters.yaml"), dir.join("parameters.yaml"))?;
    }

    Ok(genesis)
}

/// Start the network using the node cli command.
fn start_nodes(temp_path: &Path, validators: &Vec<(&str, Address)>) -> eyre::Result<Vec<Child>> {
    let mut children = Vec::new();
    for (v, _) in validators.into_iter() {
        let dir = temp_path.join(v);
        let mut instance = v.chars().last().expect("validator instance").to_string();

        // assign instance for "new-validator"
        if instance == "r" {
            instance = "6".to_string();
            info!(target: "epoch-test", ?v, "starting new validator");
        }
        let mut exe_path = PathBuf::from(
            std::env::var("CARGO_MANIFEST_DIR").expect("Missing CARGO_MANIFEST_DIR!"),
        );
        exe_path.push("../../target/debug/telcoin-network");
        let mut command = Command::new(exe_path);
        command
            .env("TN_BLS_PASSPHRASE", NODE_PASSWORD.to_string())
            .arg("node")
            .arg("--datadir")
            .arg(&*dir.to_string_lossy())
            .arg("--instance")
            .arg(format!("{}", instance))
            .arg("--http");

        #[cfg(feature = "faucet")]
        command
            .arg("--public-key") // If the binary is built with the faucet need this to start...
            .arg("0223382261d641424b8d8b63497a811c56f85ee89574f9853474c3e9ab0d690d99")
            .arg("--google-kms")
            .arg("--faucet-contract")
            .arg("0x0000000000000000000000000000000000000000");

        children.push(command.spawn().expect("failed to execute"));
    }

    Ok(children)
}

/// Generate all the transactions needed for the new validator to be shuffled into the committee.
fn generate_new_validator_txs(
    temp_path: &Path,
    chain: Arc<RethChainSpec>,
    new_validator: &mut TransactionFactory,
    governance_wallet: &mut TransactionFactory,
) -> eyre::Result<Vec<Vec<u8>>> {
    // read bls public key from fs for new validator
    let new_validator_path = temp_path.join(NEW_VALIDATOR);
    let new_validator_info = Config::load_from_path_or_default::<NodeInfo>(
        new_validator_path.join("node-info.yaml").as_path(),
        ConfigFmt::YAML,
    )?;

    // governance issue nft to new validator tx
    let calldata = ConsensusRegistry::mintCall { validatorAddress: new_validator.address() }
        .abi_encode()
        .into();
    let mint_nft = governance_wallet.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        calldata,
    );

    // stake tx
    let proof = ConsensusRegistry::ProofOfPossession {
        uncompressedPubkey: new_validator_info.bls_public_key.serialize().into(),
        uncompressedSignature: new_validator_info.proof_of_possession.serialize().into(),
    };
    let calldata = ConsensusRegistry::stakeCall {
        blsPubkey: new_validator_info.bls_public_key.compress().into(),
        proofOfPossession: proof,
    }
    .abi_encode()
    .into();
    let stake_tx = new_validator.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        parse_ether(INITIAL_STAKE_AMOUNT)?,
        calldata,
    );

    // activation tx
    let calldata = ConsensusRegistry::activateCall {}.abi_encode().into();
    let activate_tx = new_validator.create_eip1559_encoded(
        chain.clone(),
        None,
        100,
        Some(CONSENSUS_REGISTRY_ADDRESS),
        U256::ZERO,
        calldata,
    );

    Ok(vec![mint_nft, stake_tx, activate_tx])
}
