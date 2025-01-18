use clap::Parser;
use reth::providers::ExecutionOutcome;
use reth_chainspec::ChainSpec;
use std::{path::PathBuf, sync::Arc};
use telcoin_network::{genesis::GenesisArgs, keytool::KeyArgs, node::NodeCommand};
use tn_node::launch_node;
use tn_test_utils::{default_test_execution_node, execution_outcome_for_tests, CommandParser};
use tn_types::{TaskManager, TransactionSigned, WorkerBlock};
use tracing::error;

pub static IT_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Execute genesis ceremony inside tempdir
pub async fn create_validator_info(datadir: &str, address: &str) -> eyre::Result<()> {
    // init genesis
    // Note, we speed up block times for tests.
    let init_command = CommandParser::<GenesisArgs>::parse_from([
        "tn",
        "init",
        "--datadir",
        datadir,
        "--dev-funded-account",
        "test-source",
        "--max-header-delay-ms",
        "1000",
        "--min-header-delay-ms",
        "1000",
    ]);
    init_command.args.execute().await?;

    // keytool
    let keys_command = CommandParser::<KeyArgs>::parse_from([
        "tn",
        "generate",
        "validator",
        "--datadir",
        datadir,
        "--address",
        address,
    ]);
    keys_command.args.execute().await?;

    // add validator
    let add_validator_command =
        CommandParser::<GenesisArgs>::parse_from(["tn", "add-validator", "--datadir", datadir]);
    add_validator_command.args.execute().await
}

/// Create validator info, genesis ceremony, and spawn node command with faucet active.
pub async fn config_local_testnet(temp_path: PathBuf) -> eyre::Result<()> {
    let validators = [
        ("validator-1", "0x1111111111111111111111111111111111111111"),
        ("validator-2", "0x2222222222222222222222222222222222222222"),
        ("validator-3", "0x3333333333333333333333333333333333333333"),
        ("validator-4", "0x4444444444444444444444444444444444444444"),
    ];

    // create shared genesis dir
    let shared_genesis_dir = temp_path.join("shared-genesis");
    let copy_path = shared_genesis_dir.join("genesis/validators");
    std::fs::create_dir_all(&copy_path)?;

    // create validator info and copy to shared genesis dir
    for (v, addr) in validators.into_iter() {
        let dir = temp_path.join(v);
        let datadir = dir.to_str().expect("validator temp dir");
        // init genesis ceremony to create committee / worker_cache files
        create_validator_info(datadir, addr).await?;

        // copy to shared genesis dir
        let copy = dir.join("genesis/validators");
        for config in std::fs::read_dir(copy)? {
            let entry = config?;
            std::fs::copy(entry.path(), copy_path.join(entry.file_name()))?;
        }
    }

    // create committee from shared genesis dir
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from([
        "tn",
        "create-committee",
        "--datadir",
        shared_genesis_dir.to_str().expect("shared genesis dir"),
    ]);
    create_committee_command.args.execute().await?;

    for (v, _addr) in validators.into_iter() {
        let dir = temp_path.join(v);
        // copy genesis files back to validator dirs
        std::fs::copy(
            shared_genesis_dir.join("genesis/committee.yaml"),
            dir.join("genesis/committee.yaml"),
        )?;
        std::fs::copy(
            shared_genesis_dir.join("genesis/worker_cache.yaml"),
            dir.join("genesis/worker_cache.yaml"),
        )?;
    }

    Ok(())
}

/// Create validator info, genesis ceremony, and spawn node command with faucet active.
pub async fn spawn_local_testnet(
    chain: Arc<ChainSpec>,
    contract_address: &str,
) -> eyre::Result<()> {
    // create temp path for test
    let temp_path = tempfile::TempDir::new().expect("tempdir is okay").into_path();

    let validators = ["validator-1", "validator-2", "validator-3", "validator-4"];

    // create shared genesis dir
    let shared_genesis_dir = temp_path.join("shared-genesis");
    let copy_path = shared_genesis_dir.join("genesis/validators");
    std::fs::create_dir_all(&copy_path)?;

    // create validator info and copy to shared genesis dir
    for v in validators.into_iter() {
        let dir = temp_path.join(v);
        let datadir = dir.to_str().expect("validator temp dir");
        // init genesis ceremony to create committee / worker_cache files
        create_validator_info(datadir, "0").await?;

        // copy to shared genesis dir
        let copy = dir.join("genesis/validators");
        for config in std::fs::read_dir(copy)? {
            let entry = config?;
            std::fs::copy(entry.path(), copy_path.join(entry.file_name()))?;
        }
    }

    // create committee from shared genesis dir
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from([
        "tn",
        "create-committee",
        "--datadir",
        shared_genesis_dir.to_str().expect("shared genesis dir"),
    ]);
    create_committee_command.args.execute().await?;

    let task_executor = TaskManager::default();
    for v in validators.into_iter() {
        let dir = temp_path.join(v);
        let datadir = dir.to_str().expect("validator temp dir");

        // copy genesis files back to validator dirs
        std::fs::copy(
            shared_genesis_dir.join("genesis/committee.yaml"),
            dir.join("genesis/committee.yaml"),
        )?;
        std::fs::copy(
            shared_genesis_dir.join("genesis/worker_cache.yaml"),
            dir.join("genesis/worker_cache.yaml"),
        )?;

        // use genesis file
        let genesis_json_path = dir.join("genesis/genesis.json");
        std::fs::copy(shared_genesis_dir.join("genesis/genesis.json"), &genesis_json_path)?;

        let instance = v.chars().last().expect("validator instance").to_string();

        #[cfg(feature = "faucet")]
        let mut command = NodeCommand::<tn_faucet::FaucetArgs>::parse_from([
            "tn",
            "--dev",
            "--datadir",
            datadir,
            //
            // TODO: debug max-block doesn't work
            //
            // "--debug.max-block",
            // "5",
            // "--debug.terminate",
            "--chain",
            genesis_json_path.to_str().expect("genesis_json_path casts to &str"),
            "--instance",
            &instance,
            "--google-kms",
            "--contract-address",
            contract_address,
        ]);
        #[cfg(not(feature = "faucet"))]
        let mut command = NodeCommand::parse_from([
            "tn",
            "--public-key",
            "0223382261d641424b8d8b63497a811c56f85ee89574f9853474c3e9ab0d690d99",
            "--dev",
            "--datadir",
            datadir,
            //
            // TODO: debug max-block doesn't work
            //
            // "--debug.max-block",
            // "5",
            // "--debug.terminate",
            "--chain",
            genesis_json_path.to_str().expect("genesis_json_path casts to &str"),
            "--instance",
            &instance,
            "--contract-address",
            contract_address,
        ]);

        // update genesis with seeded accounts
        command.chain = chain.clone();

        task_executor.spawn_task(v, async move {
            let err = command
                .execute(
                    false, // don't overwrite chain with the default
                    |mut builder, faucet_args, tn_datadir| async move {
                        builder.opt_faucet_args = Some(faucet_args);
                        launch_node(builder, tn_datadir).await
                    },
                )
                .await;
            error!("{:?}", err);
        });
    }

    Ok(())
}

// imports for traits used in faucet tests only
#[cfg(feature = "faucet")]
use jsonrpsee::core::client::ClientT;
#[cfg(feature = "faucet")]
use std::str::FromStr as _;

/// RPC request to continually check until an account balance is above 0.
///
/// Warning: this should only be called with a timeout - could result in infinite loop otherwise.
#[cfg(feature = "faucet")]
pub async fn ensure_account_balance_infinite_loop(
    client: &jsonrpsee::http_client::HttpClient,
    address: reth_primitives::Address,
    expected_bal: reth_primitives::U256,
) -> eyre::Result<reth_primitives::U256> {
    while let Ok(bal) =
        client.request::<String, _>("eth_getBalance", jsonrpsee::rpc_params!(address)).await
    {
        tracing::debug!(target: "faucet-test", "{address} bal: {bal:?}");
        let balance = reth_primitives::U256::from_str(&bal)?;

        // return Ok if expected bal
        if balance == expected_bal {
            return Ok(balance);
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(reth_primitives::U256::ZERO)
}

/// Test utility to get desired state changes from a temporary genesis for a subsequent one.
pub async fn get_contract_state_for_genesis(
    chain: Arc<ChainSpec>,
    raw_txs_to_execute: Vec<TransactionSigned>,
) -> eyre::Result<ExecutionOutcome> {
    let execution_node = default_test_execution_node(Some(chain.clone()), None)?;
    let provider = execution_node.get_provider().await;
    let batch_executor = execution_node.get_batch_executor().await;

    // execute batch
    let parent = chain.sealed_genesis_header();
    let batch = WorkerBlock {
        transactions: raw_txs_to_execute,
        parent_hash: parent.hash(),
        ..Default::default()
    };
    let execution_outcome =
        execution_outcome_for_tests(&batch, &parent, &provider, &batch_executor);

    Ok(execution_outcome)
}
