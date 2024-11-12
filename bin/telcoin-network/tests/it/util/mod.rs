use clap::Parser;
use reth::{
    providers::ExecutionOutcome,
    tasks::{TaskExecutor, TaskManager},
    CliContext,
};
use reth_chainspec::ChainSpec;
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider};
use reth_primitives::SealedHeader;
use std::{path::PathBuf, sync::Arc};
use telcoin_network::{genesis::GenesisArgs, keytool::KeyArgs, node::NodeCommand};
use tn_node::launch_node;
use tn_test_utils::{default_test_execution_node, execution_outcome_for_tests, CommandParser};
use tn_types::{TransactionSigned, WorkerBlock};
use tokio::task::JoinHandle;
use tracing::error;

pub static IT_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Execute genesis ceremony inside tempdir
pub async fn create_validator_info(datadir: &str, address: &str) -> eyre::Result<()> {
    // init genesis
    let init_command = CommandParser::<GenesisArgs>::parse_from([
        "tn",
        "init",
        "--datadir",
        datadir,
        "--dev-funded-account",
        "test-source",
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
    task_executor: &TaskExecutor,
    chain: Arc<ChainSpec>,
    contract_address: &str,
) -> eyre::Result<Vec<JoinHandle<()>>> {
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

    let mut node_handles = Vec::with_capacity(validators.len());
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

        let cli_ctx = CliContext { task_executor: task_executor.clone() };

        // update genesis with seeded accounts
        command.chain = chain.clone();

        // collect join handles
        node_handles.push(task_executor.spawn_critical(
            v,
            Box::pin(async move {
                let err = command
                    .execute(
                        cli_ctx,
                        false, // don't overwrite chain with the default
                        |mut builder, faucet_args, tn_datadir| async move {
                            builder.opt_faucet_args = Some(faucet_args);
                            let evm_config = EthEvmConfig::default();
                            let executor = EthExecutorProvider::new(
                                std::sync::Arc::clone(&builder.node_config.chain),
                                evm_config,
                            );
                            launch_node(builder, executor, evm_config, tn_datadir).await
                        },
                    )
                    .await;
                error!("{:?}", err);
            }),
        ));
    }

    Ok(node_handles)
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
    // create execution components
    let manager = TaskManager::current();
    let executor = manager.executor();
    let execution_node = default_test_execution_node(Some(chain.clone()), None, executor)?;
    let provider = execution_node.get_provider().await;
    let block_executor = execution_node.get_block_executor().await;

    // execute batch
    let batch = WorkerBlock::new_for_test(raw_txs_to_execute, SealedHeader::default());
    let parent = chain.sealed_genesis_header();
    let execution_outcome =
        execution_outcome_for_tests(&batch, &parent, &provider, &block_executor);

    Ok(execution_outcome)
}
