use clap::Parser;
use reth_chainspec::ChainSpec;
use std::{path::PathBuf, sync::Arc};
use telcoin_network::{genesis::GenesisArgs, keytool::KeyArgs, node::NodeCommand};
use tn_node::launch_node;
use tn_test_utils::CommandParser;
use tracing::error;

pub static IT_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Execute genesis ceremony inside tempdir
pub fn create_validator_info(datadir: &str, address: &str) -> eyre::Result<()> {
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
    init_command.args.execute()?;

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
    keys_command.args.execute()?;

    // add validator
    let add_validator_command =
        CommandParser::<GenesisArgs>::parse_from(["tn", "add-validator", "--datadir", datadir]);
    add_validator_command.args.execute()
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
        create_validator_info(datadir, addr)?;

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
    create_committee_command.args.execute()?;

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
pub fn spawn_local_testnet(chain: Arc<ChainSpec>, contract_address: &str) -> eyre::Result<()> {
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
        create_validator_info(datadir, "0")?;

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
    create_committee_command.args.execute()?;

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

        std::thread::spawn(|| {
            let err = command.execute(
                false, // don't overwrite chain with the default
                |mut builder, faucet_args, tn_datadir| {
                    builder.opt_faucet_args = Some(faucet_args);
                    launch_node(builder, tn_datadir)
                },
            );
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
#[cfg(feature = "faucet")]
use tn_types::{Address, U256};

/// RPC request to continually check until an account balance is above 0.
///
/// Warning: this should only be called with a timeout - could result in infinite loop otherwise.
#[cfg(feature = "faucet")]
pub async fn ensure_account_balance_infinite_loop(
    client: &jsonrpsee::http_client::HttpClient,
    address: Address,
    expected_bal: U256,
) -> eyre::Result<U256> {
    while let Ok(bal) =
        client.request::<String, _>("eth_getBalance", jsonrpsee::rpc_params!(address)).await
    {
        tracing::debug!(target: "faucet-test", "{address} bal: {bal:?}");
        let balance = U256::from_str(&bal)?;

        // return Ok if expected bal
        if balance == expected_bal {
            return Ok(balance);
        }

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    Ok(U256::ZERO)
}
