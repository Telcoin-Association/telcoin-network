//! Utilities for it tests.

use clap::Parser;
use std::path::Path;
use telcoin_network::{genesis::GenesisArgs, keytool::KeyArgs, node::NodeCommand};
use tn_node::launch_node;
use tn_types::test_utils::CommandParser;
use tracing::error;

/// Limit potential for port collisions.
pub static IT_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Execute genesis ceremony inside tempdir
fn create_validator_info(
    dir: &Path,
    address: &str,
    passphrase: Option<String>,
) -> eyre::Result<()> {
    let datadir = dir.to_str().expect("validator temp dir");

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
    keys_command.args.execute(passphrase)?;

    Ok(())
}

/// Execute observer config inside tempdir
fn create_observer_info(datadir: &str, passphrase: Option<String>) -> eyre::Result<()> {
    // keytool
    let keys_command = CommandParser::<KeyArgs>::parse_from([
        "tn",
        "generate",
        "observer",
        "--datadir",
        datadir,
        "--address",
        "0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
    ]);
    keys_command.args.execute(passphrase)
}

/// Create validator info, genesis ceremony, and spawn node command with faucet active.
pub async fn config_local_testnet(
    temp_path: &Path,
    passphrase: Option<String>,
) -> eyre::Result<()> {
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
        // init genesis ceremony to create committee / worker_cache files
        create_validator_info(&dir, addr, passphrase.clone())?;

        // copy to shared genesis dir
        std::fs::copy(dir.join("validator.yaml"), copy_path.join(format!("{v}.yaml")))?;
    }

    // Create an observer config.
    let dir = temp_path.join("observer");
    let datadir = dir.to_str().expect("observer temp dir");
    // init config ceremony for observer
    create_observer_info(datadir, passphrase.clone())?;

    // create committee from shared genesis dir
    let create_committee_command = CommandParser::<GenesisArgs>::parse_from([
        "tn",
        "--datadir",
        shared_genesis_dir.to_str().expect("shared genesis dir"),
        "--consensus-registry-owner",
        "0x00000000000000000000000000000000000007e1", // doesn't matter for tests
        "--dev-funded-account",
        "test-source",
        "--max-header-delay-ms",
        "1000",
        "--min-header-delay-ms",
        "1000",
    ]);
    create_committee_command.args.execute()?;

    for (v, _addr) in validators.into_iter() {
        let dir = temp_path.join(v);
        std::fs::create_dir_all(&dir.join("genesis"))?;
        // copy genesis files back to validator dirs
        std::fs::copy(
            shared_genesis_dir.join("genesis/committee.yaml"),
            dir.join("genesis/committee.yaml"),
        )?;
        std::fs::copy(
            shared_genesis_dir.join("genesis/worker_cache.yaml"),
            dir.join("genesis/worker_cache.yaml"),
        )?;
        std::fs::copy(
            shared_genesis_dir.join("genesis/genesis.yaml"),
            dir.join("genesis/genesis.yaml"),
        )?;
        std::fs::copy(shared_genesis_dir.join("parameters.yaml"), dir.join("parameters.yaml"))?;
    }

    let dir = temp_path.join("observer");
    // copy genesis files back to observer dirs
    std::fs::create_dir_all(dir.join("genesis"))?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/committee.yaml"),
        dir.join("genesis/committee.yaml"),
    )?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/worker_cache.yaml"),
        dir.join("genesis/worker_cache.yaml"),
    )?;
    std::fs::copy(
        shared_genesis_dir.join("genesis/genesis.yaml"),
        dir.join("genesis/genesis.yaml"),
    )?;
    std::fs::copy(shared_genesis_dir.join("parameters.yaml"), dir.join("parameters.yaml"))?;
    Ok(())
}

/// Create validator info, genesis ceremony, and spawn node command with faucet active.
pub async fn spawn_local_testnet(
    temp_path: &Path,
    #[cfg(feature = "faucet")] faucet_contract_address: &str,
) -> eyre::Result<()> {
    config_local_testnet(temp_path, None).await?;

    let validators = ["validator-1", "validator-2", "validator-3", "validator-4"];
    for v in validators.into_iter() {
        let dir = temp_path.join(v);
        let datadir = dir.to_str().expect("validator temp dir");
        let instance = v.chars().last().expect("validator instance").to_string();

        #[cfg(feature = "faucet")]
        let command = NodeCommand::<tn_faucet::FaucetArgs>::parse_from([
            "tn",
            "--http",
            "--datadir",
            datadir,
            "--instance",
            &instance,
            "--google-kms",
            "--faucet-contract",
            faucet_contract_address,
        ]);
        #[cfg(not(feature = "faucet"))]
        let command = NodeCommand::parse_from([
            "tn",
            "--http",
            "--public-key",
            "0223382261d641424b8d8b63497a811c56f85ee89574f9853474c3e9ab0d690d99",
            "--datadir",
            datadir,
            "--instance",
            &instance,
        ]);

        std::thread::spawn(|| {
            let err = command.execute(
                Some("it_test_pass".to_string()),
                |mut builder, faucet_args, tn_datadir, passphrase| {
                    builder.opt_faucet_args = Some(faucet_args);
                    launch_node(builder, tn_datadir, passphrase)
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
