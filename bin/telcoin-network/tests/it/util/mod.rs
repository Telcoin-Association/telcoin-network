use std::path::PathBuf;

use clap::Parser;
use narwhal_test_utils::CommandParser;
use telcoin_network::{genesis::GenesisArgs, keytool::KeyArgs};

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
