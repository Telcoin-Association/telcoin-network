//! Utils for IT tests.

use crate::{genesis::GenesisArgs, keytool::KeyArgs};
use clap::{Args, Parser};
use std::path::PathBuf;

/// Guard for ensuring restart tests run in-sync.
pub static IT_TEST_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// A helper type to parse Args more easily.
/// NOTE: this also lives in tn_test_utils but creates a circular dependency.
#[derive(Parser, Debug)]
pub struct CommandParser<T: Args> {
    /// The generic args to parse.
    #[clap(flatten)]
    pub args: T,
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
