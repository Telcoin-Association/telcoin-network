//! Main binary for TN CLI

use telcoin_network_cli::{cli::Cli, passphrase::get_bls_passphrase_from_env};
use tn_node::launch_node;

fn main() {
    // Must be the first statement of main: reads and clears TN_BLS_PASSPHRASE
    // before any threads exist (see `get_bls_passphrase_from_env`).
    let preloaded = get_bls_passphrase_from_env();
    let cli = Cli::parse_args(); // = Cli::<NoArgs>::parse(), cli.rs:77-79

    let passphrase = cli.resolve_bls_passphrase(preloaded).unwrap_or_else(|err| {
        eprintln!("{err}");
        std::process::exit(1);
    });

    if let Err(err) = cli.run(passphrase, |builder, _, tn_datadir, key_config, version| {
        launch_node(builder, tn_datadir, key_config, version)
    }) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
