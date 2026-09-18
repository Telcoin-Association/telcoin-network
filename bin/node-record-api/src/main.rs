//! Telcoin Network node-record API binary.
//!
//! A thin shim over the `tn_node_record_api` library: parse the CLI, initialize tracing, and run
//! the daemon on a multi-thread runtime until SIGTERM / ctrl-c. See the crate `README.md` for the
//! configuration, HTTP contract, and readiness semantics.

// this binary only names clap, eyre, tokio, and the library; every other dependency is exercised
// by the library target, where the lint still applies
#![allow(unused_crate_dependencies)]

use clap::Parser as _;
use tn_node_record_api::{app, cli::Cli};

fn main() {
    if let Err(err) = try_main() {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}

/// Parse the CLI, initialize tracing, and run the daemon on a multi-thread runtime until
/// SIGTERM / ctrl-c.
fn try_main() -> eyre::Result<()> {
    let cli = Cli::parse();
    app::init_tracing(&cli.log_filter);
    let settings = cli.into_settings()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("node-record-api")
        .enable_io()
        .enable_time()
        .build()?;

    runtime.block_on(app::run(settings))
}
