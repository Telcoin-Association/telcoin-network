//! Telcoin Network worker gateway.
//!
//! A stateless reverse proxy that fronts the worker JSON-RPC endpoint. It
//! forwards the full JSON-RPC method surface (`eth_*` / `net_*` / `web3_*` /
//! `tn_*`) unchanged to a ready upstream worker, gates traffic on a polled
//! per-worker readiness signal (`GET /health/workers`), and exposes its own
//! liveness and readiness endpoints for orchestration. See the crate
//! `README.md` for the configuration, forwarding, and readiness contracts.

mod app;
mod cli;
mod config;
mod error;
mod proxy;
mod ratelimit;
mod readiness;
mod server;
mod telemetry;

use clap::Parser as _;

use crate::cli::Cli;

fn main() {
    if let Err(err) = try_main() {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}

/// Parse the CLI, initialize tracing, and run the gateway on a multi-thread
/// runtime until SIGTERM / ctrl-c.
fn try_main() -> eyre::Result<()> {
    let cli = Cli::parse();
    init_tracing(&cli.log_filter);
    let settings = cli.into_settings()?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .thread_name("worker-gateway")
        .enable_io()
        .enable_time()
        .build()?;

    runtime.block_on(app::run(settings))
}

/// Initialize a fmt tracing subscriber honouring the `--log-filter` directive
/// (and `RUST_LOG`).
fn init_tracing(filter: &str) {
    let env_filter = tracing_subscriber::EnvFilter::builder().parse_lossy(filter);
    tracing_subscriber::fmt().with_env_filter(env_filter).init();
}
