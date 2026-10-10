//! Telcoin Network worker gateway.
//!
//! A mostly stateless reverse proxy that fronts the worker JSON-RPC endpoint:
//! each replica keeps only bounded per-process state, such as its rate-limit
//! buckets and upstream readiness, so any replica can serve any request. It
//! forwards JSON-RPC calls unchanged to a ready upstream worker, gates them on
//! a polled per-worker readiness signal (`GET /health/workers`), and exposes
//! its own liveness and readiness endpoints for orchestration. With
//! `--redirect-queries <URL>` the worker keeps transaction submissions and any
//! other call the README's "Query redirect" section routes to it, and
//! everything else goes to that endpoint, which has no readiness gate. See the
//! crate `README.md` for the configuration, routing, and readiness contracts.

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
