//! Read-only HTTP directory of verified Telcoin DHT node records.

mod cli;
mod directory;
mod error;
mod sources;

use clap::Parser;
use cli::Args;
use directory::Snapshot;
use error::Error;
use std::{future::IntoFuture, sync::Arc};
use tokio::{net::TcpListener, sync::RwLock};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

/// Bind HTTP and run the independent directory refresh until shutdown.
#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let args = Args::parse();
    let sources = sources::Sources::new(&args)?;
    let client = tn_kad_client::Client::new(
        args.chain_id(),
        args.network(),
        args.bootstrap(),
        args.timeout(),
    )
    .map_err(Error::Kad)?;
    let listener = TcpListener::bind(args.bind()).await.map_err(Error::Io)?;
    let shared = Arc::new(RwLock::new(Snapshot::new(args.chain_id(), args.network_name())));
    let mut refresh =
        tokio::spawn(directory::refresh(shared.clone(), sources, client, args.refresh_interval()));
    info!(address = %listener.local_addr().map_err(Error::Io)?, "node record API listening");
    let server = axum::serve(listener, directory::router(shared))
        .with_graceful_shutdown(async {
            tokio::signal::ctrl_c()
                .await
                .unwrap_or_else(|error| warn!(%error, "shutdown signal failed"));
        })
        .into_future();
    let result = tokio::select! {
        result = server => result.map_err(Error::Io),
        result = &mut refresh => result.map_err(Error::Task).and_then(|()| Err(Error::RefreshStopped)),
    };
    refresh.abort();
    result
}
