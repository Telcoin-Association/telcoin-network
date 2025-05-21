// SPDX-License-Identifier: Apache-2.0
//! Library for managing all components used by a full-node in a single process.

#![warn(unused_crate_dependencies)]

use engine::TnBuilder;
use manager::EpochManager;
use tn_config::TelcoinDirs;
use tokio::runtime::Builder;
use tracing::{instrument, warn};

pub mod engine;
mod error;
mod manager;
pub mod primary;
pub mod worker;

/// Launch all components for the node.
///
/// Worker, Primary, and Execution.
/// This will possibly "loop" to launch multiple times in response to
/// a nodes mode changes.  This ensures a clean state and fresh tasks
/// when switching modes.
#[instrument(level = "info", skip_all)]
pub fn launch_node<P>(
    builder: TnBuilder,
    tn_datadir: P,
    passphrase: Option<String>,
) -> eyre::Result<()>
where
    P: TelcoinDirs + 'static,
{
    let runtime = Builder::new_multi_thread()
        .thread_name("telcoin-network")
        .enable_io()
        .enable_time()
        .build()?;

    let res = runtime.block_on(async move {
        // create the epoch manager
        let mut epoch_manager = EpochManager::new(builder, tn_datadir, passphrase)?;
        epoch_manager.run().await
    });

    // shutdown background tasks
    runtime.shutdown_background();

    // return result after shutdown
    res
}
