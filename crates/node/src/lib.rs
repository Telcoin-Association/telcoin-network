// SPDX-License-Identifier: Apache-2.0
// Library for managing all components used by a full-node in a single process.

#![allow(missing_docs)]

use engine::TnBuilder;
use manager::EpochManager;
use tn_config::{KeyConfig, TelcoinDirs};
use tokio::task::JoinHandle;

pub mod engine;
mod engine_to_primary_rpc;
mod error;
mod health;
mod manager;
pub mod primary;
pub mod worker;
pub(crate) use engine_to_primary_rpc::EngineToPrimaryRpc;
pub use manager::catchup_accumulator;

/// Launch all components for the node.
///
/// Worker, Primary, and Execution.
/// This will possibly "loop" to launch multiple times in response to
/// a nodes mode changes.  This ensures a clean state and fresh tasks
/// when switching modes.
pub fn launch_node<P>(
    builder: TnBuilder,
    tn_datadir: P,
    key_config: KeyConfig,
) -> JoinHandle<eyre::Result<()>>
where
    P: TelcoinDirs + Clone + 'static,
{
    let consensus_db = manager::open_consensus_db(&tn_datadir);

    // create the epoch manager
    let mut epoch_manager = EpochManager::new(builder, tn_datadir, consensus_db, key_config);
    // run the node
    // Note this is the "entry task" for the node and the caller needs to wait on the JoinHandle
    // then exit.
    tokio::spawn(async move { epoch_manager.run().await })
}
