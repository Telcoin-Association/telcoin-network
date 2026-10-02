// SPDX-License-Identifier: Apache-2.0

//! Library for managing all components used by a full-node in a single process.
//!
//! `tn-node` assembles the consensus, execution, storage, and networking crates into a running
//! full node and drives them across epoch boundaries with `EpochManager`.

#![allow(missing_docs)]

use engine::TnBuilder;
use manager::EpochManager;
use tn_config::{KeyConfig, TelcoinDirs};
use tn_primary::ConsensusBusApp;
use tn_rpc::{ConsensusStorageError, EngineToPrimary, RpcNodeInfo};
use tn_storage::consensus::ConsensusChain;
use tn_types::{
    ConsensusHeader, ConsensusHeaderDigest, Epoch, EpochCertificate, EpochDigest, EpochRecord,
};
use tokio::task::JoinHandle;

pub mod engine;
mod error;
mod health;
mod manager;
mod metrics;
mod network_readiness;
pub mod primary;
pub mod worker;
pub use manager::{
    build_epoch_record, catchup_accumulator, read_base_fees_for_entered_epoch,
    sync_num_workers_from_chain, EpochBaseFees, ExecStateExporter, ExportOutcome,
};

#[cfg(test)]
use tempfile as _;

/// Launch all components for the node.
///
/// Worker, Primary, and Execution.
/// This will possibly "loop" to launch multiple times in response to
/// a node's mode changes.  This ensures a clean state and fresh tasks
/// when switching modes.
pub fn launch_node<P>(
    builder: TnBuilder,
    tn_datadir: P,
    key_config: KeyConfig,
    version: &'static str,
) -> JoinHandle<eyre::Result<()>>
where
    P: TelcoinDirs + Clone + 'static,
{
    // run the node
    // Note this is the "entry task" for the node and the caller needs to wait on the JoinHandle
    // then exit.
    tokio::spawn(async move {
        // Refuse to run a second writer against this datadir, and hold the PID lockfile for the
        // node's whole lifetime: it is released on the clean-shutdown path below, and on any early
        // error or panic via the guard's `Drop`. The CLI takes it before it opens the execution
        // engine's database (whose own lock only coordinates concurrent users, it does not
        // exclude a second node) and hands it over in the builder; any other caller has it taken
        // here, still before consensus storage is touched (its open is fail-fast and would
        // otherwise panic on a datadir another node holds before this clear error is reached). A
        // crashed holder never blocks a restart: the kernel releases its `flock` when the process
        // exits. This is TN-owned and does not depend on the execution engine's own lock.
        let mut builder = builder;
        let _pid_lock = match builder.take_pid_lock() {
            Some(lock) => lock,
            None => match tn_config::PidLock::acquire(&tn_datadir) {
                Ok(lock) => lock,
                Err(err) => {
                    tracing::error!("Error running node (datadir already locked): {err}");
                    return Err(err);
                }
            },
        };
        let consensus_db = manager::open_consensus_db(&tn_datadir);

        // create the epoch manager
        let mut epoch_manager =
            match EpochManager::new(builder, tn_datadir, consensus_db, key_config, version).await {
                Ok(epoch_manager) => epoch_manager,
                Err(err) => {
                    tracing::error!("Error running node (creating EpochManager): {err}");
                    return Err(err);
                }
            };
        let result = epoch_manager.run().await;
        if let Err(err) = &result {
            tracing::error!("Error running node: {err}");
        }
        // Async-close consensus storage so its background-thread joins don't block this tokio
        // worker on `Drop` (the runtime is still alive here, inside `block_on`). `run()` has
        // already persisted and awaited task shutdown, so `epoch_manager` normally holds
        // the last reference; if a winding-down RPC clone briefly outlives it, `shutdown()`
        // bounds the wait and then force-seals rather than leaving the pack unsealed.
        epoch_manager.shutdown().await;
        result
    })
}

/// Consensus and node metadata exposed by each worker's RPC server.
#[derive(Clone, Debug)]
pub struct EngineToPrimaryRpc {
    /// Container for consensus channels.
    consensus_bus: ConsensusBusApp,
    /// Consensus Chain DB
    consensus_chain: ConsensusChain,
    /// Static node info to provide clients.
    node_info: RpcNodeInfo,
}

impl EngineToPrimaryRpc {
    pub fn new(
        consensus_bus: ConsensusBusApp,
        consensus_chain: ConsensusChain,
        node_info: RpcNodeInfo,
    ) -> Self {
        Self { consensus_bus, consensus_chain, node_info }
    }

    /// Retrieve the consensus header by number.
    async fn get_epoch_by_number(&self, epoch: Epoch) -> Option<(EpochRecord, EpochCertificate)> {
        if let Some((r, Some(c))) = self.consensus_chain.epochs().get_epoch_by_number(epoch).await {
            Some((r, c))
        } else {
            None
        }
    }

    /// Retrieve the consensus header by hash
    async fn get_epoch_by_hash(
        &self,
        hash: EpochDigest,
    ) -> Option<(EpochRecord, EpochCertificate)> {
        if let Some((r, Some(c))) = self.consensus_chain.epochs().get_epoch_by_hash(hash).await {
            Some((r, c))
        } else {
            None
        }
    }
}

impl EngineToPrimary for EngineToPrimaryRpc {
    fn get_latest_consensus_block(&self) -> ConsensusHeader {
        self.consensus_bus.last_consensus_header().borrow().clone().unwrap_or_default()
    }

    async fn epoch(
        &self,
        epoch: Option<Epoch>,
        hash: Option<EpochDigest>,
    ) -> Option<(EpochRecord, EpochCertificate)> {
        match (epoch, hash) {
            (_, Some(hash)) => self.get_epoch_by_hash(hash).await,
            (Some(epoch), _) => self.get_epoch_by_number(epoch).await,
            (None, None) => None,
        }
    }

    async fn consensus_header_by_digest(
        &self,
        epoch: Epoch,
        digest: ConsensusHeaderDigest,
    ) -> Result<Option<ConsensusHeader>, ConsensusStorageError> {
        self.consensus_chain.consensus_header_by_digest(epoch, digest).await.map_err(|e| {
            // rpc callers only learn that the lookup failed, never why; the warn is where
            // operators see the storage error
            tracing::warn!(
                target: "engine",
                ?e,
                epoch,
                ?digest,
                "consensus header lookup failed"
            );
            ConsensusStorageError
        })
    }

    fn node_info(&self) -> &tn_rpc::RpcNodeInfo {
        &self.node_info
    }

    fn node_mode(&self) -> tn_types::NodeMode {
        self.consensus_bus.current_node_mode().into()
    }
}

#[cfg(test)]
mod clippy {
    use rand as _;
    use tn_network_types as _;
    use tn_test_utils as _;
}
