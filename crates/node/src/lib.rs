// SPDX-License-Identifier: Apache-2.0
// Library for managing all components used by a full-node in a single process.

#![allow(missing_docs)]

use engine::TnBuilder;
use manager::EpochManager;
use tn_config::{KeyConfig, TelcoinDirs};
use tn_primary::ConsensusBusApp;
use tn_rpc::{EngineToPrimary, RpcNodeInfo};
use tn_storage::consensus::ConsensusChain;
use tn_types::{ConsensusHeader, Epoch, EpochCertificate, EpochDigest, EpochRecord};
use tokio::task::JoinHandle;

pub mod engine;
mod error;
mod health;
mod manager;
mod metrics;
pub mod primary;
pub mod worker;
pub use manager::{
    build_epoch_record, catchup_accumulator, derive_base_fees_for_entered_epoch,
    derive_idle_worker_fee, fold_next_epoch_base_fees, sync_num_workers_from_chain,
    DerivedBaseFees, ExecStateExporter, ExportOutcome,
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
    let consensus_db = manager::open_consensus_db(&tn_datadir);

    // run the node
    // Note this is the "entry task" for the node and the caller needs to wait on the JoinHandle
    // then exit.
    tokio::spawn(async move {
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
        result
    })
}

#[derive(Debug)]
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

    fn node_info(&self) -> &tn_rpc::RpcNodeInfo {
        &self.node_info
    }
}

#[cfg(test)]
mod clippy {
    use rand as _;
    use tn_network_types as _;
    use tn_test_utils as _;
}
