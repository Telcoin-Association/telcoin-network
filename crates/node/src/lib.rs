// SPDX-License-Identifier: Apache-2.0
// Library for managing all components used by a full-node in a single process.

#![allow(missing_docs)]

use engine::TnBuilder;
use manager::EpochManager;
use tn_config::{KeyConfig, TelcoinDirs};
use tn_primary::ConsensusBusApp;
use tn_rpc::EngineToPrimary;
use tn_storage::consensus::ConsensusChain;
use tn_types::{BlockHash, ConsensusHeader, Epoch, EpochCertificate, EpochRecord};
use tokio::task::JoinHandle;

pub mod engine;
mod error;
mod health;
mod manager;
pub mod primary;
pub mod worker;
pub use manager::catchup_accumulator;

#[cfg(test)]
use tempfile as _;

/// Launch all components for the node.
///
/// Worker, Primary, and Execution.
/// This will possibly "loop" to launch multiple times in response to
/// a nodes mode changes.  This ensures a clean state and fresh tasks
/// when switching modes.
///
/// ## ExEx Support
///
/// To run ExExes with the node, provide a custom `exex_launcher` that has ExExs installed.
/// The launcher can be created and populated like this:
///
/// ```no_run
/// use tn_exex::TnExExLauncher;
///
/// async fn my_indexer_exex<P>(ctx: tn_exex::TnExExContext<P>) -> eyre::Result<()>
/// where
///     P: tn_reth::BlockReader + Clone + Unpin + 'static,
/// {
///     // ExEx implementation here
///     Ok(())
/// }
///
/// let mut launcher = TnExExLauncher::new();
/// launcher.install("my-indexer", my_indexer_exex);
/// // Pass launcher to launch_node()
/// ```
///
/// The config's `exex` field and `--exex` CLI flag are available to custom node binaries,
/// but the launcher does not automatically filter installed ExExs.
/// If no ExExs are installed in the launcher, the node runs with zero ExEx overhead.
pub fn launch_node<P>(
    builder: TnBuilder,
    tn_datadir: P,
    key_config: KeyConfig,
    exex_launcher: Option<tn_exex::TnExExLauncher>,
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
            EpochManager::new(builder, tn_datadir, consensus_db, key_config).await;
        let result = epoch_manager.run(exex_launcher).await;
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
}

impl EngineToPrimaryRpc {
    pub fn new(consensus_bus: ConsensusBusApp, consensus_chain: ConsensusChain) -> Self {
        Self { consensus_bus, consensus_chain }
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
    async fn get_epoch_by_hash(&self, hash: BlockHash) -> Option<(EpochRecord, EpochCertificate)> {
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
        hash: Option<BlockHash>,
    ) -> Option<(EpochRecord, EpochCertificate)> {
        match (epoch, hash) {
            (_, Some(hash)) => self.get_epoch_by_hash(hash).await,
            (Some(epoch), _) => self.get_epoch_by_number(epoch).await,
            (None, None) => None,
        }
    }
}

#[cfg(test)]
mod clippy {
    use rand as _;
    use tn_network_types as _;
    use tn_test_utils as _;
}
