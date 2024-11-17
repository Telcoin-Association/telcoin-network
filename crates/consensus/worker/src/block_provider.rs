// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The receiving side of the execution layer's `BlockProvider`.
//!
//! Consensus `BlockProvider` takes a block from the EL, stores it,
//! and sends it to the quorum waiter for broadcasting to peers.

use crate::{metrics::WorkerMetrics, quorum_waiter::QuorumWaiterTrait};
use consensus_network::{local::LocalNetwork, WorkerToPrimaryClient as _};
use consensus_network_types::WorkerOwnBlockMessage;
use std::{sync::Arc, time::Duration};
use tn_storage::{tables::WorkerBlocks, traits::Database};
use tn_types::{error::BlockSealError, WorkerBlock, WorkerBlockSender, WorkerId};
use tracing::error;

#[cfg(test)]
#[path = "tests/block_provider_tests.rs"]
pub mod block_provider_tests;

/// Process blocks from EL into sealed blocks for CL.
#[derive(Clone)]
pub struct BlockProvider<DB, QW> {
    /// Our worker's id.
    id: WorkerId,
    /// Use `QuorumWaiter` to attest to blocks.
    quorum_waiter: QW,
    /// Metrics handler
    node_metrics: Arc<WorkerMetrics>,
    /// The network client to send our blocks to the primary.
    client: LocalNetwork,
    /// The block store to store our own blocks.
    store: DB,
    /// Channel sender for alternate block submision if not calling seal directly.
    tx_blocks: WorkerBlockSender,
    /// The amount of time to wait on a reply from peer before timing out.
    timeout: Duration,
}

impl<DB, QW> std::fmt::Debug for BlockProvider<DB, QW> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlockProvider for worker {}", self.id)
    }
}

impl<DB: Database, QW: QuorumWaiterTrait> BlockProvider<DB, QW> {
    pub fn new(
        id: WorkerId,
        quorum_waiter: QW,
        node_metrics: Arc<WorkerMetrics>,
        client: LocalNetwork,
        store: DB,
        timeout: Duration,
    ) -> Self {
        let (tx_blocks, mut rx_blocks) = tokio::sync::mpsc::channel(1000);
        let this = Self { id, quorum_waiter, node_metrics, client, store, tx_blocks, timeout };
        let this_clone = this.clone();
        // Spawn a little task to accept blocks from a channel and seal them that way.
        // Allows the engine to remain removed from the worker.
        tokio::spawn(async move {
            while let Some((block, tx)) = rx_blocks.recv().await {
                let res = this_clone.seal(block).await;
                if tx.send(res).is_err() {
                    error!(target: "worker::block_provider", "Error sending result to channel caller!  Channel closed.");
                }
            }
        });
        this
    }

    pub fn blocks_rx(&self) -> WorkerBlockSender {
        self.tx_blocks.clone()
    }

    /// Seal and broadcast the current block.
    pub async fn seal(&self, block: WorkerBlock) -> Result<(), BlockSealError> {
        let size = block.size();

        self.node_metrics
            .created_block_size
            .with_label_values(&["latest block size"])
            .observe(size as f64);

        let block_attest_handle = self.quorum_waiter.verify_block(block.clone(), self.timeout);

        // Wait for our block to reach quorum or fail to do so.
        match block_attest_handle.await {
            Ok(res) => {
                match res {
                    Ok(_) => {} // Block reach quorum!
                    Err(e) => {
                        return Err(match e {
                            crate::quorum_waiter::QuorumWaiterError::QuorumRejected => {
                                BlockSealError::QuorumRejected
                            }
                            crate::quorum_waiter::QuorumWaiterError::AntiQuorum => {
                                BlockSealError::AntiQuorum
                            }
                            crate::quorum_waiter::QuorumWaiterError::Timeout => {
                                BlockSealError::Timeout
                            }
                            crate::quorum_waiter::QuorumWaiterError::Network
                            | crate::quorum_waiter::QuorumWaiterError::Rpc(_) => {
                                BlockSealError::FailedQuorum
                            }
                        })
                    }
                }
            }
            Err(e) => {
                error!(target: "worker::block_provider", "Join error attempting block quorum! {e}");
                return Err(BlockSealError::FailedQuorum);
            }
        }

        // Now save it to disk
        let digest = block.digest();

        if let Err(e) = self.store.insert::<WorkerBlocks>(&digest, &block) {
            error!(target: "worker::block_provider", "Store failed with error: {:?}", e);
            return Err(BlockSealError::FatalDBFailure);
        }

        // Send the block to the primary.
        let message =
            WorkerOwnBlockMessage { digest, worker_id: self.id, worker_block: block.clone() };
        if let Err(err) = self.client.report_own_block(message).await {
            error!(target: "worker::block_provider", "Failed to report our block: {err:?}");
            // Should we return an error here?  Doing so complicates some tests but also the block
            // is sealed, etc. If we can not report our own block is this a
            // showstopper?
        }
        Ok(())
    }
}
