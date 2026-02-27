//! The type that handles core logic for requests between workers.
use super::{
    error::{WorkerNetworkError, WorkerNetworkResult},
    handle::WorkerNetworkHandle,
    message::WorkerGossip,
};
use crate::network::{stream_codec, PendingBatchStream};
use futures::AsyncWriteExt as _;
use std::{collections::HashSet, sync::Arc};
use tn_config::ConsensusConfig;
use tn_network_libp2p::GossipMessage;
use tn_network_types::{WorkerOthersBatchMessage, WorkerToPrimaryClient};
use tn_storage::tables::Batches;
use tn_types::{
    ensure, now, try_decode, BatchValidation, BlsPublicKey, Database, SealedBatch, WorkerId, B256,
};
use tracing::{debug, warn};

/// The type that handles requests from peers.
#[derive(Clone, Debug)]
pub struct RequestHandler<DB> {
    /// This worker's id.
    id: WorkerId,
    /// The type that validates batches received from peers.
    validator: Arc<dyn BatchValidation>,
    /// Consensus config with access to database.
    consensus_config: ConsensusConfig<DB>,
    /// Network handle- so we can respond to gossip.
    network_handle: WorkerNetworkHandle,
}

impl<DB> RequestHandler<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        id: WorkerId,
        validator: Arc<dyn BatchValidation>,
        consensus_config: ConsensusConfig<DB>,
        network_handle: WorkerNetworkHandle,
    ) -> Self {
        Self { id, validator, consensus_config, network_handle }
    }

    /// Process gossip from the committee.
    ///
    /// Workers gossip the Batch Digests once accepted so that non-committee peers can request the
    /// Batch.
    pub(super) async fn process_gossip(&self, msg: &GossipMessage) -> WorkerNetworkResult<()> {
        // deconstruct message
        let GossipMessage { data, source: _, sequence_number: _, topic } = msg;

        // gossip is uncompressed
        let gossip = try_decode(data)?;

        match gossip {
            WorkerGossip::Batch(batch_hash) => {
                ensure!(
                    topic.to_string().eq(&tn_config::LibP2pConfig::worker_batch_topic()),
                    WorkerNetworkError::InvalidTopic
                );
                // Retrieve the batch...
                let store = self.consensus_config.node_storage();
                if !matches!(store.get::<Batches>(&batch_hash), Ok(Some(_))) {
                    // If batch is missing from db, then request from peer.
                    // If we are a CVV then we should already have it.
                    // This allows non-CVVs to pre fetch batches they will soon need.
                    let mut missing = HashSet::from([batch_hash]);
                    match self.network_handle.request_batches(&mut missing).await {
                        Ok(batches) => {
                            if let Some((digest, batch)) = batches.first() {
                                store.insert::<Batches>(digest, batch).map_err(|e| {
                                    WorkerNetworkError::Internal(format!(
                                        "failed to write to batch store: {e}"
                                    ))
                                })?;
                            }
                        }
                        Err(e) => {
                            tracing::error!(target: "worker:network", "failed to get gossipped batch {batch_hash}: {e}");
                        }
                    }
                }
            }
            WorkerGossip::Txn(tx_bytes) => {
                ensure!(
                    topic.to_string().eq(&tn_config::LibP2pConfig::worker_txn_topic()),
                    WorkerNetworkError::InvalidTopic
                );
                if let Some(authority) = self.consensus_config.authority() {
                    let committee = self.consensus_config.committee();
                    let authorities = committee.authorities();
                    let size = authorities.len();
                    for (slot, auth) in authorities.into_iter().enumerate() {
                        if &auth == authority {
                            self.validator.submit_txn_if_mine(&tx_bytes, size as u64, slot as u64);
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a new reported batch.
    pub(super) async fn process_report_batch(
        &self,
        peer: &BlsPublicKey,
        sealed_batch: SealedBatch,
    ) -> WorkerNetworkResult<()> {
        // return error if reporter isn't in current committee
        if !self.consensus_config.committee_pub_keys().contains(peer) {
            return Err(WorkerNetworkError::NonCommitteeBatch);
        }

        let client = self.consensus_config.local_network().clone();
        let store = self.consensus_config.node_storage().clone();
        // validate batch - log error if invalid
        self.validator.validate_batch(sealed_batch.clone())?;

        let (mut batch, digest) = sealed_batch.split();

        // Set received_at timestamp for remote batch.
        batch.set_received_at(now());
        store.insert::<Batches>(&digest, &batch).map_err(|e| {
            WorkerNetworkError::Internal(format!("failed to write to batch store: {e}"))
        })?;

        // notify primary for payload store
        client
            .report_others_batch(WorkerOthersBatchMessage { digest, worker_id: self.id })
            .await
            .map_err(|e| WorkerNetworkError::Internal(e.to_string()))?;

        Ok(())
    }

    /// Process request to open batches sync stream.
    pub(super) async fn process_request_batches_stream(
        &self,
        peer: BlsPublicKey,
        pending_request: Option<PendingBatchStream>,
        mut stream: libp2p::Stream,
        request_digest: B256,
    ) -> WorkerNetworkResult<()> {
        // `None` indicates unexpected request
        let Some(request) = pending_request else {
            // this is a protocol violation - return error for penalty
            warn!(
                target: "worker::network",
                %peer,
                ?request_digest,
                "inbound stream has no matching pending request"
            );
            return Err(WorkerNetworkError::UnknownStreamRequest(request_digest));
        };

        // process request to send batches through stream
        debug!(
            target: "worker::network",
            %peer,
            ?request_digest,
            batch_count = request.batch_digests.len(),
            "processing inbound batch stream"
        );

        let store = self.consensus_config.node_storage();
        if let Err(e) =
            stream_codec::send_batches_over_stream(&mut stream, store, &request.batch_digests).await
        {
            warn!(target: "worker::network", %peer, ?e, "failed to send batches over stream");
        }

        // attempt to close the stream gracefully
        let _ = stream.close().await;

        Ok(())
    }
}

// support IT tests
#[cfg(any(test, feature = "test-utils"))]
impl<DB> RequestHandler<DB>
where
    DB: Database,
{
    /// Publicly available for tests.
    /// See [Self::process_gossip].
    pub async fn pub_process_gossip_for_test(
        &self,
        msg: &GossipMessage,
    ) -> WorkerNetworkResult<()> {
        self.process_gossip(msg).await
    }

    /// Publicly available for tests.
    /// See [Self::process_report_batch].
    pub async fn pub_process_report_batch(
        &self,
        peer: &BlsPublicKey,
        sealed_batch: SealedBatch,
    ) -> WorkerNetworkResult<()> {
        self.process_report_batch(peer, sealed_batch).await
    }

    /// Publicly available for tests.
    /// Sends requested batches over the provided stream.
    ///
    /// This is a simplified version for tests that bypasses the pending request mechanism.
    pub async fn pub_process_request_batches_stream(
        &self,
        peer: BlsPublicKey,
        stream: libp2p::Stream,
        pending_request: Option<PendingBatchStream>,
        request_digest: B256,
    ) -> WorkerNetworkResult<()> {
        self.process_request_batches_stream(peer, pending_request, stream, request_digest).await
    }
}
