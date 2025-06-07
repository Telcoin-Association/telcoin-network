use super::{
    error::{WorkerNetworkError, WorkerNetworkResult},
    message::WorkerGossip,
    WorkerNetworkHandle,
};
use std::sync::Arc;
use tn_config::ConsensusConfig;
use tn_network_libp2p::GossipMessage;
use tn_network_types::{WorkerOthersBatchMessage, WorkerToPrimaryClient};
use tn_storage::tables::Batches;
use tn_types::{
    now, try_decode, Batch, BatchValidation, BlockHash, Database, SealedBatch, WorkerId,
};

/// The type that handles requests from peers.
#[derive(Clone)]
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
        let GossipMessage { data, source: _, sequence_number: _, topic: _ } = msg;

        // gossip is uncompressed
        let gossip = try_decode(data)?;

        match gossip {
            WorkerGossip::Batch(batch_hash) => {
                // Retrieve the block...
                let store = self.consensus_config.node_storage();
                if !matches!(store.get::<Batches>(&batch_hash), Ok(Some(_))) {
                    // If we don't have this batch already then try to get it.
                    // If we are CVV then we should already have it.
                    // This allows non-CVVs to pre fetch batches they will soon need.
                    match self.network_handle.request_batches(vec![batch_hash]).await {
                        Ok(batches) => {
                            if let Some(batch) = batches.first() {
                                store.insert::<Batches>(&batch.digest(), batch).map_err(|e| {
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
    pub(crate) async fn process_report_batch(
        &self,
        sealed_batch: SealedBatch,
    ) -> WorkerNetworkResult<()> {
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

    /// Attempt to return requested batches.
    pub(crate) async fn process_request_batches(
        &self,
        batch_digests: Vec<BlockHash>,
    ) -> WorkerNetworkResult<Vec<Batch>> {
        const MAX_REQUEST_BATCHES_RESPONSE_SIZE: usize = 6_000_000;
        const BATCH_DIGESTS_READ_CHUNK_SIZE: usize = 200;
        let store = self.consensus_config.node_storage().clone();

        let digests_chunks = batch_digests
            .chunks(BATCH_DIGESTS_READ_CHUNK_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();
        let mut batches = Vec::new();
        let mut total_size = 0;

        for digests_chunks in digests_chunks {
            let stored_batches =
                store.multi_get::<Batches>(digests_chunks.iter()).map_err(|e| {
                    WorkerNetworkError::Internal(format!("failed to read from batch store: {e:?}"))
                })?;

            for stored_batch in stored_batches.into_iter().flatten() {
                let batch_size = stored_batch.size();
                if total_size + batch_size <= MAX_REQUEST_BATCHES_RESPONSE_SIZE {
                    batches.push(stored_batch);
                    total_size += batch_size;
                } else {
                    break;
                }
            }
        }

        Ok(batches)
    }
}
