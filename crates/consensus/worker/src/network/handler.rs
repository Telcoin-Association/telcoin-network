use super::{
    error::{WorkerNetworkError, WorkerNetworkResult},
    message::WorkerGossip,
    WorkerNetworkHandle,
};
use crate::WorkerResponse;
use std::sync::{Arc, LazyLock};
use tn_config::ConsensusConfig;
use tn_network_libp2p::GossipMessage;
use tn_network_types::{WorkerOthersBatchMessage, WorkerToPrimaryClient};
use tn_storage::tables::Batches;
use tn_types::{
    now, try_decode, Batch, BatchValidation, BlockHash, BlsPublicKey, Database, SealedBatch,
    WorkerId,
};
use tracing::debug;

/// The minimal length of a single, encoded, default [Batch] used to set a local min for
/// message validation.
static LOCAL_MIN_REQUEST_SIZE: LazyLock<usize> =
    LazyLock::new(|| tn_types::encode(&Batch::default()).len());
/// The minimal response wrapper using a default, empty message.
static MESSAGE_OVERHEAD: LazyLock<usize> =
    LazyLock::new(|| tn_types::encode(&WorkerResponse::RequestBatches(vec![])).len());

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
    pub(super) async fn process_report_batch(
        &self,
        peer: BlsPublicKey,
        sealed_batch: SealedBatch,
    ) -> WorkerNetworkResult<()> {
        // return error if reporter isn't in current committee
        if !self.consensus_config.committee_pub_keys().contains(&peer) {
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

    /// Attempt to return requested batches.
    pub(super) async fn process_request_batches(
        &self,
        batch_digests: Vec<BlockHash>,
        max_response_size: usize,
    ) -> WorkerNetworkResult<Vec<Batch>> {
        const BATCH_DIGESTS_READ_CHUNK_SIZE: usize = 200;

        // assume reasonable min is 1 encoded batch (no transactions)
        // NOTE: caller needs to account for batches + msg overhead, and batches must have
        // transactions
        if max_response_size < *LOCAL_MIN_REQUEST_SIZE {
            debug!(target: "cert-collector", "batch request max size too small: {}", max_response_size);
            return Err(WorkerNetworkError::InvalidRequest("Request size too small".into()));
        }

        // return error for empty batches
        if batch_digests.is_empty() {
            debug!(target: "cert-collector", "batch request empty");
            return Err(WorkerNetworkError::InvalidRequest("Empty batch digests".into()));
        }

        // use the min value between this node's max rpc message size and the requestor's reported
        // max message size
        //
        // NOTE: assume safe overhead is accounted for because the codec will also compress messages
        let local_max = self.consensus_config.network_config().libp2p_config().max_rpc_message_size
            - *MESSAGE_OVERHEAD;
        let max_message_size = max_response_size.min(local_max);

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
                if total_size + batch_size <= max_message_size {
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

// support IT tests
#[cfg(any(test, feature = "test-utils"))]
impl<DB> RequestHandler<DB>
where
    DB: Database,
{
    /// Publicly available for tests.
    /// See [Self::process_gossip].
    pub async fn pub_process_gossip(&self, msg: &GossipMessage) -> WorkerNetworkResult<()> {
        self.process_gossip(msg).await
    }

    /// Publicly available for tests.
    /// See [Self::process_report_batch].
    pub async fn pub_process_report_batch(
        &self,
        peer: BlsPublicKey,
        sealed_batch: SealedBatch,
    ) -> WorkerNetworkResult<()> {
        self.process_report_batch(peer, sealed_batch).await
    }

    /// Publicly available for tests.
    /// See [Self::process_request_batches].
    pub async fn pub_process_request_batches(
        &self,
        batch_digests: Vec<BlockHash>,
        max_response_size: usize,
    ) -> WorkerNetworkResult<Vec<Batch>> {
        self.process_request_batches(batch_digests, max_response_size).await
    }
}
