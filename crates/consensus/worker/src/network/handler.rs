//! The type that handles core logic for requests between workers.
use super::{
    error::{WorkerNetworkError, WorkerNetworkResult},
    handle::WorkerNetworkHandle,
    message::WorkerGossip,
};
use crate::{
    batch_fetcher::get_batch_local_cache,
    metrics::WorkerMetrics,
    network::{stream_codec, PendingBatchStream},
};
use futures::AsyncWriteExt as _;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    write_frame, GossipMessage, Stream, SyncFrame, SyncFrameError, WorkerSyncRequest,
};
use tn_network_types::{WorkerOthersBatchMessage, WorkerToPrimaryClient};
use tn_storage::{consensus::ConsensusChain, tables::NodeBatchesCache};
use tn_types::{
    ensure, now, try_decode, BatchValidation, BlsPublicKey, Database, Epoch, SealedBatch, WorkerId,
    B256,
};
use tracing::{debug, warn};

/// Total timeout for sending all batches over a stream.
/// Prevents slow-reader attacks where a peer accepts a stream but never reads.
/// Set for 500MB through emerging market worse-case 20MB/s upload.
///
/// Exposed to the requester side (`handle.rs`), which sizes its per-chunk-count
/// read tolerance (`INTER_CHUNK_STREAM_TIMEOUT`) to be at least this whole-stream
/// cap so an honest sender is never disconnected mid-transfer.
pub(crate) const SEND_STREAM_TIMEOUT: Duration = Duration::from_secs(200);

/// The type that handles requests from peers.
///
/// An instance is cloned for each request and used in a spawned task.
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
    /// Prometheus metrics for peer batch handling.
    metrics: WorkerMetrics,
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
        let metrics = WorkerMetrics::new_for_worker(id);
        Self { id, validator, consensus_config, network_handle, metrics }
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
            WorkerGossip::Batch(epoch, batch_hash) => {
                ensure!(
                    topic.to_string().eq(&tn_config::LibP2pConfig::worker_batch_topic(
                        self.consensus_config.chain_id()
                    )),
                    WorkerNetworkError::InvalidTopic
                );
                let my_epoch = self.consensus_config.epoch();
                // We are probably behind.  Do not bother to fetch and store this Batch now, it will
                // most likely be removed before we can use it and will be fetched
                // later when needed.
                ensure!(my_epoch == epoch, WorkerNetworkError::BatchEpochMismatch(epoch, my_epoch));
                // Retrieve the batch...
                let store = self.consensus_config.node_storage();
                // Since we are precaching Batches for the current epoch we only need to check if it
                // is in the local cache. There should not have been an opertunity
                // for it to be in the consensus chain yet.
                if !matches!(
                    get_batch_local_cache(batch_hash, self.consensus_config.node_storage(),),
                    Ok(Some(_))
                ) {
                    // If batch is missing from db, then request from peer.
                    // If we are a CVV then we should already have it.
                    // This allows non-CVVs to pre fetch batches they will soon need.
                    let mut missing = BTreeSet::from([batch_hash]);
                    match self.network_handle.request_batches(&mut missing).await {
                        Ok(batches) => {
                            if let Some((digest, batch)) = batches.first() {
                                // Storing batches for future epochs can cause problems.  This might
                                // open an attack for rogue
                                // validator to fill disk space, the cache is cleared on
                                // epoch boundaries anyway, etc.
                                // Note: retrieving this batch for no reason is wasteful, it should
                                // only effect nodes catching up old epochs though...
                                if batch.epoch == self.consensus_config.epoch() {
                                    store.insert::<NodeBatchesCache>(digest, batch).map_err(
                                        |e| {
                                            WorkerNetworkError::Internal(format!(
                                                "failed to write to batch store: {e}"
                                            ))
                                        },
                                    )?;
                                } else {
                                    debug!(
                                        target: "worker:network",
                                        batch_epoch = batch.epoch,
                                        current_epoch = self.consensus_config.epoch(),
                                        "gossipped batch epoch mismatch - discarding"
                                    );
                                }
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
                    topic.to_string().eq(&tn_config::LibP2pConfig::worker_txn_topic(
                        self.consensus_config.chain_id()
                    )),
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
        self.validator
            .validate_batch(sealed_batch.clone())
            .inspect_err(|_| self.metrics.record_batch_validation_failure())?;

        let (mut batch, digest) = sealed_batch.split();

        // Set received_at timestamp for remote batch.
        batch.set_received_at(now());
        store.insert::<NodeBatchesCache>(&digest, &batch).map_err(|e| {
            WorkerNetworkError::Internal(format!("failed to write to batch store: {e}"))
        })?;

        // notify primary for payload store
        client
            .report_others_batch(WorkerOthersBatchMessage { digest, worker_id: self.id })
            .await
            .map_err(|e| WorkerNetworkError::Internal(e.to_string()))?;

        Ok(())
    }

    /// Process request to open stream for batches.
    pub(super) async fn process_request_batches_stream(
        &self,
        peer: BlsPublicKey,
        pending_request: Option<PendingBatchStream>,
        mut stream: Stream,
        request_digest: B256,
        consensus_chain: &ConsensusChain,
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

        // set timeout to prevent slow-read attack
        match tokio::time::timeout(
            SEND_STREAM_TIMEOUT,
            stream_codec::send_batches_over_stream(
                &mut stream,
                store,
                consensus_chain,
                &request.batch_digests,
                request.epoch,
            ),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(target: "worker::network", %peer, ?e, "failed to send batches over stream");
            }
            Err(_elapsed) => {
                warn!(target: "worker::network", %peer, ?request_digest, "sending batches stream timed out");
            }
        }

        // attempt to close the stream gracefully, bounded so a peer that stops
        // reading cannot pin this legacy responder task (and the admission permit
        // it holds via `PendingBatchStream`) on the FIN flush. Mirrors the bounded
        // close in `process_sync_batches_stream` and the trailing writes in `mod.rs`;
        // every best-effort trailing op on this task is time-bounded.
        let _ =
            tokio::time::timeout(crate::network::SYNC_REQUEST_READ_TIMEOUT, stream.close()).await;

        Ok(())
    }

    /// Serve an admitted inbound sync batch exchange.
    ///
    /// The opening [`SyncFrame::Req`] has already been read and validated by the
    /// caller, and the exchange has been admitted against the concurrency caps.
    /// This writes the [`SyncFrame::Ack`], streams the requested batches as
    /// [`SyncFrame::Data`] frames, and ends the stream with [`SyncFrame::End`].
    ///
    /// A send failure or storage error is logged and best-effort signalled to the
    /// requester with a [`SyncFrame::Err`] so it stops waiting; it is not a peer
    /// fault, so no penalty is returned. Like the legacy responder, the sync
    /// responder reports errors metrics-only during the item-5 rollout.
    pub(super) async fn process_sync_batches_stream(
        &self,
        peer: BlsPublicKey,
        mut stream: Stream,
        batch_digests: BTreeSet<B256>,
        epoch: Epoch,
        consensus_chain: &ConsensusChain,
    ) -> WorkerNetworkResult<()> {
        debug!(
            target: "worker::network",
            %peer,
            batch_count = batch_digests.len(),
            epoch,
            "serving inbound sync batch stream"
        );

        let store = self.consensus_config.node_storage();
        let max_frame = crate::network::handle::max_sync_frame_size(epoch);

        // set timeout to prevent slow-read attack; flatten the timeout's outer
        // `Result` into the send's so a single error path handles both
        let served = tokio::time::timeout(
            SEND_STREAM_TIMEOUT,
            stream_codec::send_sync_batches_over_stream(
                &mut stream,
                store,
                consensus_chain,
                &batch_digests,
                epoch,
                max_frame,
            ),
        )
        .await
        .map_err(WorkerNetworkError::from)
        .and_then(|sent| sent);

        // a send failure or timeout is logged and best-effort signalled so the
        // requester stops waiting; it is not a peer fault, so no penalty
        if let Err(e) = served {
            warn!(target: "worker::network", %peer, ?e, "failed to serve sync batch stream");
            // bound the best-effort error write: a peer that passed admission, read
            // the `Ack`, then stopped reading would otherwise pin this responder
            // task on an unbounded write. Mirrors the shed/malformed error writes in
            // `mod.rs`, which the same constant bounds.
            let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
            let _ = tokio::time::timeout(crate::network::SYNC_REQUEST_READ_TIMEOUT, async {
                let _ = write_frame(
                    &mut stream,
                    &SyncFrame::<WorkerSyncRequest>::Err(SyncFrameError::Internal),
                    &mut encode_buffer,
                    &mut compressed_buffer,
                    max_frame,
                )
                .await;
            })
            .await;
        }

        // attempt to close the stream gracefully, bounded so a peer that stops
        // reading cannot pin this responder task (and the admission slot it holds)
        // on the FIN flush: like the shed/malformed paths in `mod.rs`, every
        // best-effort trailing op on this task is time-bounded.
        let _ =
            tokio::time::timeout(crate::network::SYNC_REQUEST_READ_TIMEOUT, stream.close()).await;

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
        stream: Stream,
        pending_request: Option<PendingBatchStream>,
        request_digest: B256,
        consensus_chain: &ConsensusChain,
    ) -> WorkerNetworkResult<()> {
        self.process_request_batches_stream(
            peer,
            pending_request,
            stream,
            request_digest,
            consensus_chain,
        )
        .await
    }
}
