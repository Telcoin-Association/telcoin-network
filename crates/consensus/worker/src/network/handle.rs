//! The worker's handle to the network layer abstraction.
//!
//! The network handle provides compatibility methods for the
//! worker to interact with `ConsensusNetwork` within the worker's
//! context.

use std::{collections::HashSet, time::Duration};

use futures::{AsyncRead, AsyncWriteExt as _};
use tn_network_libp2p::{
    error::NetworkError,
    types::{NetworkHandle, NetworkResponseMessage, NetworkResult},
    Penalty,
};
use tn_types::{
    encode, max_batch_size, Batch, BlockHash, BlsPublicKey, Epoch, SealedBatch, TaskSpawner, B256,
};
use tracing::{debug, warn};

use crate::{
    network::{stream_codec, Req, Res, MAX_BATCH_DIGESTS_PER_REQUEST},
    WorkerGossip, WorkerRPCError, WorkerRequest, WorkerResponse,
};

/// Timeout for streaming a single batch from peer. Batches capped at 1MB.
const BATCH_STREAM_TIMEOUT: Duration = Duration::from_secs(5);

/// Maximum number of retries through the full peer list in `request_batches()`.
const MAX_BATCH_REQUEST_RETRIES: usize = 3;

/// Delay between retry attempts in `request_batches()` to give semaphores time to release.
const BATCH_REQUEST_RETRY_DELAY: Duration = Duration::from_millis(500);

/// The wrapper around worker-specific network calls.
#[derive(Clone, Debug)]
pub struct WorkerNetworkHandle {
    /// The handle to the node's network.
    handle: NetworkHandle<Req, Res>,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
    /// The current epoch for this node.
    epoch: Epoch,
}

impl WorkerNetworkHandle {
    /// Create a new instance of [Self].
    pub fn new(handle: NetworkHandle<Req, Res>, task_spawner: TaskSpawner, epoch: Epoch) -> Self {
        Self { handle, task_spawner, epoch }
    }

    /// Return a reference to the task spawner.
    pub fn get_task_spawner(&self) -> &TaskSpawner {
        &self.task_spawner
    }

    /// Return a reference to the inner handle.
    pub fn inner_handle(&self) -> &NetworkHandle<Req, Res> {
        &self.handle
    }

    /// Publish a batch digest to the worker network.
    pub(crate) async fn publish_batch(&self, batch_digest: BlockHash) -> NetworkResult<()> {
        let data = encode(&WorkerGossip::Batch(batch_digest));
        self.handle.publish(tn_config::LibP2pConfig::worker_batch_topic(), data).await?;
        Ok(())
    }

    /// Publish a transaction (as raw bytes) worker network.
    /// Do this when not a committee member so a CVV can include the txn.
    pub(crate) async fn publish_txn(&self, txn: Vec<u8>) -> NetworkResult<()> {
        let data = encode(&WorkerGossip::Txn(txn));
        self.handle.publish("tn-txn".into(), data).await?;
        Ok(())
    }

    /// Report a new batch to a peer.
    pub(crate) async fn report_batch(
        &self,
        peer_bls: BlsPublicKey,
        sealed_batch: SealedBatch,
    ) -> NetworkResult<()> {
        let request = WorkerRequest::ReportBatch { sealed_batch };
        let res = self.handle.send_request(request, peer_bls).await?;
        let res = res.await??.result;
        match res {
            WorkerResponse::ReportBatch => Ok(()),
            WorkerResponse::RequestBatchesStream { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a report batch is stream ack!".to_string(),
            )),
            WorkerResponse::PeerExchange { .. } => Err(NetworkError::RPCError(
                "Got wrong response, not a report batch is peer exchange!".to_string(),
            )),
            WorkerResponse::Error(WorkerRPCError(s)) => Err(NetworkError::RPCError(s)),
        }
    }

    /// Request a group of batches by hashes using stream-based transfer.
    ///
    /// Tries peers one at a time until all batches are received or all peers fail.
    /// Retries up to [MAX_BATCH_REQUEST_RETRIES] times through the full peer list
    /// with [BATCH_REQUEST_RETRY_DELAY] between attempts, giving semaphores time
    /// to release. Returns `Ok` if any batches successfully fetched from peers.
    pub(crate) async fn request_batches(
        &self,
        requested_digests: &mut HashSet<BlockHash>,
    ) -> NetworkResult<Vec<(BlockHash, Batch)>> {
        let mut all_batches = Vec::with_capacity(requested_digests.len());

        for attempt in 0..MAX_BATCH_REQUEST_RETRIES {
            // re-fetch peers each attempt to pick up newly connected peers
            let peers = self.handle.connected_peers().await?;
            if peers.is_empty() {
                return Ok(all_batches);
            }

            // try peers one at a time with fallback
            for peer in peers {
                // check remaining digests before sending request
                if requested_digests.is_empty() {
                    break;
                }

                // cap digests for this peer and send remainder to subsequent peers
                // this should never happen
                let peer_batch;
                let digests_for_peer = if requested_digests.len() > MAX_BATCH_DIGESTS_PER_REQUEST {
                    warn!(
                        target: "worker::network",
                        total = requested_digests.len(),
                        max = MAX_BATCH_DIGESTS_PER_REQUEST,
                        "truncating oversized digest set for peer request"
                    );
                    peer_batch = requested_digests
                        .iter()
                        .copied()
                        .take(MAX_BATCH_DIGESTS_PER_REQUEST)
                        .collect();
                    &peer_batch
                } else {
                    &*requested_digests
                };

                // loop: update remaining batches or log error
                match self.request_batches_from_peer(peer, digests_for_peer).await {
                    Ok(batches) => {
                        for (digest, batch) in batches {
                            if requested_digests.remove(&digest) {
                                all_batches.push((digest, batch));
                            }
                        }
                        debug!(
                            target: "worker::network",
                            %peer,
                            received = all_batches.len(),
                            remaining = requested_digests.len(),
                            "received batches from peer"
                        );
                    }
                    Err(e) => {
                        warn!(
                            target: "worker::network",
                            %peer,
                            ?e,
                            "failed to fetch batches from peer, trying next"
                        );
                    }
                }
            }

            // return immediately if any batches fetched (partial success) or all done
            if !all_batches.is_empty() || requested_digests.is_empty() {
                return Ok(all_batches);
            }

            // sleep before retrying (skip sleep on last attempt)
            if attempt + 1 < MAX_BATCH_REQUEST_RETRIES {
                debug!(
                    target: "worker::network",
                    attempt = attempt + 1,
                    max = MAX_BATCH_REQUEST_RETRIES,
                    "all peers rejected, retrying after delay"
                );
                tokio::time::sleep(BATCH_REQUEST_RETRY_DELAY).await;
            }
        }

        // all retries exhausted
        if all_batches.is_empty() && !requested_digests.is_empty() {
            warn!(
                target: "worker::network",
                missing = requested_digests.len(),
                retries = MAX_BATCH_REQUEST_RETRIES,
                "request batches from all peers exhausted retries, still missing digests"
            );
            Err(NetworkError::RPCError("Unable to get batches from any peers!".to_string()))
        } else {
            Ok(all_batches)
        }
    }

    /// Request batches from a single peer via stream.
    ///
    /// This method:
    /// 1. Sends a `RequestBatchesStream` request to negotiate
    /// 2. If accepted, opens a stream with the request digest for correlation
    /// 3. Reads and validates batches from the stream in real-time
    async fn request_batches_from_peer(
        &self,
        peer: BlsPublicKey,
        batch_digests: &HashSet<BlockHash>,
    ) -> NetworkResult<Vec<(BlockHash, Batch)>> {
        // sanity check - should never happen
        if batch_digests.is_empty() {
            warn!(target: "worker::network", "requested empty batches from peer!");
            return Ok(vec![]);
        }

        // send request to negotiate stream
        let request = WorkerRequest::RequestBatchesStream {
            batch_digests: batch_digests.clone(),
            epoch: self.epoch(),
        };
        let request_digest = self.generate_batch_request_id(batch_digests);

        // send request and await response from peer
        //
        // SAFETY: network layer handles request timeout
        let NetworkResponseMessage { peer: _, result: res } =
            self.handle.send_request(request, peer).await?.await??;
        match res {
            WorkerResponse::RequestBatchesStream { ack } => {
                // return error if denied to try next peer
                if !ack {
                    return Err(NetworkError::RPCError(
                        "Peer {peer:%} denied request to sync".to_string(),
                    ));
                }

                debug!(
                    target: "worker::network",
                    %peer,
                    ?ack,
                    "peer ack for stream request"
                );

                // open raw stream then write request_digest for correlation
                let mut stream = self.handle.open_stream(peer).await??;
                stream.write_all(request_digest.as_slice()).await.map_err(|e| {
                    NetworkError::RPCError(format!("failed to write request digest: {e}"))
                })?;
                stream.flush().await.map_err(|e| {
                    NetworkError::RPCError(format!("failed to flush request digest: {e}"))
                })?;

                debug!(
                    target: "worker::network",
                    %peer,
                    "stream opened - reading and validating batches..."
                );

                // read and validate batches from stream with timeout per batch
                let batches =
                    self.read_and_validate_batches_with_timeout(&mut stream, batch_digests).await?;

                Ok(batches)
            }
            WorkerResponse::ReportBatch => Err(NetworkError::RPCError(
                "Got wrong response: report batch instead of stream ack".to_string(),
            )),
            WorkerResponse::PeerExchange { .. } => Err(NetworkError::RPCError(
                "Got wrong response: peer exchange instead of stream ack".to_string(),
            )),
            WorkerResponse::Error(WorkerRPCError(s)) => Err(NetworkError::RPCError(s)),
        }
    }

    /// Read and validate batches from a stream.
    ///
    /// Validates each batch in real-time:
    /// - Checks batch count matches expected
    /// - Verifies each batch digest was requested
    /// - Detects duplicate batches
    ///
    /// The sender chunks batch digests into groups of 200 and writes
    /// `[chunk_count][batches...][flush]` per chunk. This method loops
    /// reading chunks until the stream closes.
    ///
    /// SAFETY: this method times out if a batch fails to stream within time limit. If this
    /// happens, the entire stream is shut down.
    pub(crate) async fn read_and_validate_batches_with_timeout<S: AsyncRead + Unpin + Send>(
        &self,
        stream: &mut S,
        requested_digests: &HashSet<BlockHash>,
    ) -> NetworkResult<Vec<(BlockHash, Batch)>> {
        // allocate reusable buffers
        //
        // SAFETY: num of requests capped by `MAX_CONCURRENT_BATCH_STREAMS`
        let max_size = max_batch_size(self.epoch);
        let mut decode_buffer = Vec::with_capacity(max_size);
        let mut compressed_buffer = Vec::with_capacity(snap::raw::max_compress_len(max_size));

        // SAFETY: requested_digests is capped to MAX_BATCH_DIGESTS_PER_REQUEST (500) above.
        // Worst-case allocation: 500 entries of (B256, Batch) where each batch is up to 1MB.
        // This bounds memory to ~500MB rather than the theoretical ~33GB from 33k digests.
        let mut batches = Vec::with_capacity(requested_digests.len());
        let mut received_digests = HashSet::with_capacity(requested_digests.len());

        // read chunks until stream closes (sender writes multiple chunks for large requests)
        loop {
            // try to read next chunk count — StreamClosed means transfer complete
            let batch_chunk_count = match stream_codec::read_chunk_count(stream).await {
                Ok(count) => count as usize,
                Err(super::error::WorkerNetworkError::StreamClosed) => break,
                Err(e) => {
                    return Err(NetworkError::RPCError(format!("Failed to read batch count: {e}")))
                }
            };

            // validate running total doesn't exceed requested
            if batches.len() + batch_chunk_count > requested_digests.len() {
                return Err(NetworkError::ProtocolError(format!(
                    "Peer sent too many batches: expected {}, received {}",
                    requested_digests.len(),
                    batches.len() + batch_chunk_count
                )));
            }

            // validate each batch as it arrives - immediately return error for malformed batches
            //
            // SAFETY: ensure timeout for stream per batch (disconnect if no progress made)
            for i in 0..batch_chunk_count {
                let batch = tokio::time::timeout(
                    BATCH_STREAM_TIMEOUT,
                    stream_codec::read_batch(
                        stream,
                        &mut decode_buffer,
                        &mut compressed_buffer,
                        self.epoch,
                    ),
                )
                .await
                .map_err(|_| {
                    warn!(target: "worker::network", "timeout streaming batch");
                    NetworkError::Timeout
                })?
                .map_err(|e| {
                    warn!(target: "worker::network", ?e, "error reading batch from stream");
                    NetworkError::RPCError(format!("Failed to read batch {}: {e}", i))
                })?;

                let batch_digest = batch.digest();

                // validate batch was requested
                if !requested_digests.contains(&batch_digest) {
                    return Err(NetworkError::ProtocolError(format!(
                        "Peer sent unexpected batch with digest {batch_digest}"
                    )));
                }

                // validate batch is unique (no duplicates)
                if !received_digests.insert(batch_digest) {
                    return Err(NetworkError::ProtocolError(format!(
                        "Peer sent duplicate batch with digest {batch_digest}"
                    )));
                }

                batches.push((batch_digest, batch));
            }
        }

        Ok(batches)
    }

    /// Report penalty to peer manager.
    pub(super) async fn report_penalty(&self, peer: BlsPublicKey, penalty: Penalty) {
        self.handle.report_penalty(peer, penalty).await;
    }

    /// Retrieve the count of connected peers.
    pub async fn connected_peers_count(&self) -> NetworkResult<usize> {
        self.handle.connected_peer_count().await
    }

    /// Update the task spawner at the epoch boundary.
    pub fn update_task_spawner(&mut self, task_spawner: TaskSpawner) {
        self.task_spawner = task_spawner
    }

    /// Return an copy of the node's current epoch.
    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    /// Update the current epoch.
    pub fn update_epoch(&mut self, epoch: Epoch) {
        self.epoch = epoch;
    }

    /// Helper method to digest missing batch request before initiating stream.
    ///
    /// The digest is used to detect duplicate requests from peers.
    pub(crate) fn generate_batch_request_id(&self, batch_digests: &HashSet<BlockHash>) -> B256 {
        let mut hasher = tn_types::DefaultHashFunction::new();
        let bytes = encode(batch_digests);
        hasher.update(&bytes);
        B256::from_slice(hasher.finalize().as_bytes())
    }
}

// support IT tests
#[cfg(any(test, feature = "test-utils"))]
impl WorkerNetworkHandle {
    /// Convenience method for creating a new Self for tests- sends events no-where and does
    /// nothing.
    pub fn new_for_test(task_spawner: TaskSpawner) -> Self {
        let (tx, _rx) = tokio::sync::mpsc::channel(5);
        Self { handle: NetworkHandle::new(tx), task_spawner, epoch: 0 }
    }

    /// Publicly available for tests.
    /// See [Self::request_batches].
    pub async fn pub_request_batches(
        &self,
        requested_digests: &mut HashSet<BlockHash>,
    ) -> NetworkResult<Vec<(BlockHash, Batch)>> {
        self.request_batches(requested_digests).await
    }

    /// Publicly available for tests.
    /// See [Self::generate_batch_request_id].
    pub fn pub_generate_batch_request_id(&self, batch_digests: &HashSet<BlockHash>) -> B256 {
        self.generate_batch_request_id(batch_digests)
    }
}
