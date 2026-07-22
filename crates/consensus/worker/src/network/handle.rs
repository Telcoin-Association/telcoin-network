//! The worker's handle to the network layer abstraction.
//!
//! The network handle provides compatibility methods for the
//! worker to interact with `ConsensusNetwork` within the worker's
//! context.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use futures::{AsyncRead, AsyncWriteExt as _};
use parking_lot::Mutex;
use tn_network_libp2p::{
    error::NetworkError,
    read_frame,
    types::{NetworkHandle, NetworkResult},
    write_frame, Penalty, StreamError, SyncFrame, WorkerSyncRequest,
};
use tn_types::{
    encode, max_batch_size, try_decode, Batch, BlockHash, BlsPublicKey, Epoch, RpcInfo,
    SealedBatch, TaskSpawner,
};
use tracing::{debug, warn};

use crate::{
    network::{Req, Res, MAX_BATCH_DIGESTS_PER_REQUEST},
    WorkerGossip, WorkerRPCError, WorkerRequest, WorkerResponse,
};

/// Timeout for reading a single sync data frame from a peer. Batches capped at 1MB.
const BATCH_STREAM_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for the responder's first sync frame (`Ack`/`Deny`) after the request
/// frame is written. A peer that negotiated the sync protocol but does not answer
/// (e.g. an item-4 node that registered the protocol but never sends `Ack`) trips
/// this, so the requester stops waiting and tries the next peer.
const SYNC_ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Headroom added to `max_batch_size` for the sync-frame envelope (the `SyncFrame`
/// enum tag and the `Data` length prefix) when bounding a decoded frame.
const SYNC_FRAME_OVERHEAD: usize = 1024;

/// Maximum number of retries through the full peer list in `request_batches()`.
const MAX_BATCH_REQUEST_RETRIES: usize = 3;

/// Delay between retry attempts in `request_batches()` to give semaphores time to release.
const BATCH_REQUEST_RETRY_DELAY: Duration = Duration::from_millis(500);

/// The largest sync frame accepted for `epoch`: a `Data` frame carrying one
/// encoded batch, plus envelope headroom.
pub(crate) fn max_sync_frame_size(epoch: Epoch) -> usize {
    max_batch_size(epoch).saturating_add(SYNC_FRAME_OVERHEAD)
}

/// The outcome of a sync-protocol batch fetch attempt.
enum SyncAttempt {
    /// The peer served the requested batches over the sync protocol.
    Fetched(Vec<(BlockHash, Batch)>),
    /// The peer did not answer the sync exchange; skip it (no legacy fallback).
    Unsupported,
    /// The peer answered but the exchange failed; try the next peer.
    Failed(NetworkError),
}

/// The wrapper around worker-specific network calls.
#[derive(Clone, Debug)]
pub struct WorkerNetworkHandle {
    /// The handle to the node's network.
    handle: NetworkHandle<Req, Res>,
    /// The type to spawn tasks.
    task_spawner: TaskSpawner,
    /// The current epoch for this node.
    epoch: Epoch,
    /// Per-peer sync-protocol capability, learned by probing.
    ///
    /// Absent means not yet probed (try the sync protocol); `false` means the
    /// peer did not answer a sync open (a pre-item-4 peer that fails negotiation,
    /// or an item-4 peer that registered the protocol but does not serve it), so
    /// it is skipped this epoch (there is no legacy fallback); `true` means the
    /// peer served a sync exchange. The cache is reset each epoch
    /// ([`Self::update_epoch`]) so a peer upgraded over the rotation boundary is
    /// re-probed.
    sync_capability: Arc<Mutex<HashMap<BlsPublicKey, bool>>>,
    /// The genesis chain id, used to namespace gossip topics this handle publishes.
    chain_id: u64,
}

impl WorkerNetworkHandle {
    /// Create a new instance of [Self].
    pub fn new(
        handle: NetworkHandle<Req, Res>,
        task_spawner: TaskSpawner,
        epoch: Epoch,
        chain_id: u64,
    ) -> Self {
        Self {
            handle,
            task_spawner,
            epoch,
            chain_id,
            sync_capability: Arc::new(Mutex::new(HashMap::new())),
        }
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
        let data = encode(&WorkerGossip::Batch(self.epoch, batch_digest));
        self.handle
            .publish(tn_config::LibP2pConfig::worker_batch_topic(self.chain_id), data)
            .await?;
        Ok(())
    }

    /// Snapshot of every committee validator's advertised JSON-RPC endpoint, discovered over
    /// kademlia.
    ///
    /// A non-committee ("observer") worker uses this to forward the transactions it accepts to
    /// the validators that own them (issue #804), instead of pushing them over the worker
    /// protocol. Returns an empty list if no validator has advertised an endpoint.
    pub(crate) async fn get_all_validator_rpcs(
        &self,
    ) -> NetworkResult<Vec<(BlsPublicKey, RpcInfo)>> {
        self.handle.get_all_validator_rpcs().await
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
            WorkerResponse::RecoverableError(WorkerRPCError(s)) => {
                Err(NetworkError::RPCRetryable(s))
            }
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
        requested_digests: &mut BTreeSet<BlockHash>,
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

    /// Request batches from a single peer over the typed sync protocol.
    ///
    /// Opens a `/tn-worker-{id}-sync` exchange that folds the request into the
    /// opening stream frame. There is no legacy fallback (#739, step 9c): a peer
    /// that does not answer the sync protocol (negotiation fails, or it negotiated
    /// but never `Ack`ed) is cached unsyncable this epoch and skipped, and the
    /// caller tries the next peer. The sync probe is penalty-exempt, so skipping
    /// never lowers the peer's score.
    async fn request_batches_from_peer(
        &self,
        peer: BlsPublicKey,
        batch_digests: &BTreeSet<BlockHash>,
    ) -> NetworkResult<Vec<(BlockHash, Batch)>> {
        // sanity check - should never happen
        if batch_digests.is_empty() {
            warn!(target: "worker::network", "requested empty batches from peer!");
            return Ok(vec![]);
        }

        // a peer known not to serve sync this epoch is skipped so the caller tries
        // the next peer
        if self.sync_capability.lock().get(&peer) == Some(&false) {
            return Err(NetworkError::RPCRetryable(format!(
                "peer {peer} does not serve the batch sync protocol this epoch"
            )));
        }

        match self.request_batches_from_peer_sync(peer, batch_digests).await {
            SyncAttempt::Fetched(batches) => {
                self.sync_capability.lock().insert(peer, true);
                Ok(batches)
            }
            SyncAttempt::Unsupported => {
                // no legacy fallback: cache the peer unsyncable this epoch and let
                // the caller try the next peer
                self.sync_capability.lock().insert(peer, false);
                debug!(
                    target: "worker::network",
                    %peer,
                    "peer does not answer the batch sync protocol, skipping"
                );
                Err(NetworkError::RPCRetryable(format!(
                    "peer {peer} does not serve the batch sync protocol"
                )))
            }
            SyncAttempt::Failed(e) => {
                // the peer speaks sync but this exchange failed; try the next peer
                self.sync_capability.lock().insert(peer, true);
                Err(e)
            }
        }
    }

    /// Attempt a batch fetch over the typed sync protocol.
    ///
    /// Opens a `/tn-worker-{id}-sync` stream, writes the request in the opening
    /// [`SyncFrame::Req`] frame, and reads the response: an [`SyncFrame::Ack`]
    /// followed by [`SyncFrame::Data`] frames terminated by [`SyncFrame::End`].
    /// Returns [`SyncAttempt::Unsupported`] when the peer does not answer (so the
    /// caller skips it), [`SyncAttempt::Fetched`] on success, or
    /// [`SyncAttempt::Failed`] when the peer answered but the exchange failed.
    async fn request_batches_from_peer_sync(
        &self,
        peer: BlsPublicKey,
        batch_digests: &BTreeSet<BlockHash>,
    ) -> SyncAttempt {
        self.try_sync_batch_exchange(peer, batch_digests)
            .await
            .map_or_else(|attempt| attempt, SyncAttempt::Fetched)
    }

    /// Run one sync exchange, yielding the fetched batches or the classified
    /// non-fetch outcome.
    ///
    /// `Err(SyncAttempt::Unsupported)` means the peer did not answer the protocol
    /// (negotiation failed, or it negotiated but never sent `Ack`), so the caller
    /// skips it (there is no legacy fallback). `Err(SyncAttempt::Failed(_))` means
    /// a transient or exchange-level error once the peer has proved sync-capable, so
    /// the caller keeps it sync-capable and tries the next peer. A transport I/O
    /// error during the open (`UpgradeIo`) is transient rather than a protocol
    /// mismatch, so it maps to `Failed` instead of poisoning the capability cache.
    async fn try_sync_batch_exchange(
        &self,
        peer: BlsPublicKey,
        batch_digests: &BTreeSet<BlockHash>,
    ) -> Result<Vec<(BlockHash, Batch)>, SyncAttempt> {
        // open the sync stream, flattening the command-channel and stream-open
        // results. Only a genuine negotiation failure (`UpgradeFailed`) is a
        // pre-item-4 peer that does not advertise the protocol -> Unsupported
        // (penalty-exempt, cached unsyncable this epoch). A transient upgrade I/O
        // error or any other open error is not proof the peer lacks sync -> try
        // next peer.
        let mut stream =
            self.handle.open_stream(peer).await.and_then(|s| s).map_err(|e| match () {
                () if matches!(e, NetworkError::Stream(StreamError::UpgradeFailed)) => {
                    SyncAttempt::Unsupported
                }
                () => SyncAttempt::Failed(e),
            })?;

        // write the request in the opening frame. Negotiation already succeeded,
        // so the peer is sync-capable; a write failure is transient -> try next.
        let max_frame = max_sync_frame_size(self.epoch());
        let request = SyncFrame::Req(WorkerSyncRequest::Batches {
            batch_digests: batch_digests.clone(),
            epoch: self.epoch(),
        });
        let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        write_frame(&mut stream, &request, &mut encode_buffer, &mut compressed_buffer, max_frame)
            .await
            .and(stream.flush().await)
            .map_err(|e| {
                SyncAttempt::Failed(NetworkError::RPCRetryable(format!(
                    "failed to write sync request frame: {e}"
                )))
            })?;

        // read the responder's first frame. Negotiation already succeeded, so the
        // peer IS sync-capable: a missing `Ack` (timeout) or any read error is a
        // transient exchange failure, not a capability signal. Both map to `Failed`
        // (try the next peer, keep the peer sync-capable) rather than `Unsupported`,
        // which would cache the peer unsyncable for the whole epoch with no fallback
        // (#739, step 9c removed the legacy path). Only a negotiation failure at open
        // time (`UpgradeFailed`, above) proves the peer lacks the sync protocol.
        let (mut decode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        let first = tokio::time::timeout(
            SYNC_ACK_TIMEOUT,
            read_frame::<_, WorkerSyncRequest>(
                &mut stream,
                &mut decode_buffer,
                &mut compressed_buffer,
                max_frame,
            ),
        )
        .await
        .map_err(|_elapsed| {
            SyncAttempt::Failed(NetworkError::RPCRetryable(
                "timed out reading sync ack frame".to_string(),
            ))
        })?
        .map_err(|e| {
            SyncAttempt::Failed(NetworkError::RPCRetryable(format!(
                "failed to read sync ack frame: {e}"
            )))
        })?;

        match first {
            SyncFrame::Ack => self
                .read_sync_batches(&mut stream, batch_digests)
                .await
                .map_err(SyncAttempt::Failed),
            // sync-capable, but shedding load or lacking the data: try next peer
            SyncFrame::Deny(reason) => Err(SyncAttempt::Failed(NetworkError::RPCRetryable(
                format!("peer denied sync batch request: {reason:?}"),
            ))),
            SyncFrame::Err(err) => Err(SyncAttempt::Failed(NetworkError::RPCError(format!(
                "peer aborted sync batch exchange: {err:?}"
            )))),
            // a well-behaved responder never opens with these
            SyncFrame::Req(_) | SyncFrame::Data(_) | SyncFrame::End => Err(SyncAttempt::Failed(
                NetworkError::ProtocolError("unexpected opening sync frame from peer".to_string()),
            )),
        }
    }

    /// Read `Data` frames terminated by `End` from an accepted sync exchange,
    /// decoding and validating each batch as it arrives.
    ///
    /// Validates every batch: it must have been requested, none may repeat, and
    /// the running total may not exceed the request. Each frame read is bounded by
    /// [`BATCH_STREAM_TIMEOUT`].
    pub(crate) async fn read_sync_batches<S: AsyncRead + Unpin + Send>(
        &self,
        stream: &mut S,
        requested_digests: &BTreeSet<BlockHash>,
    ) -> NetworkResult<Vec<(BlockHash, Batch)>> {
        let max_frame = max_sync_frame_size(self.epoch());
        let (mut decode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        let mut batches = Vec::with_capacity(requested_digests.len());
        let mut received_digests = HashSet::with_capacity(requested_digests.len());

        loop {
            let frame = tokio::time::timeout(
                BATCH_STREAM_TIMEOUT,
                read_frame::<_, WorkerSyncRequest>(
                    stream,
                    &mut decode_buffer,
                    &mut compressed_buffer,
                    max_frame,
                ),
            )
            .await
            .map_err(|_| {
                warn!(target: "worker::network", "timeout reading sync batch frame");
                NetworkError::Timeout
            })?
            .map_err(|e| NetworkError::RPCError(format!("failed to read sync frame: {e}")))?;

            match frame {
                SyncFrame::Data(bytes) => {
                    // running total must not exceed the request
                    if batches.len() >= requested_digests.len() {
                        return Err(NetworkError::ProtocolError(format!(
                            "Peer sent too many batches: expected {}",
                            requested_digests.len()
                        )));
                    }
                    let batch: Batch = try_decode(&bytes).map_err(|e| {
                        NetworkError::ProtocolError(format!("failed to decode sync batch: {e}"))
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
                SyncFrame::End => break,
                SyncFrame::Err(err) => {
                    return Err(NetworkError::RPCError(format!(
                        "peer aborted sync batch stream: {err:?}"
                    )))
                }
                // a well-behaved responder never sends these mid-stream
                SyncFrame::Ack | SyncFrame::Deny(_) | SyncFrame::Req(_) => {
                    return Err(NetworkError::ProtocolError(
                        "unexpected sync frame during batch stream".to_string(),
                    ))
                }
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
    ///
    /// Also clears the per-peer sync capability cache: committees rotate at the
    /// boundary and binaries are upgraded there, so a peer that could only speak
    /// legacy last epoch is re-probed for the sync protocol this epoch.
    pub fn update_epoch(&mut self, epoch: Epoch) {
        self.epoch = epoch;
        self.sync_capability.lock().clear();
    }
}

// support IT tests
#[cfg(any(test, feature = "test-utils"))]
impl WorkerNetworkHandle {
    /// Convenience method for creating a new Self for tests- sends events no-where and does
    /// nothing.
    pub fn new_for_test(task_spawner: TaskSpawner) -> Self {
        let (tx, _rx) = tokio::sync::mpsc::channel(5);
        Self {
            handle: NetworkHandle::new(tx),
            task_spawner,
            epoch: 0,
            chain_id: 0,
            sync_capability: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Publicly available for tests.
    /// See [Self::request_batches].
    pub async fn pub_request_batches(
        &self,
        requested_digests: &mut BTreeSet<BlockHash>,
    ) -> NetworkResult<Vec<(BlockHash, Batch)>> {
        self.request_batches(requested_digests).await
    }
}
