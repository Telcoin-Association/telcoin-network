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

use futures::{AsyncRead, AsyncWrite, AsyncWriteExt as _};
use parking_lot::Mutex;
use tn_network_libp2p::{
    error::NetworkError,
    read_frame,
    types::{NetworkHandle, NetworkResult},
    write_frame, Penalty, StreamError, SyncFrame, WorkerSyncRequest,
};
use tn_types::{
    encode, max_batch_size, try_decode, AuthorityIdentifier, Batch, BlockHash, BlsPublicKey, Epoch,
    RpcInfo, SealedBatch, TaskSpawner, WorkerId,
};
use tracing::{debug, warn};

use crate::{
    network::{Req, Res, MAX_BATCH_DIGESTS_PER_REQUEST},
    WorkerGossip, WorkerRPCError, WorkerRequest, WorkerResponse,
};

/// Timeout for reading a single sync data frame from a peer. Batches capped at 1MB.
const BATCH_STREAM_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for the whole batch-sync read exchange, bounding the sum of the frame
/// reads rather than each frame on its own.
///
/// [`BATCH_STREAM_TIMEOUT`] resets on every frame, so a source that drips one
/// valid, requested, unique batch just inside each per-frame deadline would
/// otherwise hold [`WorkerNetworkHandle::read_sync_batches`] for up to
/// `MAX_BATCH_DIGESTS_PER_REQUEST * BATCH_STREAM_TIMEOUT` (~40 min) per peer,
/// stalling batch synchronization. The value is the stream's share of
/// [`BATCH_REQUEST_PEER_BUDGET`]: that budget less [`STREAM_OPEN_DEADLINE`],
/// [`SYNC_REQUEST_WRITE_TIMEOUT`] and [`SYNC_ACK_TIMEOUT`] (95s). Expiry keeps
/// the batches read before it (see [`SyncRead`]), so an honest transfer slower
/// than this keeps what arrived and the remainder goes to the next peer. The
/// responder's own `SEND_STREAM_TIMEOUT` (200s) whole-send bound is unchanged.
pub(crate) const BATCH_STREAM_TOTAL_TIMEOUT: Duration = Duration::from_secs(
    BATCH_REQUEST_PEER_BUDGET
        .as_secs()
        .saturating_sub(STREAM_OPEN_DEADLINE.as_secs())
        .saturating_sub(SYNC_REQUEST_WRITE_TIMEOUT.as_secs())
        .saturating_sub(SYNC_ACK_TIMEOUT.as_secs()),
);

/// Timeout for the responder's first sync frame (`Ack`/`Deny`) after the request
/// frame is written. A peer that negotiated the sync protocol but does not answer
/// (e.g. an item-4 node that registered the protocol but never sends `Ack`) trips
/// this, so the requester stops waiting and tries the next peer.
const SYNC_ACK_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for writing and flushing the sync request frame.
///
/// The responder controls stream credit, so a peer that negotiates the sync
/// protocol and then never reads would otherwise hold the write, and the caller,
/// open with no bound. A request frame carries at most
/// [`MAX_BATCH_DIGESTS_PER_REQUEST`] digests, so an honest peer accepts it well
/// inside this bound. Pairs with [`SYNC_ACK_TIMEOUT`] for the reply.
pub(crate) const SYNC_REQUEST_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// Headroom added to `max_batch_size` for the sync-frame envelope (the `SyncFrame`
/// enum tag and the `Data` length prefix) when bounding a decoded frame.
const SYNC_FRAME_OVERHEAD: usize = 1024;

/// Maximum number of retries through the full peer list in `request_batches()`.
const MAX_BATCH_REQUEST_RETRIES: usize = 3;

/// Delay between retry attempts in `request_batches()` to give semaphores time to release.
const BATCH_REQUEST_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Timeout for one whole `request_batches()` call, across every peer and retry.
///
/// [`BATCH_REQUEST_PEER_SHARES`] shares of [`BATCH_REQUEST_PEER_BUDGET`] (480s),
/// so three peers that each run their exchange to its deadline still leave the
/// fourth peer a full share. Without it, one call walks every connected peer up
/// to [`MAX_BATCH_REQUEST_RETRIES`] times, so a set of stalling peers can hold
/// the caller (the vote-path `synchronize` or a gossip prefetch permit) for
/// hours.
pub(crate) const BATCH_REQUEST_TOTAL_TIMEOUT: Duration = Duration::from_secs(
    BATCH_REQUEST_PEER_BUDGET.as_secs().saturating_mul(BATCH_REQUEST_PEER_SHARES),
);

/// The most of [`BATCH_REQUEST_TOTAL_TIMEOUT`] one peer can use in one call.
///
/// The worst exchange with one peer is [`STREAM_OPEN_DEADLINE`] plus
/// [`SYNC_REQUEST_WRITE_TIMEOUT`] plus [`SYNC_ACK_TIMEOUT`] plus
/// [`BATCH_STREAM_TOTAL_TIMEOUT`], and the stream bound is derived from this
/// budget so the four add up to it exactly. Before this share, two peers that
/// each ran the stream to the old 210s cap used about 470s of the 480s call
/// budget between them (#1773).
pub(crate) const BATCH_REQUEST_PEER_BUDGET: Duration = Duration::from_secs(120);

/// How many [`BATCH_REQUEST_PEER_BUDGET`] shares make one call budget.
const BATCH_REQUEST_PEER_SHARES: u64 = 4;

/// The swarm's deadline for opening an outbound stream, mirrored from
/// `OPEN_DEADLINE` in `tn-network-libp2p` so the per-peer budget accounts for it.
const STREAM_OPEN_DEADLINE: Duration = Duration::from_secs(15);

/// The largest sync frame accepted for `epoch`: a `Data` frame carrying one
/// encoded batch, plus envelope headroom.
pub(crate) fn max_sync_frame_size(epoch: Epoch) -> usize {
    max_batch_size(epoch).saturating_add(SYNC_FRAME_OVERHEAD)
}

/// How a sync batch stream ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamEnd {
    /// The responder sent `End`: the exchange completed.
    Complete,
    /// A deadline cut the stream before `End`: the batches read before it are
    /// kept, and the peer counts as stalled (#1773).
    Stalled,
}

/// The batches read from one sync batch stream, and how the stream ended.
#[derive(Debug)]
pub(crate) struct SyncRead {
    /// The validated batches read before the stream ended, in arrival order.
    batches: Vec<(BlockHash, Batch)>,
    /// Whether the responder finished the stream or a deadline cut it.
    end: StreamEnd,
}

impl SyncRead {
    /// Take the validated batches read before the stream ended.
    pub(crate) fn into_batches(self) -> Vec<(BlockHash, Batch)> {
        self.batches
    }

    /// How the stream ended.
    pub(crate) fn end(&self) -> StreamEnd {
        self.end
    }
}

/// The outcome of a sync-protocol batch fetch attempt.
#[derive(Debug)]
pub(crate) enum SyncAttempt {
    /// The peer answered `Ack` and the stream was read: the batches that arrived,
    /// and whether the stream completed or stalled.
    Fetched(SyncRead),
    /// The peer did not answer the sync exchange; skip it (no legacy fallback).
    Unsupported,
    /// The peer answered with `Deny` (shedding load or lacking the data); try the
    /// next peer, and this peer again on a later pass.
    Denied(NetworkError),
    /// The peer answered but the exchange failed; try the next peer.
    Failed(NetworkError),
}

/// Why one peer did not supply batches to `request_batches()`.
#[derive(Debug)]
enum PeerFetchError {
    /// The peer is sync-capable but answered `Deny`. It stays eligible for later
    /// retry passes, because the retry delay lets its semaphores release.
    Denied(NetworkError),
    /// Any other failure (open error, unsupported protocol, write or read
    /// timeout, protocol error, or an empty answer). The peer is skipped for the
    /// rest of the call and, once it has answered the protocol, tried last on
    /// later calls this epoch.
    Failed(NetworkError),
}

/// Order the connected peers for one pass of
/// [`WorkerNetworkHandle::request_batches`].
///
/// Drops the peers in `skipped` (failed earlier in this call), then yields the
/// preferred peer first, the peers not in `stalled` in the order received (the
/// network layer puts the current committee first and shuffles each tier per
/// request), and the peers in `stalled` last. A marked peer is still asked, so a
/// transient stall costs an honest peer only its place in the order (#1773).
fn order_peers(
    peers: Vec<BlsPublicKey>,
    preferred: Option<&AuthorityIdentifier>,
    skipped: &HashSet<BlsPublicKey>,
    stalled: &HashSet<BlsPublicKey>,
) -> Vec<BlsPublicKey> {
    let (preferred_peers, rest): (Vec<_>, Vec<_>) =
        peers.into_iter().filter(|peer| !skipped.contains(peer)).partition(|peer| {
            preferred.is_some_and(|target| AuthorityIdentifier::from(*peer) == *target)
        });
    let (fresh, marked): (Vec<_>, Vec<_>) =
        rest.into_iter().partition(|peer| !stalled.contains(peer));
    preferred_peers.into_iter().chain(fresh).chain(marked).collect()
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
    /// Peers whose last sync exchange this epoch stalled, failed after
    /// negotiation, or served none of the requested batches.
    ///
    /// [`Self::request_batches`] still asks a marked peer, but after every
    /// unmarked peer, so a committee peer that runs each exchange to its deadline
    /// cannot keep an early place in every call (#1773). A complete exchange that
    /// serves a requested batch clears the mark; [`Self::update_epoch`] resets the
    /// set at the rotation boundary, like the capability cache.
    stalled_peers: Arc<Mutex<HashSet<BlsPublicKey>>>,
    /// The genesis chain id, used to namespace gossip topics this handle publishes.
    chain_id: u64,
    /// The id of the worker this handle belongs to, used to select its gossip topics.
    worker_id: WorkerId,
}

impl WorkerNetworkHandle {
    /// Create a new instance of [Self] for worker `worker_id`.
    pub fn new(
        handle: NetworkHandle<Req, Res>,
        task_spawner: TaskSpawner,
        worker_id: WorkerId,
        epoch: Epoch,
        chain_id: u64,
    ) -> Self {
        Self {
            handle,
            task_spawner,
            epoch,
            chain_id,
            worker_id,
            sync_capability: Arc::new(Mutex::new(HashMap::new())),
            stalled_peers: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Return the id of the worker this handle belongs to.
    pub fn worker_id(&self) -> WorkerId {
        self.worker_id
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
            .publish(
                tn_config::LibP2pConfig::worker_batch_topic(self.chain_id, self.worker_id),
                data,
            )
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
    /// Tries peers one at a time until all batches are received or no peer is
    /// left to try. Each pass re-fetches the connected peers, which come with
    /// current-committee members first. When `preferred` names an authority (the
    /// author of the header that references the batches), its peer is tried
    /// first, because it is the peer most likely to hold them.
    ///
    /// A peer that answers `Deny` stays eligible for later passes; a peer that
    /// fails in any other way (a deadline, an error after negotiation, or `Ack`
    /// then `End` with none of the requested batches) is skipped for the rest of
    /// the call and tried last on later calls this epoch (see [`order_peers`]).
    /// The network layer shuffles each tier of the peer list per request, so no
    /// peer keeps a fixed early place. Retries up to
    /// [MAX_BATCH_REQUEST_RETRIES] times through the peer list with
    /// [BATCH_REQUEST_RETRY_DELAY] between attempts, giving semaphores time to
    /// release.
    ///
    /// The whole call is bounded by [BATCH_REQUEST_TOTAL_TIMEOUT]: on expiry it
    /// returns the batches fetched so far, or [NetworkError::Timeout] if none were
    /// fetched. Returns `Ok` if any batches successfully fetched from peers, or if
    /// no peer is connected. Only fetched digests are removed from
    /// `requested_digests`.
    pub(crate) async fn request_batches(
        &self,
        requested_digests: &mut BTreeSet<BlockHash>,
        preferred: Option<&AuthorityIdentifier>,
    ) -> NetworkResult<Vec<(BlockHash, Batch)>> {
        let mut all_batches = Vec::with_capacity(requested_digests.len());

        // the passes push each fetched batch into `all_batches` as it arrives, so a
        // timeout keeps the batches fetched before it fired
        let outcome = tokio::time::timeout(
            BATCH_REQUEST_TOTAL_TIMEOUT,
            self.request_batch_passes(requested_digests, preferred, &mut all_batches),
        )
        .await;
        outcome
            .unwrap_or_else(|_elapsed| {
                warn!(
                    target: "worker::network",
                    missing = requested_digests.len(),
                    fetched = all_batches.len(),
                    "request batches hit the total timeout, still missing digests"
                );
                if all_batches.is_empty() {
                    Err(NetworkError::Timeout)
                } else {
                    Ok(())
                }
            })
            .map(|()| all_batches)
    }

    /// The retry passes of [Self::request_batches], without the total timeout.
    ///
    /// Pushes each fetched batch into `all_batches` and removes its digest from
    /// `requested_digests` as it arrives, so a caller that stops waiting keeps
    /// what was fetched. `Ok(())` means stop and return `all_batches`.
    async fn request_batch_passes(
        &self,
        requested_digests: &mut BTreeSet<BlockHash>,
        preferred: Option<&AuthorityIdentifier>,
        all_batches: &mut Vec<(BlockHash, Batch)>,
    ) -> NetworkResult<()> {
        // peers that failed in a way a retry does not fix, not tried again this call
        let mut skipped: HashSet<BlsPublicKey> = HashSet::new();

        for attempt in 0..MAX_BATCH_REQUEST_RETRIES {
            // re-fetch peers each attempt to pick up newly connected peers
            let peers = self.handle.connected_peers().await?;
            if peers.is_empty() {
                return Ok(());
            }

            // the preferred peer first, then the unmarked peers in the order
            // received, then the peers whose last exchange stalled (#1773)
            let stalled = self.stalled_peers.lock().clone();
            let ordered = order_peers(peers, preferred, &skipped, &stalled);

            // try peers one at a time with fallback
            for peer in ordered {
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

                // update remaining batches, or log the error and skip a failed peer
                self.request_batches_from_peer(peer, digests_for_peer).await.map_or_else(
                    |error| match error {
                        PeerFetchError::Denied(e) => {
                            warn!(
                                target: "worker::network",
                                %peer,
                                ?e,
                                "peer denied batch request, trying next"
                            );
                        }
                        PeerFetchError::Failed(e) => {
                            warn!(
                                target: "worker::network",
                                %peer,
                                ?e,
                                "failed to fetch batches from peer, skipping it for this request"
                            );
                            skipped.insert(peer);
                        }
                    },
                    |batches| {
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
                    },
                );
            }

            // return immediately if any batches fetched (partial success) or all done
            if !all_batches.is_empty() || requested_digests.is_empty() {
                return Ok(());
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
            Ok(())
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
    ///
    /// Returns [`PeerFetchError::Denied`] when the peer answered `Deny`, so the
    /// caller may try it again on a later pass, and [`PeerFetchError::Failed`] for
    /// every other failure, so the caller skips the peer for the rest of the call.
    async fn request_batches_from_peer(
        &self,
        peer: BlsPublicKey,
        batch_digests: &BTreeSet<BlockHash>,
    ) -> Result<Vec<(BlockHash, Batch)>, PeerFetchError> {
        // sanity check - should never happen
        if batch_digests.is_empty() {
            warn!(target: "worker::network", "requested empty batches from peer!");
            return Ok(vec![]);
        }

        // a peer known not to serve sync this epoch is skipped so the caller tries
        // the next peer
        if self.sync_capability.lock().get(&peer) == Some(&false) {
            return Err(PeerFetchError::Failed(NetworkError::RPCRetryable(format!(
                "peer {peer} does not serve the batch sync protocol this epoch"
            ))));
        }

        match self.request_batches_from_peer_sync(peer, batch_digests).await {
            SyncAttempt::Fetched(read) => {
                self.sync_capability.lock().insert(peer, true);
                self.record_sync_read(peer, read)
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
                Err(PeerFetchError::Failed(NetworkError::RPCRetryable(format!(
                    "peer {peer} does not serve the batch sync protocol"
                ))))
            }
            SyncAttempt::Denied(e) => {
                // the peer speaks sync but is shedding load or lacks the data; it
                // stays eligible for a later pass
                self.sync_capability.lock().insert(peer, true);
                Err(PeerFetchError::Denied(e))
            }
            SyncAttempt::Failed(e) => {
                // the peer speaks sync but this exchange failed; try the next peer,
                // and this peer last on later calls (#1773)
                self.sync_capability.lock().insert(peer, true);
                self.mark_stalled(peer);
                Err(PeerFetchError::Failed(e))
            }
        }
    }

    /// Mark `peer` as stalled for the rest of the epoch, so later calls try it last.
    fn mark_stalled(&self, peer: BlsPublicKey) {
        self.stalled_peers.lock().insert(peer);
    }

    /// Record the outcome of an `Ack`ed exchange with `peer` and yield its batches.
    ///
    /// A complete stream that served at least one requested batch clears the
    /// peer's stall mark. A stream cut by a deadline keeps the batches read before
    /// it but marks the peer; with no batches at all it is a failed exchange, as
    /// before, so the caller skips the peer for the rest of the call. A complete
    /// stream with none of the requested batches (`Ack` then `End`) is a failed
    /// exchange too: the peer is marked and skipped, instead of counting as a
    /// success and keeping its place on every later pass (#1773).
    fn record_sync_read(
        &self,
        peer: BlsPublicKey,
        read: SyncRead,
    ) -> Result<Vec<(BlockHash, Batch)>, PeerFetchError> {
        let end = read.end();
        let batches = read.into_batches();
        match end {
            StreamEnd::Complete if batches.is_empty() => {
                self.mark_stalled(peer);
                Err(PeerFetchError::Failed(NetworkError::RPCRetryable(format!(
                    "peer {peer} answered the batch sync request with none of the requested batches"
                ))))
            }
            StreamEnd::Complete => {
                self.stalled_peers.lock().remove(&peer);
                Ok(batches)
            }
            StreamEnd::Stalled => {
                self.mark_stalled(peer);
                (!batches.is_empty())
                    .then_some(batches)
                    .ok_or(PeerFetchError::Failed(NetworkError::Timeout))
            }
        }
    }

    /// Attempt a batch fetch over the typed sync protocol.
    ///
    /// Opens a `/tn-worker-{id}-sync` stream, writes the request in the opening
    /// [`SyncFrame::Req`] frame, and reads the response: an [`SyncFrame::Ack`]
    /// followed by [`SyncFrame::Data`] frames terminated by [`SyncFrame::End`].
    /// Returns [`SyncAttempt::Unsupported`] when the peer does not answer (so the
    /// caller skips it), [`SyncAttempt::Fetched`] on success,
    /// [`SyncAttempt::Denied`] when the peer answered `Deny`, or
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
    /// the caller keeps it sync-capable and tries the next peer.
    /// `Err(SyncAttempt::Denied(_))` means the peer answered `Deny`. A transport I/O
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
        // so the peer is sync-capable; a write failure or a stalled write is
        // transient -> try next.
        self.write_sync_request(&mut stream, batch_digests).await?;
        let max_frame = max_sync_frame_size(self.epoch());

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
            // sync-capable, but shedding load or lacking the data: try next peer,
            // and this peer again on a later pass
            SyncFrame::Deny(reason) => Err(SyncAttempt::Denied(NetworkError::RPCRetryable(
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

    /// Write and flush the sync request frame for `batch_digests`, bounded by
    /// [`SYNC_REQUEST_WRITE_TIMEOUT`].
    ///
    /// The responder controls stream credit, so a peer that never reads would
    /// otherwise hold the write open with no bound. A write error and an expired
    /// write both map to [`SyncAttempt::Failed`]: negotiation already succeeded,
    /// so the peer is sync-capable and the caller tries the next peer.
    pub(crate) async fn write_sync_request<S: AsyncWrite + Unpin + Send>(
        &self,
        stream: &mut S,
        batch_digests: &BTreeSet<BlockHash>,
    ) -> Result<(), SyncAttempt> {
        let max_frame = max_sync_frame_size(self.epoch());
        let request = SyncFrame::Req(WorkerSyncRequest::Batches {
            batch_digests: batch_digests.clone(),
            epoch: self.epoch(),
        });
        let (mut encode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        tokio::time::timeout(SYNC_REQUEST_WRITE_TIMEOUT, async {
            write_frame(stream, &request, &mut encode_buffer, &mut compressed_buffer, max_frame)
                .await
                .and(stream.flush().await)
        })
        .await
        .map_err(|_elapsed| {
            SyncAttempt::Failed(NetworkError::RPCRetryable(
                "timed out writing sync request frame".to_string(),
            ))
        })?
        .map_err(|e| {
            SyncAttempt::Failed(NetworkError::RPCRetryable(format!(
                "failed to write sync request frame: {e}"
            )))
        })
    }

    /// Read `Data` frames terminated by `End` from an accepted sync exchange,
    /// decoding and validating each batch as it arrives.
    ///
    /// Validates every batch: it must have been requested, none may repeat, and
    /// the running total may not exceed the request. Each frame read is bounded by
    /// [`BATCH_STREAM_TIMEOUT`] and the whole stream by
    /// [`BATCH_STREAM_TOTAL_TIMEOUT`]. A deadline does not discard the validated
    /// batches read before it: the result carries them with
    /// [`StreamEnd::Stalled`], so a stalling peer no longer wastes the frames it
    /// did deliver (#1773). A protocol violation is still an error.
    pub(crate) async fn read_sync_batches<S: AsyncRead + Unpin + Send>(
        &self,
        stream: &mut S,
        requested_digests: &BTreeSet<BlockHash>,
    ) -> NetworkResult<SyncRead> {
        let max_frame = max_sync_frame_size(self.epoch());
        let (mut decode_buffer, mut compressed_buffer) = (Vec::new(), Vec::new());
        let mut batches = Vec::with_capacity(requested_digests.len());
        let mut received_digests = HashSet::with_capacity(requested_digests.len());

        // Read the frame loop under an aggregate deadline so the exchange as a
        // whole is bounded, not just each frame. Without this, a source that drips
        // one valid frame just inside every `BATCH_STREAM_TIMEOUT` keeps this loop
        // alive for `MAX_BATCH_DIGESTS_PER_REQUEST` frames (~40 min). The
        // per-frame timeout stays as the inner bound; see `BATCH_STREAM_TOTAL_TIMEOUT`.
        let read_stream = async {
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

            Ok::<(), NetworkError>(())
        };

        // a per-frame or whole-stream deadline ends the read as `Stalled` and keeps
        // the batches validated so far; any other error is still an error
        let end = tokio::time::timeout(BATCH_STREAM_TOTAL_TIMEOUT, read_stream)
            .await
            .map_or_else(
                |_elapsed| {
                    warn!(target: "worker::network", "timeout reading sync batch stream");
                    Ok(StreamEnd::Stalled)
                },
                |read| read.map(|()| StreamEnd::Complete),
            )
            .or_else(|e| {
                matches!(e, NetworkError::Timeout).then_some(StreamEnd::Stalled).ok_or(e)
            })?;

        Ok(SyncRead { batches, end })
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
    /// legacy last epoch is re-probed for the sync protocol this epoch. The stall
    /// record is reset too: the committee rotates, so last epoch's marks no longer
    /// describe the peers that serve this epoch.
    pub fn update_epoch(&mut self, epoch: Epoch) {
        self.epoch = epoch;
        self.sync_capability.lock().clear();
        self.stalled_peers.lock().clear();
    }
}

// support IT tests
#[cfg(any(test, feature = "test-utils"))]
impl WorkerNetworkHandle {
    /// Convenience method for creating a new Self for tests- sends events no-where and does
    /// nothing.
    pub fn new_for_test(task_spawner: TaskSpawner) -> Self {
        let (tx, _rx) = tokio::sync::mpsc::channel(5);
        Self::new_for_test_with_sender(tx, task_spawner)
    }

    /// Like [`Self::new_for_test`], but sends network commands to `sender`, so a test
    /// can observe what the handle reports (for example a peer penalty).
    pub fn new_for_test_with_sender(
        sender: tokio::sync::mpsc::Sender<
            tn_network_libp2p::types::NetworkCommand<WorkerRequest, WorkerResponse>,
        >,
        task_spawner: TaskSpawner,
    ) -> Self {
        Self {
            handle: NetworkHandle::new(sender),
            task_spawner,
            epoch: 0,
            chain_id: 0,
            worker_id: tn_types::DEFAULT_WORKER_ID,
            sync_capability: Arc::new(Mutex::new(HashMap::new())),
            stalled_peers: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Publicly available for tests.
    /// See [Self::request_batches].
    pub async fn pub_request_batches(
        &self,
        requested_digests: &mut BTreeSet<BlockHash>,
        preferred: Option<&AuthorityIdentifier>,
    ) -> NetworkResult<Vec<(BlockHash, Batch)>> {
        self.request_batches(requested_digests, preferred).await
    }

    /// The peers marked as stalled this epoch. See [`Self::request_batches`].
    pub fn stalled_peers_for_test(&self) -> HashSet<BlsPublicKey> {
        self.stalled_peers.lock().clone()
    }

    /// Mark `peer` as stalled, as a failed exchange would. See [`Self::request_batches`].
    pub fn mark_stalled_for_test(&self, peer: BlsPublicKey) {
        self.mark_stalled(peer);
    }
}

#[cfg(test)]
mod tests {
    use super::{StreamEnd, SyncRead, WorkerNetworkHandle};
    use tn_types::{BlsPublicKey, TaskManager};

    /// `Ack` then `End` with none of the requested batches is a failed exchange and marks
    /// the peer, instead of counting as a success (#1773).
    #[test]
    fn empty_complete_answer_fails_and_marks_the_peer() {
        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
        let peer = BlsPublicKey::default();
        let read = SyncRead { batches: Vec::new(), end: StreamEnd::Complete };
        assert!(handle.record_sync_read(peer, read).is_err(), "an empty answer is a failure");
        assert!(handle.stalled_peers_for_test().contains(&peer), "the peer is marked");
    }

    /// A stream cut by a deadline before any batch arrived is a failed exchange and marks
    /// the peer.
    #[test]
    fn stalled_empty_stream_fails_and_marks_the_peer() {
        let task_manager = TaskManager::default();
        let handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
        let peer = BlsPublicKey::default();
        let read = SyncRead { batches: Vec::new(), end: StreamEnd::Stalled };
        assert!(handle.record_sync_read(peer, read).is_err(), "a stalled empty stream fails");
        assert!(handle.stalled_peers_for_test().contains(&peer), "the peer is marked");
    }

    /// The epoch boundary resets the stall record, like the capability cache.
    #[test]
    fn update_epoch_clears_the_stall_record() {
        let task_manager = TaskManager::default();
        let mut handle = WorkerNetworkHandle::new_for_test(task_manager.get_spawner());
        let peer = BlsPublicKey::default();
        handle.mark_stalled_for_test(peer);
        assert!(handle.stalled_peers_for_test().contains(&peer));
        handle.update_epoch(1);
        assert!(handle.stalled_peers_for_test().is_empty(), "the record is reset per epoch");
    }
}
