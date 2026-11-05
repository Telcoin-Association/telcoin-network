//! The type that handles core logic for requests between workers.
use super::{
    error::{WorkerNetworkError, WorkerNetworkResult},
    handle::WorkerNetworkHandle,
    message::WorkerGossip,
};
use crate::{
    batch_fetcher::get_batch_local_cache,
    metrics::WorkerMetrics,
    network::{stream_codec, SyncServeOutcome, MAX_CONCURRENT_GOSSIP_PREFETCHES},
};
use futures::AsyncWriteExt as _;
use lru::LruCache;
use parking_lot::Mutex;
use std::{
    collections::{BTreeSet, HashSet},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use tn_config::ConsensusConfig;
use tn_network_libp2p::{
    write_frame, GossipMessage, Stream, SyncFrame, SyncFrameError, WorkerSyncRequest,
};
use tn_network_types::{WorkerOthersBatchMessage, WorkerToPrimaryClient};
use tn_storage::{consensus::ConsensusChain, tables::NodeBatchesCache};
use tn_types::{
    ensure, now, try_decode, Batch, BatchValidation, BatchValidationError, BlsPublicKey, Database,
    Epoch, SealedBatch, WorkerId, B256,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, warn};

/// Total timeout for sending all batches over a stream.
/// Prevents slow-reader attacks where a peer accepts a stream but never reads.
/// Set for 500MB through emerging market worse-case 20MB/s upload.
///
/// Exposed to the requester side (`handle.rs`), which sizes its per-chunk-count
/// read tolerance (`INTER_CHUNK_STREAM_TIMEOUT`) to be at least this whole-stream
/// cap so an honest sender is never disconnected mid-transfer.
pub(crate) const SEND_STREAM_TIMEOUT: Duration = Duration::from_secs(200);

/// Timeout for writing one sync `Data` frame, flushing one chunk, or writing the
/// `End` frame.
///
/// The limit restarts for every frame, so an honest reader must only keep up with
/// one frame (at most one batch, about 1 MiB) per 30 seconds. A peer that stops
/// reading releases its batch-serve permit within one limit instead of holding
/// it for the whole [`SEND_STREAM_TIMEOUT`].
pub(crate) const SEND_STREAM_FRAME_TIMEOUT: Duration = Duration::from_secs(30);

/// Capacity of the per-epoch set of rejected reported batch digests (issue #1774).
///
/// The set is a least-recently-used cache, so its memory stays bounded: at most 1024 digests of
/// 32 bytes each, plus the overhead of the cache. When the set is full, a new digest replaces the
/// oldest one, and a later repeat of the dropped digest is validated again.
pub(crate) const REJECTED_BATCH_DIGESTS_CAPACITY: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(1023);

/// Whether a batch validation failure proves that the batch body itself is invalid.
///
/// Only the digest of a body fault is kept in the set of rejected digests of
/// [`RequestHandler`]. A [`BatchValidationError::InvalidDigest`] failure is not a body fault: the
/// body did not hash to the claimed digest, so the sender chose that digest, not the body.
/// Keeping that digest would let a sender poison the digest of an honest batch that arrives
/// later. A [`BatchValidationError::CanonicalChain`] failure is not a body fault either: it
/// depends on the local chain state (a parent header this node has not stored yet), so the same
/// body can validate later.
///
/// Every other failure proves a body fault only because `validate_batch` checks the digest
/// before all other checks. Then the failed body hashes to the digest, and no other body can
/// carry it. If a check moves before the digest check, its error no longer proves this.
fn is_body_fault(err: &BatchValidationError) -> bool {
    match err {
        BatchValidationError::InvalidDigest | BatchValidationError::CanonicalChain { .. } => false,
        BatchValidationError::EmptyBatch
        | BatchValidationError::HeaderMaxGasExceedsGasLimit { .. }
        | BatchValidationError::CalculateMaxPossibleGas
        | BatchValidationError::HeaderTransactionBytesExceedsMax(_)
        | BatchValidationError::RecoverTransaction(_, _)
        | BatchValidationError::InvalidBaseFee { .. }
        | BatchValidationError::InvalidWorkerId { .. }
        | BatchValidationError::InvalidTx4844(_)
        | BatchValidationError::UnsupportedTxType { .. }
        | BatchValidationError::InvalidEpoch { .. }
        | BatchValidationError::GasOverflow => true,
    }
}

/// RAII guard for an admitted gossip-triggered batch prefetch.
///
/// Holds a global concurrency permit and marks the batch digest as having a
/// prefetch in flight. Dropping it releases the permit and clears the in-flight
/// mark, so a completed, failed, or aborted prefetch frees a concurrency slot and
/// re-enables a later prefetch of the same digest.
#[derive(Debug)]
struct PrefetchPermit {
    /// Global concurrency permit, released on drop.
    _permit: OwnedSemaphorePermit,
    /// Shared set of digests with an in-flight prefetch; `digest` is removed on drop.
    in_flight: Arc<Mutex<HashSet<B256>>>,
    /// The digest this permit tracks.
    digest: B256,
}

impl Drop for PrefetchPermit {
    fn drop(&mut self) {
        self.in_flight.lock().remove(&self.digest);
    }
}

/// Try to admit one gossip-triggered batch prefetch for `digest`.
///
/// Returns `None` to skip the prefetch when either guard trips:
/// - concurrency: the semaphore already holds [`MAX_CONCURRENT_GOSSIP_PREFETCHES`] in-flight
///   prefetches, so load is shed rather than amplified;
/// - dedup: a prefetch for `digest` is already in flight, so a re-gossiped digest collapses to a
///   single fetch.
///
/// On success the returned [`PrefetchPermit`] holds the permit and the in-flight
/// mark until dropped. Together these bound the fetch fan-out a Byzantine author can
/// trigger by gossiping many distinct (or repeated) batch digests.
fn try_admit_prefetch(
    semaphore: &Arc<Semaphore>,
    in_flight: &Arc<Mutex<HashSet<B256>>>,
    digest: B256,
) -> Option<PrefetchPermit> {
    // Cap concurrency: shed when the semaphore is full.
    let permit = semaphore.clone().try_acquire_owned().ok()?;
    // Dedup: skip (releasing `permit`) when a prefetch for this digest is already in
    // flight. `insert` returns `false` when the digest is already present.
    in_flight.lock().insert(digest).then_some(())?;
    Some(PrefetchPermit { _permit: permit, in_flight: in_flight.clone(), digest })
}

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
    /// Digests with a gossip-triggered batch prefetch currently in flight.
    ///
    /// Deduplicates concurrent prefetches for the same digest so a repeated
    /// (possibly forged) gossiped digest cannot by itself trigger a fetch storm.
    /// Shared across clones via `Arc`; bounded by `prefetch_semaphore` (an entry is
    /// retained only while its permit is held).
    in_flight_prefetch: Arc<Mutex<HashSet<B256>>>,
    /// Caps concurrent gossip-triggered batch prefetches at
    /// [`MAX_CONCURRENT_GOSSIP_PREFETCHES`], shedding beyond that to bound the fetch
    /// fan-out a Byzantine author can trigger by gossiping many distinct digests.
    prefetch_semaphore: Arc<Semaphore>,
    /// Digests of batches that failed validation with a body fault in this epoch (issue #1774).
    ///
    /// Committee peers are exempt from peer scoring. Without this set, a committee peer can send
    /// the same rejected batch again and again and make this worker validate it at full cost each
    /// time. [`Self::process_report_batch`] refuses a digest in this set without validation.
    ///
    /// The handler is built once per epoch: `spawn_worker_network_for_epoch`
    /// (`crates/node/src/manager/node/start_epoch.rs`) builds a new `WorkerNetwork`, and its
    /// constructor builds this handler. The set is therefore per epoch and never carries a digest
    /// into a later epoch. It is shared across clones through `Arc` and holds at most
    /// [`REJECTED_BATCH_DIGESTS_CAPACITY`] digests.
    rejected_batches: Arc<Mutex<LruCache<B256, ()>>>,
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
        Self {
            id,
            validator,
            consensus_config,
            network_handle,
            metrics,
            in_flight_prefetch: Arc::new(Mutex::new(HashSet::new())),
            prefetch_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_GOSSIP_PREFETCHES)),
            rejected_batches: Arc::new(Mutex::new(LruCache::new(REJECTED_BATCH_DIGESTS_CAPACITY))),
        }
    }

    /// Record a failed validation of the batch with `digest`.
    ///
    /// Every failure is counted in metrics. The digest is kept in `rejected_batches` for the
    /// rest of the epoch only when [`is_body_fault`] holds for `err`.
    fn record_validation_failure(&self, digest: B256, err: &BatchValidationError) {
        self.metrics.record_batch_validation_failure();
        if is_body_fault(err) {
            self.rejected_batches.lock().put(digest, ());
        }
    }

    /// Process gossip from the committee.
    ///
    /// Workers gossip a batch digest once it is accepted so that peers which consume individual
    /// current-epoch batches — committee validators, including one catching up to rejoin — can
    /// prefetch the body into `NodeBatchesCache` before the vote path needs it. Observers do not
    /// subscribe to this topic (issue #960): they receive batch bodies inside the verified
    /// consensus output and epoch packs they already download (`crates/state-sync`).
    pub(super) async fn process_gossip(&self, msg: &GossipMessage) -> WorkerNetworkResult<()> {
        // deconstruct message
        let GossipMessage { data, source: _, sequence_number: _, topic } = msg;

        // gossip is uncompressed
        let gossip = try_decode(data)?;

        match gossip {
            WorkerGossip::Batch(epoch, batch_hash) => {
                ensure!(
                    topic.to_string().eq(&tn_config::LibP2pConfig::worker_batch_topic(
                        self.consensus_config.chain_id(),
                        self.id,
                    )),
                    WorkerNetworkError::InvalidTopic
                );
                let my_epoch = self.consensus_config.epoch();
                // We are probably behind. Do not bother to fetch and store this Batch now: the
                // cache is cleared at the epoch boundary, and a node that is behind receives the
                // bodies for the epochs it is missing inside the epoch packs / consensus output
                // it syncs (`crates/state-sync`), not through a later per-batch fetch.
                ensure!(my_epoch == epoch, WorkerNetworkError::BatchEpochMismatch(epoch, my_epoch));
                // Gossip normally arrives before the batch is committed, so only the cache is
                // checked. A late gossip for a batch that is already committed and evicted
                // refetches it and caches it again until the epoch ends, at most once per digest.
                // `WorkerNetwork` holds the consensus chain and could pass it into this call, as
                // it already does for sync streams, to also check the pack with
                // `ConsensusChain::contains_current_batch`. That check is left out because it
                // would add a pack-actor round trip to every first-arrival gossip.
                if !matches!(
                    get_batch_local_cache(batch_hash, self.consensus_config.node_storage(),),
                    Ok(Some(_))
                ) {
                    // Deduplicate and cap gossip-triggered prefetches before fanning
                    // out a fetch. A Byzantine committee author can gossip many
                    // distinct (possibly forged) digests; without this gate each one
                    // would trigger an unbounded, undeduplicated `request_batches`
                    // fan-out across the worker mesh. `_prefetch` holds the dedup mark
                    // and a concurrency permit for the whole fetch, releasing both on
                    // scope exit. When admission declines (a prefetch for this digest
                    // is already in flight, or the cap is reached) the prefetch is
                    // skipped: it is a best-effort optimization, and a genuinely needed
                    // batch is still fetched by the vote path itself
                    // (`PrimaryReceiverHandler::synchronize` -> `request_batches`).
                    let Some(_prefetch) = try_admit_prefetch(
                        &self.prefetch_semaphore,
                        &self.in_flight_prefetch,
                        batch_hash,
                    ) else {
                        return Ok(());
                    };
                    // If batch is missing from db, then request from peer. Only committee
                    // validators subscribe to this topic (issue #960), so this warms the cache
                    // the vote path reads: an active CVV usually already has the body from the
                    // normal worker path, while a CVV catching up to rejoin gets the most out of
                    // the prefetch — the cache survives the mode-change re-entry that promotes it.
                    let mut missing = BTreeSet::from([batch_hash]);
                    match self.network_handle.request_batches(&mut missing, None).await {
                        Ok(batches) => {
                            if let Some((digest, batch)) = batches.first() {
                                self.validate_and_cache_prefetched_batch(digest, batch)?;
                            }
                        }
                        Err(e) => {
                            tracing::error!(target: "worker:network", "failed to get gossipped batch {batch_hash}: {e}");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Validate a gossip-prefetched batch body, then cache it on success.
    ///
    /// The vote-path sync (`PrimaryReceiverHandler::synchronize`) treats a digest
    /// already present in `NodeBatchesCache` as validated-and-available and skips
    /// `validate_batch` for it. A body fetched by the gossip prefetch must therefore
    /// be validated here before it enters that cache, exactly as
    /// `process_report_batch` does; an invalid body is dropped (never cached) and the
    /// failure recorded. Without this, a Byzantine committee member could gossip and
    /// serve a semantically-invalid batch, have every honest worker cache it
    /// unvalidated, and then get it voted on and certified (issue #933).
    ///
    /// A digest that already failed validation in this epoch is dropped without validating it
    /// again, like any other invalid body. A failed validation keeps the digest in
    /// `rejected_batches` under the same rule as [`Self::process_report_batch`], so both paths
    /// agree (issue #1774).
    ///
    /// A batch for a future epoch is discarded rather than cached: the cache is
    /// cleared on epoch boundaries and storing it would let a rogue validator waste
    /// disk on batches that can never be used.
    fn validate_and_cache_prefetched_batch(
        &self,
        digest: &B256,
        batch: &Batch,
    ) -> WorkerNetworkResult<()> {
        if batch.epoch != self.consensus_config.epoch() {
            debug!(
                target: "worker:network",
                batch_epoch = batch.epoch,
                current_epoch = self.consensus_config.epoch(),
                "gossipped batch epoch mismatch - discarding"
            );
            return Ok(());
        }

        // `request_batches` guarantees the fetched body hashes to `digest`, so seal
        // with it and let `validate_batch` re-check the digest along with byte size,
        // per-transaction decode/recovery, blob exclusion, gas limit, and base fee. A
        // validation failure means a peer served an invalid body: drop it (do not
        // cache) and record the failure. This is a content fault of the served body,
        // not of the gossip relayer, so it is not propagated as an error.
        let already_rejected = self.rejected_batches.lock().contains(digest);
        let verdict = if already_rejected {
            // a digest that failed validation earlier in this epoch is dropped without
            // validating it again, like any other invalid body
            Err(WorkerNetworkError::RejectedBatchRepeat(*digest))
        } else {
            self.validator
                .validate_batch(batch.clone().seal(*digest))
                .inspect_err(|err| self.record_validation_failure(*digest, err))
                .map_err(WorkerNetworkError::from)
        };
        if let Err(err) = verdict {
            warn!(
                target: "worker:network",
                ?digest,
                %err,
                "gossip-prefetched batch failed validation - discarding"
            );
            return Ok(());
        }

        self.consensus_config.node_storage().insert::<NodeBatchesCache>(digest, batch).map_err(
            |e| WorkerNetworkError::Internal(format!("failed to write to batch store: {e}")),
        )
    }

    /// Process a new reported batch.
    ///
    /// A committee member reports a batch to ask this worker to attest it. The worker
    /// validates the batch, caches it in `NodeBatchesCache`, and reports the digest to its
    /// own primary, which acknowledges the batch to the reporter.
    ///
    /// A digest that is already in `NodeBatchesCache` skips validation and the write
    /// (issue #1515). The worker already treats a cached digest as validated: the vote-path
    /// sync skips `validate_batch` for it, and [`Self::validate_and_cache_prefetched_batch`]
    /// validates before it caches for that reason. The cache is cleared at every epoch
    /// boundary, so a hit is always a batch validated in the current epoch. A report that
    /// carries an older epoch is not in the cache and fails validation.
    ///
    /// A digest that failed validation earlier in this epoch is refused with
    /// [`WorkerNetworkError::RejectedBatchRepeat`] before validation (issue #1774). The handler
    /// keeps these digests in `rejected_batches`, a bounded set that is new in each epoch. A
    /// failure with [`BatchValidationError::InvalidDigest`] is not kept, because that body did
    /// not hash to the claimed digest. A repeat of such a report is validated again.
    ///
    /// A hit still reports the digest to the primary, so the reporter gets its acknowledgement
    /// exactly as before. The acknowledgement is truthful because the worker validated that
    /// digest itself. The digest in a [`SealedBatch`] comes from the sender and is not checked
    /// when the value is built, so a repeat can attach a different body to a cached digest.
    /// That body is dropped and the validated body stays in the cache, so the only party
    /// misled is the sender.
    ///
    /// Without these checks, any committee member (through a bug or on purpose) can make every
    /// peer redo full validation for each repeat it sends, and a database write for each repeat
    /// of a valid batch, with no penalty, because committee members are exempt from peer
    /// scoring. The checks cover exact repeats only. They do not limit a flood of distinct
    /// batches.
    pub(super) async fn process_report_batch(
        &self,
        peer: &BlsPublicKey,
        sealed_batch: SealedBatch,
    ) -> WorkerNetworkResult<()> {
        // return error if reporter isn't in current committee
        if !self.consensus_config.committee_pub_keys().contains(peer) {
            return Err(WorkerNetworkError::NonCommitteeBatch);
        }

        let client = self.consensus_config.local_network(self.id).cloned().ok_or_else(|| {
            WorkerNetworkError::Internal(format!(
                "no local network instance for worker id {}",
                self.id
            ))
        })?;
        let digest = sealed_batch.digest();

        // cheap key lookup before the expensive validation: a cached digest is already
        // validated for this epoch, so a repeat only needs the acknowledgement below
        let already_cached = self
            .consensus_config
            .node_storage()
            .contains_key::<NodeBatchesCache>(&digest)
            .map_err(|e| {
                WorkerNetworkError::Internal(format!("failed to read batch store: {e}"))
            })?;
        if already_cached {
            debug!(
                target: "worker:network",
                %peer,
                ?digest,
                "reported batch already validated and cached - skipping validation"
            );
        } else {
            // cheap set lookup before the expensive validation: a digest that failed validation
            // in this epoch is refused without validating it again
            let already_rejected = self.rejected_batches.lock().contains(&digest);
            ensure!(!already_rejected, WorkerNetworkError::RejectedBatchRepeat(digest));
            self.validate_and_cache_reported_batch(sealed_batch)?;
        }

        // notify primary for payload store
        client
            .report_others_batch(WorkerOthersBatchMessage::new(digest, self.id))
            .await
            .map_err(|e| WorkerNetworkError::Internal(e.to_string()))?;

        Ok(())
    }

    /// Validate a reported batch, then cache it with the time it was received.
    ///
    /// A validation failure is recorded in metrics and returned, and the batch is not
    /// cached. When the failure is a body fault (see [`is_body_fault`]), the digest is also
    /// kept in `rejected_batches`, so [`Self::process_report_batch`] refuses a repeat of it for
    /// the rest of the epoch without validating it again (issue #1774).
    fn validate_and_cache_reported_batch(
        &self,
        sealed_batch: SealedBatch,
    ) -> WorkerNetworkResult<()> {
        // validate batch - record a failure, and keep the digest of a body fault
        let digest = sealed_batch.digest();
        self.validator
            .validate_batch(sealed_batch.clone())
            .inspect_err(|err| self.record_validation_failure(digest, err))?;

        let (mut batch, _) = sealed_batch.split();

        // Set received_at timestamp for remote batch.
        batch.set_received_at(now());
        self.consensus_config.node_storage().insert::<NodeBatchesCache>(&digest, &batch).map_err(
            |e| WorkerNetworkError::Internal(format!("failed to write to batch store: {e}")),
        )
    }

    /// Serve an admitted inbound sync batch exchange and report how it ended.
    ///
    /// The opening [`SyncFrame::Req`] has already been read and the exchange has
    /// passed both admission phases. This writes the [`SyncFrame::Ack`], streams
    /// the requested batches as [`SyncFrame::Data`] frames, and ends the stream
    /// with [`SyncFrame::End`]. The `Ack` write is bounded by
    /// `SYNC_REQUEST_READ_TIMEOUT`; each `Data` write, chunk flush, and the `End`
    /// write by [`SEND_STREAM_FRAME_TIMEOUT`]; and the whole serve by
    /// [`SEND_STREAM_TIMEOUT`]. Thus a peer that stops reading releases the
    /// batch-serve permit at the first limit it trips.
    ///
    /// A failed serve is logged and best-effort signalled to the requester with a
    /// [`SyncFrame::Err`] so it stops waiting. The returned [`SyncServeOutcome`]
    /// tells the caller if the failure was a stall, which it charges to a public
    /// peer, or another failure, which it does not charge.
    ///
    /// `epoch` is the request epoch, which the caller has checked is inside the
    /// node's serve window; it selects the epoch pack read on a cache miss.
    /// `max_frame` is the node's own frame limit, taken from the node's epoch, so
    /// the requester cannot size the responder's buffers (#1718).
    pub(super) async fn process_sync_batches_stream(
        &self,
        peer: BlsPublicKey,
        mut stream: Stream,
        batch_digests: BTreeSet<B256>,
        epoch: Epoch,
        max_frame: usize,
        consensus_chain: &ConsensusChain,
    ) -> SyncServeOutcome {
        debug!(
            target: "worker::network",
            %peer,
            batch_count = batch_digests.len(),
            epoch,
            "serving inbound sync batch stream"
        );

        let store = self.consensus_config.node_storage();

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
                SEND_STREAM_FRAME_TIMEOUT,
            ),
        )
        .await
        .map_err(WorkerNetworkError::from)
        .and_then(|sent| sent);
        let outcome = SyncServeOutcome::of(&served);

        // a failed serve is logged and best-effort signalled so the requester
        // stops waiting; the caller decides on a penalty from `outcome`
        if let Err(e) = served {
            warn!(
                target: "worker::network",
                %peer,
                ?e,
                ?outcome,
                "failed to serve sync batch stream"
            );
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

        outcome
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
    /// See [Self::validate_and_cache_prefetched_batch].
    pub fn pub_validate_and_cache_prefetched_batch(
        &self,
        digest: &B256,
        batch: &Batch,
    ) -> WorkerNetworkResult<()> {
        self.validate_and_cache_prefetched_batch(digest, batch)
    }
}

#[cfg(test)]
mod prefetch_tests {
    use super::{try_admit_prefetch, MAX_CONCURRENT_GOSSIP_PREFETCHES};
    use parking_lot::Mutex;
    use std::{collections::HashSet, sync::Arc};
    use tn_types::B256;
    use tokio::sync::Semaphore;

    /// A prefetch pool sized to the production cap.
    fn new_pool() -> (Arc<Semaphore>, Arc<Mutex<HashSet<B256>>>) {
        (
            Arc::new(Semaphore::new(MAX_CONCURRENT_GOSSIP_PREFETCHES)),
            Arc::new(Mutex::new(HashSet::new())),
        )
    }

    /// A distinct batch digest (no `rand` feature dependency).
    fn digest(seed: u8) -> B256 {
        B256::from([seed; 32])
    }

    /// A second prefetch for a digest already in flight is refused, releases the
    /// permit it transiently acquired (no slot leak), and the digest becomes
    /// admissible again once the first prefetch is dropped.
    #[test]
    fn dedups_in_flight_digest() {
        let (semaphore, in_flight) = new_pool();
        let d = digest(1);

        let first = try_admit_prefetch(&semaphore, &in_flight, d).expect("first prefetch admitted");
        assert_eq!(semaphore.available_permits(), MAX_CONCURRENT_GOSSIP_PREFETCHES - 1);

        // duplicate while the first is in flight -> refused, and the transient permit
        // it briefly acquired is released, so exactly one slot stays held
        assert!(
            try_admit_prefetch(&semaphore, &in_flight, d).is_none(),
            "duplicate digest must be deduplicated"
        );
        assert_eq!(semaphore.available_permits(), MAX_CONCURRENT_GOSSIP_PREFETCHES - 1);

        // dropping the first prefetch clears the mark and frees the slot
        drop(first);
        assert_eq!(semaphore.available_permits(), MAX_CONCURRENT_GOSSIP_PREFETCHES);
        assert!(
            try_admit_prefetch(&semaphore, &in_flight, d).is_some(),
            "digest is admissible again once the prior prefetch finished"
        );
    }

    /// Concurrency is capped: distinct digests beyond the cap are shed, and slots
    /// free again once the outstanding prefetches are dropped.
    #[test]
    fn caps_concurrency_and_sheds_excess() {
        let (semaphore, in_flight) = new_pool();

        let held: Vec<_> = (0..MAX_CONCURRENT_GOSSIP_PREFETCHES)
            .map(|i| {
                try_admit_prefetch(&semaphore, &in_flight, digest(i as u8))
                    .expect("prefetch within cap admitted")
            })
            .collect();
        assert_eq!(held.len(), MAX_CONCURRENT_GOSSIP_PREFETCHES);
        assert_eq!(semaphore.available_permits(), 0);

        // a fresh distinct digest at the cap is shed rather than amplified
        assert!(
            try_admit_prefetch(&semaphore, &in_flight, digest(u8::MAX)).is_none(),
            "distinct digest beyond the cap must be shed"
        );

        // releasing the outstanding prefetches frees the slots again
        drop(held);
        assert_eq!(semaphore.available_permits(), MAX_CONCURRENT_GOSSIP_PREFETCHES);
        assert!(
            try_admit_prefetch(&semaphore, &in_flight, digest(u8::MAX)).is_some(),
            "a freed slot admits a new prefetch"
        );
    }
}
