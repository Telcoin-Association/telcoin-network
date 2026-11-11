//! The Proposer is responsible for proposing the primary's next header when certain conditions are
//! met.
//!
//! This is the first task in the primary's header cycle. The Proposer processes messages from the
//! `Primary::StateHandler` to track which proposed headers were successfully committed. If a header
//! is not committed before it's round advances, the failed header's block digests are included in a
//! fresh header in FIFO order.
//!
//! Successfully created Headers are sent to the `Primary::Certifier`, where they are reliably
//! broadcast to voting peers. Headers are stored in the `ProposerStore` before they are sent to the
//! Certifier.
//!
//! The Proposer is also responsible for processing batch's that reach quorum.
//! Collections of batches that reach quorum are included in each header. If the Proposer's
//! header fails to be committed, then block digests from the failed round are included in the next
//! header once the Proposer's round advances.

use crate::{
    consensus::LeaderSchedule,
    error::{ProposerError, ProposerResult},
    metrics::{DigestsRequeuedHandles, HeadersProposedHandles, ProposeReason, RequeueCause},
    ConsensusBus, RecentBlocks,
};
use reth_metrics::metrics::{Counter, Histogram};
use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    time::Instant,
};
use tn_config::{ConsensusConfig, KeyConfig};
use tn_storage::{tables::LastProposed, ProposerStore};
use tn_types::{
    forks::{seed_signature_active, subsecond_timestamp_active},
    now_ms, AuthorityIdentifier, BlockHash, BlockNumHash, BlockNumber, Certificate, Committee,
    Database, Epoch, EpochDigest, EpochSeedMessage, Hash as _, Header, HeaderDigest, Noticer,
    Round, TaskManager, TaskSpawner, TnReceiver, TnSender, VotingPower, WorkerId,
};
use tokio::{
    sync::oneshot,
    time::{sleep, Duration, Interval},
};
use tracing::{debug, enabled, error, info, instrument, trace, warn};

/// Type alias for the async task that creates, stores, and sends the proposer's new header.
type PendingHeaderTask = oneshot::Receiver<ProposerResult<Header>>;

/// Messages sent to the proposer about this primary's own workers' block digests
#[derive(Debug)]
pub struct OurDigestMessage {
    /// The digest for the worker's block that reached quorum.
    pub digest: BlockHash,
    /// The worker that produced this block.
    pub worker_id: WorkerId,
    /// A channel to send an () as an ack after this digest is processed by the primary.
    pub ack_channel: oneshot::Sender<()>,
}

impl OurDigestMessage {
    /// Process the message.
    ///
    /// Splits the message into components required for processing the batch.
    fn process(self) -> (oneshot::Sender<()>, ProposerDigest) {
        let OurDigestMessage { digest, worker_id, ack_channel } = self;
        let now = Instant::now();
        let digest = ProposerDigest {
            digest,
            worker_id,
            received: now,
            first_received: now,
            requeued: false,
        };
        (ack_channel, digest)
    }
}

/// The returned type for processing `[OurDigestMessage]`.
///
/// Contains all the information needed to propose the new header.
#[derive(Debug)]
struct ProposerDigest {
    /// The digest for the worker's block that reached quorum.
    pub digest: BlockHash,
    /// The worker that produced this block.
    pub worker_id: WorkerId,
    /// When the proposer queued this digest: its arrival from the worker, or its re-queue after
    /// the header that carried it was not committed. The `header proposed` line's
    /// `oldest_digest_wait_ms` measures the oldest digest's wait from here.
    pub received: Instant,
    /// When the proposer first received this digest from its worker, kept across re-queues.
    /// `tn_primary_digest_wait_seconds` and the `header proposed` line's `oldest_requeue_wait_ms`
    /// measure from here, so a digest that rode an uncommitted header still reports its whole
    /// wait. Equal to `received` until a re-queue; a re-queued digest this proposer never received
    /// (it rode a stored header re-sent after a restart) starts here at its re-queue.
    pub first_received: Instant,
    /// Whether this digest was re-queued at least once, so `received` marks its latest re-queue
    /// rather than its arrival from the worker. The `header proposed` line counts these as
    /// `requeued`, which splits headers whose digest wait may be cut short by a re-queue from the
    /// rest.
    pub requeued: bool,
}

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
mod proposer_tests;

/// The proposer creates new headers and send them to the core for broadcasting and further
/// processing.
pub(crate) struct Proposer<DB: ProposerStore> {
    /// The id of this primary.
    authority_id: AuthorityIdentifier,
    /// The committee information.
    committee: Committee,
    /// The threshold number of batches that can trigger
    /// a header creation. When there are available at least
    /// `header_num_of_batches_threshold` batches we are ok
    /// to try and propose a header
    header_num_of_batches_threshold: usize,
    /// The maximum number of batches in header.
    max_header_num_of_batches: usize,
    /// The minimum duration between generating headers.
    min_header_delay: Duration,
    /// The maximum duration to wait for conditions like having leader in parents.
    max_header_delay: Duration,
    /// The minimum interval measured between generating headers.
    min_delay_interval: Interval,
    /// The maximum interval measured for conditions like having leader in parents.
    max_delay_interval: Interval,
    /// The latest header.
    opt_latest_header: Option<Header>,
    /// Receiver for shutdown.
    ///
    /// Also used to signal committee change.
    rx_shutdown: Noticer,
    /// consensus channels
    consensus_bus: ConsensusBus,
    /// The proposer store for persisting the last header.
    proposer_store: DB,
    /// The current round of the dag.
    round: Round,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Certificate>,
    /// Holds the certificate of the last leader (if any).
    last_leader: Option<Certificate>,
    /// Holds the batches' digests waiting to be included in the next header, one FIFO queue
    /// per worker (issue #556): within a worker digests stay oldest to newest and pop from
    /// the front, and there is no total order across workers. Header slots are shared across
    /// the workers that have pending digests so one busy worker cannot starve another's
    /// lane. `BTreeMap` (not `HashMap`) so slot allocation walks the workers in a stable
    /// ascending order.
    digests: BTreeMap<WorkerId, VecDeque<ProposerDigest>>,
    /// Round-robin cursor for header slot allocation: the worker id the next header's slot
    /// walk starts from. Advanced past the last lane granted a slot, so when more workers
    /// have pending digests than a header holds, the rounds resume where the previous
    /// header stopped instead of restarting at the lowest id (which would permanently
    /// starve the high-id lanes).
    next_lane: WorkerId,
    /// Holds the map of proposed previous round headers and their digest messages, to ensure that
    /// all batches' digest included will eventually be re-sent.
    proposed_headers: BTreeMap<Round, Header>,
    /// The first receipt of each batch digest drained into an own header this proposer still
    /// holds: the header being built, or one tracked in [`Self::proposed_headers`]. A header's
    /// payload keeps no time, so a re-queue takes the digest's stamp back from here and its wait
    /// keeps counting from its first receipt. A stamp leaves when its digest is re-queued or its
    /// header commits, so this holds at most the payloads of the tracked headers and the one in
    /// flight.
    proposed_first_received: BTreeMap<BlockHash, Instant>,
    /// The consensus leader schedule to be used in order to resolve the leader needed for the
    /// protocol advancement.
    leader_schedule: LeaderSchedule,
    /// Flag if enough conditions are met to advance the round.
    advance_round: bool,
    /// Spawner for our tasks- want to confine them to the current epoch.
    task_spawner: TaskSpawner,
    /// The cross-epoch anchor of the [`EpochSeedMessage`]: the digest of the previous epoch's
    /// [`EpochRecord`](tn_types::EpochRecord), resolved once when the epoch's primary spawns.
    prior_epoch_record: EpochDigest,
    /// Handle used to sign this authority's per-`(author, round)` [`EpochSeedMessage`].
    ///
    /// The seed message binds the header's round, so it cannot be signed once for the epoch; the
    /// signer handle is carried instead and used immediately before each [`Header::new`].
    key_config: KeyConfig,
    /// The `tn_primary_headers_proposed_total{reason}` counters, resolved once per epoch.
    headers_proposed: HeadersProposedHandles,
    /// The `tn_primary_digests_requeued_total{cause}` counters, resolved once per epoch.
    digests_requeued: DigestsRequeuedHandles,
    /// Which rule names the execution block of each new header (the `quorum_exec_block`
    /// switches), and the adaptive policy's window state. Epoch-scoped with the proposer.
    exec_block_policy: ExecBlockPolicy,
    /// The `parent_grace` switch's state: who the last own header built on, and the grace armed
    /// for the round being collected, if any. Epoch-scoped with the proposer.
    parent_grace: ParentGrace,
    /// The `digest_dedupe` switch's index of the batch digests this proposer already holds, so a
    /// digest reported again is dropped instead of riding a second header (issue #1807). `None`
    /// while the switch is off. Epoch-scoped with the proposer.
    digest_dedupe: Option<DigestDedupe>,
}

/// The identity a proposer stamps on a header it authors.
///
/// These values are fixed for a given proposal and are read straight through into [`Header::new`].
/// They are grouped into one type so the spawned header-building task takes a single
/// self-describing argument instead of a long list of positional scalars.
struct HeaderIdentity {
    /// The round the header is proposed for.
    round: Round,
    /// The epoch the header is proposed in.
    epoch: Epoch,
    /// This authority's identifier, recorded as the header's author.
    author: AuthorityIdentifier,
    /// The cross-epoch anchor of this header's [`EpochSeedMessage`].
    prior_epoch_record: EpochDigest,
    /// Handle used to sign this header's [`EpochSeedMessage`].
    ///
    /// [`Proposer::propose_header`] is a free-standing task with no signer in scope, and the seed
    /// message binds the header's round, so the signer travels with the identity and the signature
    /// is produced immediately before [`Header::new`].
    key_config: KeyConfig,
}

/// Why the proposer builds a header and what its queue held at that moment, carried into
/// [`Proposer::propose_header`] for the `header proposed` line and the proposal metrics.
///
/// Observability only: nothing here changes what the header contains.
struct ProposalTrigger {
    /// The propose-condition branch that fired.
    reason: ProposeReason,
    /// Digests pending across all workers before this header took its share.
    pending_before: usize,
    /// `tn_primary_headers_proposed_total{reason}` for [`Self::reason`].
    proposed: Counter,
    /// How long the parent grace held this header, zero when no grace was armed for it.
    grace: Duration,
}

impl<DB: Database> Proposer<DB> {
    /// Create a new instance of Self.
    ///
    /// The proposer's intervals and genesis certificate are created in this function.
    /// Also set `advance_round` to true.
    ///
    /// The starting round is recovered from the `LastProposed` guard record rather than always
    /// starting at zero. `prime_consensus` restores `primary_round` from the last *committed*
    /// leader round, which trails the last *proposed* round after a crash, so a proposer that
    /// started at zero would propose fresh at `committed + 1` and only reach the last proposed
    /// round `P` again after recovery parents arrive - by which point `LastProposed` has been
    /// overwritten and the round-`P` header is rebuilt with a *different* digest (`created_at` is
    /// wall-clock and sits inside the digest preimage). Voters holding a durable `Votes` record
    /// for round `P` then reject that new digest with `AlreadyVoted` forever, because nothing
    /// converts elapsed time into progress. Seeding `round` to `P - 1` makes the next proposal
    /// land on `P`: the max-delay re-send in `Self::run` re-sends the stored round-`P` header, and
    /// parents at `P - 1` lead `Self::propose_next_header` to the same header through its
    /// repropose filter, so only the byte-identical header is ever sent - the premise the
    /// fail-closed vote guard depends on. A max-delay re-send leaves the round at `P - 1`, so the
    /// next max-delay interval is timed for round `P` instead of `P + 1`: the leader halving is one
    /// round off for one interval, which is harmless.
    ///
    /// The round never starts below `primary_round`. `prime_consensus` seeds that watch at the
    /// network head when a validator is promoted back into the committee mid-epoch, and a proposer
    /// starting below it would follow the certificate manager's replay of the store's last two
    /// rounds through the future-round branch of `Self::process_parents` and move the watch
    /// backwards. The genesis certificates are the parents of round 1 only, so they seed
    /// `last_parents` only when the round starts at 0; a proposer seeded above round 0 waits for
    /// parents of its own round (see `Self::has_parents_for_round`).
    pub(crate) fn new(
        config: ConsensusConfig<DB>,
        authority_id: AuthorityIdentifier, // We need to be a validator so must have an id.
        consensus_bus: ConsensusBus,
        leader_schedule: LeaderSchedule,
        task_spawner: TaskSpawner,
    ) -> Self {
        let rx_shutdown = config.shutdown().subscribe();
        let genesis = Certificate::genesis(config.committee());
        // one read so the recovery filter and the round floor below see the same value
        let primary_round = consensus_bus.app().primary_round();
        // create min/max delay intervals
        let min_delay_interval = tokio::time::interval(config.parameters().min_header_delay);
        let max_delay_interval = tokio::time::interval(config.parameters().max_header_delay);

        // Recover the round of the last header this authority proposed.
        //
        // A read failure is not fatal: it only costs the reproposal, so log it and fall back to
        // the pre-recovery behavior of starting at round 0. The stored header is ignored unless it
        // is from the current epoch (the epoch DB is cleared on a clean rollover, but two exit
        // paths skip that clear, so a stale record can outlive its epoch) and unless it is ahead
        // of `primary_round` (a node that already caught up past `P` via state sync must not be
        // dragged backwards - `propose_next_header` would ignore the seed anyway).
        let recovered_round = config
            .node_storage()
            .get_last_proposed()
            .inspect_err(|e| {
                error!(
                    target: "primary::proposer",
                    ?e,
                    "failed to read last proposed header - starting from round 0",
                );
            })
            .ok()
            .flatten()
            .filter(|header| header.epoch() == config.committee().epoch())
            .map(|header| header.round())
            .filter(|round| *round > primary_round)
            .map_or(0, |round| round.saturating_sub(1));
        let round = recovered_round.max(primary_round);
        // a header above round 1 built on round-0 parents is rejected by every voter
        // (`InvalidParentRound`), so genesis seeds the parents of round 1 and nothing else
        let last_parents = if round == 0 { genesis } else { Vec::new() };
        let parent_grace = ParentGrace::new(
            config.parameters().parent_grace,
            config.parameters().max_header_delay,
            consensus_bus.app().metrics().parent_grace_wait_seconds.clone(),
        );
        let digest_dedupe = config.parameters().digest_dedupe.then(|| {
            DigestDedupe::new(
                consensus_bus.app().metrics().duplicate_digest_suppressed_total.clone(),
            )
        });

        Self {
            authority_id,
            committee: config.committee().clone(),
            header_num_of_batches_threshold: config.parameters().header_num_of_batches_threshold,
            max_header_num_of_batches: config.parameters().max_header_num_of_batches,
            min_header_delay: config.parameters().min_header_delay,
            max_header_delay: config.parameters().max_header_delay,
            min_delay_interval,
            max_delay_interval,
            opt_latest_header: None,
            rx_shutdown,
            consensus_bus,
            proposer_store: config.node_storage().clone(),
            round,
            last_parents,
            last_leader: None,
            digests: BTreeMap::new(),
            next_lane: 0,
            proposed_headers: BTreeMap::new(),
            proposed_first_received: BTreeMap::new(),
            leader_schedule,
            advance_round: true,
            task_spawner,
            prior_epoch_record: config.prior_epoch_record(),
            key_config: config.key_config().clone(),
            headers_proposed: HeadersProposedHandles::new(),
            digests_requeued: DigestsRequeuedHandles::new(),
            exec_block_policy: ExecBlockPolicy::new(
                config.parameters().quorum_exec_block,
                config.parameters().quorum_exec_block_starved_rounds,
            ),
            parent_grace,
            digest_dedupe,
        }
    }

    /// Make a new header, store it in the proposer store, and forward it to the certifier.
    ///
    /// This task is spawned outside of `Self`.
    ///
    /// - current_header: caller checks to see if there is already a header built for this round. If
    ///   current_header.is_some() the proposer uses this header instead of building a new one.
    /// - exec_inputs: what [`ExecBlockInputs::choose`] needs to pick the header's execution block;
    ///   the ring is read once, right before [`Header::new`], where the tip was read before.
    #[instrument(level = "debug", skip_all, fields(round = identity.round, epoch = identity.epoch, num_digests = digests.len()))]
    async fn propose_header(
        identity: HeaderIdentity,
        proposer_store: DB,
        consensus_bus: &ConsensusBus,
        parents: Vec<Certificate>,
        digests: VecDeque<ProposerDigest>,
        trigger: ProposalTrigger,
        exec_inputs: ExecBlockInputs,
    ) -> ProposerResult<Header> {
        let HeaderIdentity {
            round: current_round,
            epoch: current_epoch,
            author,
            prior_epoch_record,
            key_config,
        } = identity;
        // voters reject a header that is not timestamped after its parents: strictly later at
        // millisecond granularity once the sub-second fork is active for this header's epoch,
        // and at-or-after in whole seconds before it. pre-fork `Header::new` drops the
        // milliseconds, so clamping to the latest parent itself reproduces the seconds rule.
        let millis_active = subsecond_timestamp_active(current_epoch);
        let min_created_at = parents
            .iter()
            .map(|c| c.header().created_at_ms())
            .max()
            .map(|latest| if millis_active { latest.saturating_add_millis(1) } else { latest });

        // a certified parent can sit ahead of this node's clock by up to the voters' drift
        // tolerance, so this is expected rather than a fault: wait out exactly the gap
        if let Some(min_created_at) = min_created_at {
            let current_time = now_ms();
            if current_time < min_created_at {
                let drift_ms = min_created_at.as_millis() - current_time.as_millis();
                debug!(
                    target: "primary::proposer",
                    %current_time,
                    %min_created_at,
                    drift_ms,
                    "latest parent not yet in the past - sleeping until it is",
                );
                sleep(Duration::from_millis(drift_ms)).await;
            }
        }

        // Sign the seed message for exactly this `(epoch, round)`. It is signed here, immediately
        // before the header is built, because the message binds the round: a signature for round
        // `r` must not exist before this authority proposes at `r`, or every future seed
        // contribution would be publicly derivable in advance.
        //
        // Signing is fork-gated (#1086): for pre-fork epochs (`seed_signature_active` false) the
        // BLS signing is skipped and the header carries the inert `BlsSignature::default()`.
        // The default is inert because `HeaderRef` never serializes the field for those epochs,
        // the header digest does not cover it, and `Header::seed_signature` surfaces it as
        // `None` - so it can never reach the wire or a verifier, and pre-fork headers stay
        // wire-identical to origin/main.
        let seed_signature = if seed_signature_active(current_epoch) {
            EpochSeedMessage::new(current_epoch, current_round, prior_epoch_record)
                .sign(&key_config)
        } else {
            Default::default()
        };

        // clamped instead of trusting the sleep alone: the sleep runs on tokio's monotonic clock
        // while `now_ms` reads the wall clock, and the two can disagree (e.g. after a clock step)
        let created_at = now_ms().max(min_created_at.unwrap_or_default());

        // synchronous: the ring's read guard is released before the next await
        let exec = exec_inputs.choose(consensus_bus);

        let header = Header::new(
            author,
            current_round,
            current_epoch,
            digests.iter().map(|m| (m.digest, m.worker_id)).collect(),
            parents.iter().map(|x| x.header().digest()).collect(),
            exec.block,
            seed_signature,
            created_at,
        );

        // a digest's wait ends here, once the header carrying it exists, and one clock read keeps
        // the waits below comparable; a header without digests has no wait to report and records
        // no sample
        let now = Instant::now();
        let wait_since = move |stamp: Instant| now.saturating_duration_since(stamp);
        // since the oldest digest was last queued, restarted by a re-queue
        let oldest_digest_wait = digests.iter().map(|d| d.received).min().map(wait_since);
        // since the oldest digest first arrived, across re-queues: the histogram's sample, which
        // equals the larger of the two waits the line reports
        let oldest_first_wait = digests.iter().map(|d| d.first_received).min().map(wait_since);
        if let Some(wait) = oldest_first_wait {
            consensus_bus.app().metrics().digest_wait_seconds.record(wait.as_secs_f64());
        }
        // the same over only the re-queued digests, zero when the header carries none
        let oldest_requeue_wait = digests
            .iter()
            .filter(|d| d.requeued)
            .map(|d| d.first_received)
            .min()
            .map_or(Duration::ZERO, wait_since);

        // Metric: header_proposed - tracks header proposals
        info!(
            target: "consensus::metrics",
            round = current_round,
            epoch = current_epoch,
            num_batches = digests.len(),
            num_parents = parents.len(),
            created_at_ms = header.created_at_ms().as_millis(),
            reason = trigger.reason.as_str(),
            digests = %HeaderDigests(&digests),
            oldest_digest_wait_ms = oldest_digest_wait.map_or(0, duration_ms),
            requeued = digests.iter().filter(|d| d.requeued).count(),
            oldest_requeue_wait_ms = duration_ms(oldest_requeue_wait),
            pending_before = trigger.pending_before,
            grace_ms = duration_ms(trigger.grace),
            exec_block = exec.block.number,
            exec_tip = exec.tip.number,
            exec_basis = exec.basis.as_str(),
            exec_policy = exec_inputs.policy.as_str(),
            "header proposed"
        );
        trigger.proposed.increment(1);

        if enabled!(target: "primary::proposer", tracing::Level::TRACE) {
            let mut msg = format!("Created header {header:?} with parent certificates:\n");
            for parent in parents.iter() {
                msg.push_str(&format!("{parent:?}\n"));
            }
            trace!(target: "primary::proposer", ?header, ?msg, "created new header");
        } else {
            debug!(target: "primary::proposer", ?header, parents=?header.parents(), "created new header");
        }

        // store and send newly built header
        Proposer::store_and_send_header(&header, proposer_store, consensus_bus).await?;

        Ok(header)
    }

    /// Bypass creating another header and return header.
    ///
    /// This is a convenience method to help the flow of proposing new headers and reproposing
    /// headers. Headers are reproposed under certain conditions:
    /// - during a restart when the last proposed header in Self::proposer_store is from the current
    ///   round.
    /// -
    async fn repropose_header(
        header: Header,
        proposer_store: DB,
        consensus_bus: &ConsensusBus,
    ) -> ProposerResult<Header> {
        Proposer::store_and_send_header(&header, proposer_store, consensus_bus).await?;

        Ok(header)
    }

    /// Store the header in the `ProposerStore` and send to `Certifier`.
    ///
    /// `LastProposed` is a single-slot record (its key is the constant `LAST_PROPOSAL_KEY`), so
    /// every write replaces the previous one. It is only overwritten by a header that supersedes
    /// the stored one, so a transient proposal at a *lower* round cannot erase the guard record
    /// for a higher round that this authority has already broadcast. Losing that record is what
    /// lets a restart rebuild a round-`P` header with a fresh digest and deadlock against voters
    /// holding a durable `Votes` record for `P`.
    ///
    /// The comparison is epoch-aware. Rounds restart per epoch, so a round-only test would refuse
    /// every write in a new epoch for as long as a higher-round record from the previous epoch
    /// survived - which is reachable, because the epoch-close table clear is both conditional and
    /// not durability-barriered. That would silently disable the anti-equivocation guard for a
    /// whole epoch, so a header from a different epoch always supersedes.
    ///
    /// The round test is `>=`, not `>`, and the equality case is load-bearing: reproposing the
    /// identical header for the *same* round must still take the write branch, because that is what
    /// keeps the reproposal behind the durability barrier below. After any failed epoch-DB commit
    /// that barrier fails, so a reproposal fail-stops the proposer before its header reaches the
    /// certifier. Narrowing this to `>` would let a reproposal skip the barrier and reach the
    /// certifier on an epoch DB that can no longer make records durable.
    ///
    /// The barrier does not make the certifier's `ProposedCertificates` record durable. `persist`
    /// queues its barrier when called and the runner acks in FIFO order, so the barrier covers only
    /// the writes queued before it, and the certifier can insert that record after this barrier is
    /// queued. The certifier's already-certified check therefore awaits its own
    /// `ProposedCertificates` barrier before it re-gossips a stored certificate (issue #1530).
    async fn store_and_send_header(
        header: &Header,
        proposer_store: DB,
        consensus_bus: &ConsensusBus,
    ) -> ProposerResult<()> {
        // A read error is fatal here: failing it open would fall through to the write and clobber
        // the very record this guard exists to preserve.
        let read_start = Instant::now();
        let stored = proposer_store
            .get_last_proposed()
            .map_err(|e| ProposerError::StoreError(e.to_string()))?;
        let read = read_start.elapsed();
        let supersedes_stored = stored
            .is_none_or(|last| last.epoch() != header.epoch() || header.round() >= last.round());

        // a header that does not supersede the stored one skips the write and the barrier, so
        // both phases read zero for it
        let (write, persist) = if supersedes_stored {
            // Store the last header.
            let write_start = Instant::now();
            proposer_store
                .write_last_proposed(header)
                .map_err(|e| ProposerError::StoreError(e.to_string()))?;
            let write = write_start.elapsed();

            // Wait for the `LastProposed` record to be durable before broadcasting. The epoch DB
            // persists asynchronously, so `write_last_proposed` returns before the record hits
            // disk; broadcasting first would let a crash in that window lose the record. On
            // restart the anti-equivocation guard would then read nothing and build a *different*
            // header for this same round while the pre-crash header is already on the network:
            // equivocation from an ordinary crash. See issue #934.
            //
            // If the barrier reports a failed commit (disk full, `EIO`, checksum), the record is
            // not on disk, so broadcasting would risk that same self-inflicted equivocation.
            // Refuse to broadcast and return a fatal error instead: the proposer runs as a
            // critical task, so returning here stops the node rather than continuing on a lost
            // guard record (issue #975). When another task's failed barrier has already ended the
            // epoch, `run_epoch` stops the node instead, finding the store unable to write.
            let persist_start = Instant::now();
            proposer_store
                .persist::<LastProposed>()
                .await
                .map_err(|e| ProposerError::DurableBarrierFailed(e.to_string()))?;
            (write, persist_start.elapsed())
        } else {
            (Duration::ZERO, Duration::ZERO)
        };

        // Send the new header to the `Certifier` that will broadcast and certify it.
        let send_start = Instant::now();
        consensus_bus.headers().send(header.clone()).await.map_err(Box::new)?;
        debug!(
            target: "primary::proposer",
            round = header.round(),
            epoch = header.epoch(),
            stored = supersedes_stored,
            read_ms = duration_ms(read),
            write_ms = duration_ms(write),
            persist_ms = duration_ms(persist),
            send_ms = duration_ms(send_start.elapsed()),
            "header stored and sent"
        );
        Ok(())
    }

    /// Calculate the max delay to use when resetting the max_delay_interval.
    ///
    /// The max delay is reduced when this authority expects to become the leader of the next round.
    /// Reducing the max delay increases its chance of being included in the DAG. Leaders are only
    /// elected on even rounds, so the normal max delay interval is used for odd rounds.
    fn calc_max_delay(&self) -> Duration {
        // check next round
        let next_round = self.round + 1;

        if next_round.is_multiple_of(2)
            && self.leader_schedule.leader(self.round + 1).id() == self.authority_id
        {
            self.max_header_delay / 2
        } else {
            self.max_header_delay
        }
    }

    /// Calculate the min delay to use when resetting the min_delay_interval.
    ///
    /// The min delay is reduced when this authority expects to become the leader of the next round.
    /// Reducing the min delay increases the chances of successfully committing a leader.
    ///
    /// Leaders are only elected on even rounds. For even rounds, zero delay when this node is the
    /// anticipated leader increases commit chance. For odd rounds, use min_header_delay so the
    /// certifier has time to certify the previous (possibly batch-bearing) even round header
    /// before it gets canceled by the next proposal.
    fn calc_min_delay(&self) -> Duration {
        let next_round = self.round + 1;

        if next_round.is_multiple_of(2)
            && self.leader_schedule.leader(next_round).id() == self.authority_id
        {
            Duration::ZERO
        } else {
            self.min_header_delay
        }
    }

    /// Update the last leader certificate.
    ///
    /// This is called after processing parent certificates during even rounds.
    /// The returned boolean indicates if `Self::last_leader` was updated.
    fn update_leader(&mut self) -> bool {
        let leader = self.leader_schedule.leader(self.round);
        self.last_leader =
            self.last_parents.iter().find(|cert| cert.origin() == &leader.id()).cloned();

        debug!(target: "primary::proposer", leader=?self.last_leader, round=self.round, "Last leader for round?");

        self.last_leader.is_some()
    }

    /// Check if proposer has received enough votes to elect a new leader for the round.
    ///
    /// This method returns true for any of the following:
    /// - if this primary is the leader for the next round
    /// - f+1 votes for a new leader
    /// - 2f+1 nodes didn't vote for a new leader
    /// - there is no leader to vote for
    ///
    /// This is called after processing parent certificates during odd rounds.
    fn enough_votes(&self) -> bool {
        if self.leader_schedule.leader(self.round + 1).id() == self.authority_id {
            debug!(target: "primary::proposer", "enough_votes eval to true - this node anticipated leader for next round");
            return true;
        }

        let leader = match &self.last_leader {
            Some(x) => x.digest(),
            None => return true,
        };

        let mut votes_for_leader = 0;
        let mut no_votes = 0;
        for certificate in &self.last_parents {
            let stake = self.committee.voting_power_by_id(certificate.origin());
            if certificate.header().parents().contains(&leader) {
                votes_for_leader += stake;
            } else {
                no_votes += stake;
            }
        }

        // return true if either:
        // - enough votes for availability (f+1)
        // - a quorum of no_votes (2f+1)
        votes_for_leader >= self.committee.validity_threshold()
            || no_votes >= self.committee.quorum_threshold()
    }

    /// Check if conditions support advancing the round for the DAG.
    ///
    /// Odd rounds check if there are enough votes for a new leader.
    /// Even rounds check if there is the new leader certificate is in `Self::last_parents`.
    ///
    /// This method is called from `Self::process_parents`.
    /// NOTE: this value is ignored if max_delay_interval expires.
    fn ready(&mut self) -> bool {
        match self.round % 2 {
            0 => self.update_leader(),
            _ => self.enough_votes(),
        }
    }

    /// Whether `last_parents` holds parents for the round the next header builds on.
    ///
    /// `Self::propose_next_header` proposes at `max(self.round, primary_round) + 1`, so the parents
    /// must be certificates of `max(self.round, primary_round)`. The proposer is the only writer of
    /// `primary_round` while it runs (`reset_for_epoch` and `prime_consensus` write it before
    /// `Self::new`, and the follower path that also writes it never runs beside a proposer), and
    /// `Self::new` starts at or above it, so that term is defensive and the target is `self.round`
    /// in practice. `last_parents` only ever holds one round (`Self::process_parents` replaces it
    /// on a future round and extends it on the current one, a proposal takes it and a re-proposal
    /// clears it), so the first parent's round is the round of the whole set. A non-empty set of
    /// another round would build a header that voters reject with `InvalidParentRound`.
    ///
    /// The guard checks the round, not the voting weight. The certificate aggregator sends a
    /// round's certificates once they reach a quorum and never resets that round's weight, so every
    /// later certificate of the round arrives alone. A lone certificate is safe only because it
    /// lands on a set already at quorum: the aggregator is spawned with the proposer, so the first
    /// message of a round is its quorum, and none of the three places that empty `last_parents`
    /// leaves it empty at a round whose quorum message the proposer has already accepted. A
    /// proposal takes it and a re-proposal clears it inside `Self::propose_next_header`, after it
    /// has moved `self.round` past the set's round, so a later certificate of that round is older
    /// than the proposer's round and ignored. The `Greater` arm of `Self::process_parents` replaces
    /// it with the empty set of the certificate validator's empty-parents jump, which moves
    /// `self.round` onto the jump's round rather than past it. That is safe for another reason: a
    /// message of that round the proposer had already accepted would have raised `self.round` to
    /// it, so the jump would not take the `Greater` arm, and the round's quorum message is still
    /// ahead. Leaving `last_parents` empty at a round whose quorum message was already accepted
    /// would let one lone certificate pass this guard and build a header with fewer than a quorum
    /// of parents, which voters reject as inquorate.
    ///
    /// - Epoch start: `reset_for_epoch` and `prime_consensus` leave `primary_round` at 0 and the
    ///   epoch close cleared `LastProposed`, so the proposer starts at round 0 with the genesis
    ///   certificates (round 0) and proposes round 1 at once, as before.
    /// - Promotion mid-epoch: `prime_consensus` seeds `primary_round` at the network head `N` and
    ///   the proposer starts at `N` with no parents. The certificate validator's empty-parents jump
    ///   moves the round to the network head, and the first quorum of parents at a round `r` at or
    ///   above the proposer's round makes it propose `r + 1`. It never proposes `N + 1` on round-0
    ///   parents.
    /// - Restart with a stored round-`P` header: the proposer starts at `P - 1` with no parents,
    ///   the max-delay tick re-sends the stored header, and parents at `P - 1` lead to the same
    ///   header through the repropose filter.
    ///
    /// Proposals resume on any quorum of parents at or above the proposer's round, the same
    /// liveness condition as after any round jump.
    fn has_parents_for_round(&self) -> bool {
        let parents_round = self.round.max(self.consensus_bus.app().primary_round());
        self.last_parents.first().is_some_and(|parent| parent.round() == parents_round)
    }

    /// Process certificates received for this round.
    ///
    /// If the certificates are valid, include them as parents for the next header.
    ///
    /// Parents from another epoch are dropped before the round is compared: rounds restart every
    /// epoch, so a stale certificate's round says nothing about this epoch's progress. Such parents
    /// reach the proposer when the certificate store still holds a previous epoch's rows on
    /// startup (the aggregator is replayed from it). Following them would jump the round past this
    /// epoch's progress and propose a header whose parents every voter rejects.
    fn process_parents(&mut self, parents: Vec<Certificate>, round: Round) -> ProposerResult<()> {
        // Sanity check: verify provided certs are of the correct round.
        for parent in parents.iter() {
            if parent.round() != round {
                error!(target: "primary::proposer", "received certificate {parent:?} that failed to match expected round {round}. This should not be possible.");
            }
        }

        let epoch = self.committee.epoch();
        let received = parents.len();
        let parents: Vec<_> =
            parents.into_iter().filter(|parent| parent.epoch() == epoch).collect();
        if parents.len() < received {
            warn!(
                target: "primary::proposer",
                authority=?self.authority_id,
                epoch,
                round=self.round,
                parent_round=round,
                ignored=received - parents.len(),
                kept=parents.len(),
                "ignoring parent certificates from another epoch",
            );
            // the aggregator counted the dropped certificates toward its quorum, so what is left
            // only advances the round if it still reaches quorum on its own
            let weight: VotingPower = parents
                .iter()
                .map(|parent| self.committee.voting_power_by_id(parent.origin()))
                .sum();
            if parents.is_empty() || (round > self.round && !self.committee.reached_quorum(weight))
            {
                return Ok(());
            }
        }

        // Compare the parents' round number with our current round.
        match round.cmp(&self.round) {
            Ordering::Greater => {
                trace!(
                    target: "primary::proposer",
                    authority=?self.authority_id,
                    round=?self.round,
                    parent_round=?round,
                    "processing parents from future round - advacing to catch up...",
                );
                // proposer accepts a future round then jumps ahead in case it was
                // late (or just joined the network).
                self.round = round;
                // broadcast new round
                self.consensus_bus.app().primary_round_updates().send_replace(self.round);
                self.last_parents = parents;
                // Reset advance flag.
                self.advance_round = false;
                // NOTE: min_delay_interval is marked as `ready()` but max_delay_interval is reset
                // to wait the appropriate amount of time for the previous round's
                // leader.
                //
                // Disabling min_delay_interval will expidite the next proposal attempt. It's
                // important to propose next header ASAP so this node doesn't fall
                // behind again. If proposer waits another min_header_delay after
                // receiving parents from a future round, it's likely that more
                // parents from another future round will arrive while this node
                // tries to catch up.
                //
                // Disabling min_delay_interval should help node sync with quorum.
                // This is also important if this node expects to become the leader for the next
                // round.
                self.max_delay_interval.reset_after(self.calc_max_delay());
                self.min_delay_interval.reset_immediately();
            }
            Ordering::Less => {
                trace!(
                    target: "primary::proposer",
                    authority=?self.authority_id,
                    round=?self.round,
                    parent_round=?round,
                    "ignoring older parents",
                );
                // Ignore parents from older rounds.
                self.parent_grace.count_late_parents(round, self.round, parents.len());
            }
            Ordering::Equal => {
                trace!(
                    target: "primary::proposer",
                    authority=?self.authority_id,
                    round=?self.round,
                    parent_round=?round,
                    "adding parents for current round",
                );
                // certs arrive from synchronizer once quorum is reached
                // so these are extra parents
                self.last_parents.extend(parents);
                // the schedule can change after an odd round proposal
                //
                // need to ensure the interval is reset correctly for the round leader
                // no harm doing this here as well
                if self.calc_min_delay().is_zero() {
                    // min_delay_interval is ready
                    self.min_delay_interval.reset_immediately();
                }
            }
        }

        // check conditions for advancing the round
        //
        // if max_delay_interval expires, this check is ignored and the round is advanced regardless
        trace!(target: "primary::proposer", authority=?self.authority_id, advance_round=self.advance_round, round=self.round, "checking if self.ready()...");
        self.advance_round = self.ready();
        debug!(target: "primary::proposer", authority=?self.authority_id, advance_round=self.advance_round, round=self.round, "parents");

        Ok(())
    }

    /// Process notifications that Proposer's own headers have been committed in the DAG for a
    /// particular round.
    ///
    /// Committed headers are removed from the collection of `self.proposed_headers`. Headers
    /// that are skipped with no hope of being committed (proposed in a previous round) are also
    /// removed after adding the expired header's proposed block digests and system messages to
    /// the beginning of the queue.
    ///
    /// This method ensures batches that were previously proposed but weren't committed are
    /// added back to the queue so their transactions are included in the next proposal.
    fn process_committed_headers(&mut self, commit_round: Round, committed_headers: Vec<Round>) {
        // remove committed headers from pending
        let mut max_committed_round = 0;
        for round in committed_headers {
            max_committed_round = max_committed_round.max(round);
            // try to remove round - log warning if round is missing
            match self.proposed_headers.remove(&round) {
                // a committed digest is done waiting, so its first receipt is no longer needed
                Some(header) => header.payload().keys().for_each(|digest| {
                    self.proposed_first_received.remove(digest);
                }),
                None => warn!(
                    "own committed header not found at round {round}, probably because of restarts."
                ),
            }
        }

        // re-insert batches for any proposed header from a round below the current commit
        //
        // ensure batches stay FIFO within each worker to re-send them
        //
        // payloads: oldest -> newest, partitioned per worker (issue #556)
        let mut digests_to_resend: BTreeMap<WorkerId, VecDeque<ProposerDigest>> = BTreeMap::new();
        // Oldest to newest rounds.
        let mut retransmit_rounds = Vec::new();
        // a re-queued digest's wait since it was last queued restarts here; its first receipt
        // comes back from `proposed_first_received`
        let requeued_at = Instant::now();

        // walk proposed headers in order by round, up to the committed round
        self.proposed_headers
            .iter()
            .take_while(|(header_round, _)| **header_round <= max_committed_round)
            .for_each(|(header_round, header)| {
                // add payloads from oldest to newest into each worker's lane
                push_payload(
                    &mut digests_to_resend,
                    header,
                    requeued_at,
                    &mut self.proposed_first_received,
                );
                retransmit_rounds.push(*header_round);
            });

        // process rounds that need to be retransmitted
        if !retransmit_rounds.is_empty() {
            let num_digests_to_resend = self.prepend_requeued(
                digests_to_resend,
                retransmit_rounds.first().copied().unwrap_or_default(),
                RequeueCause::Committed,
            );

            // remove the old headers that failed
            // the proposed blocks are included in the next header
            retransmit_rounds.iter().for_each(|round| {
                self.proposed_headers.remove(round);
            });

            warn!(
                target: "primary::proposer",
                "Repropose {num_digests_to_resend} batches in undelivered headers {retransmit_rounds:?} at commit round {commit_round:?}, remaining headers {}",
                self.proposed_headers.len()
            );
        }
    }

    /// Process the certifier's report that the own header at `round` with `digest` will never be
    /// certified: a newer own header cancelled its vote collection.
    ///
    /// The header's batch digests go back to the front of their workers' queues, as the commit
    /// path re-queues an uncommitted header's, so they ride the next header instead of waiting
    /// for the next own commit. The header itself is never signed or sent again; only its digests
    /// move, and the header leaves `proposed_headers` so the commit path cannot re-queue them a
    /// second time.
    ///
    /// The report is acted on only for the exact header still tracked at `round`, and only below
    /// the proposer's round: the max-delay re-send needs a header at or above the round and the
    /// restart re-propose one at the round just advanced to, so a header below it is never sent to
    /// the certifier again and cannot be certified after its digests moved. Anything else (the
    /// commit path already re-queued it, or an unexpected report) is ignored, and the digests
    /// stay where they are.
    ///
    /// Each report prepends on its own, so the two re-queue paths order rounds differently. When
    /// two reports are handled before the next header is built (the certifier's cancel lags the
    /// proposer by a round, or the round jumps), the second report's digests land ahead of the
    /// first's, and the certifier reports in round order, so the newer header's batches go first.
    /// The commit path re-queues every expired header in one call and keeps them oldest first
    /// across rounds. With `max_header_num_of_batches` capping a header, the older batches here
    /// can wait one header longer. This is rare, and #1807 covers what out-of-order carriage
    /// does to transaction nonces.
    ///
    /// Every report, acted on here or not, also reaches [`ExecBlockPolicy::on_cancelled`]: under
    /// the adaptive `quorum_exec_block` policy a cancelled own header opens a window of rounds in
    /// which the next headers name a block a quorum already holds.
    fn process_uncertified_header(&mut self, round: Round, digest: HeaderDigest) {
        // every cancel report is evidence for the execution-block policy, whatever happens to the
        // header's digests below
        self.exec_block_policy.on_cancelled(round, self.round);
        let mut requeued = BTreeMap::new();
        match self.proposed_headers.get(&round) {
            Some(header) if round < self.round && header.digest() == digest => {
                push_payload(
                    &mut requeued,
                    header,
                    Instant::now(),
                    &mut self.proposed_first_received,
                );
            }
            _ => {
                debug!(
                    target: "primary::proposer",
                    round,
                    %digest,
                    proposer_round = self.round,
                    "uncertified own header is not tracked below the round; nothing to re-queue"
                );
                return;
            }
        }
        self.proposed_headers.remove(&round);
        self.prepend_requeued(requeued, round, RequeueCause::Canceled);
    }

    /// Prepend `requeued`, the batch digests of own headers that will not be committed (oldest
    /// first within each worker), to each worker's pending queue, and return how many there are.
    ///
    /// A non-empty re-queue logs one `digests resent` line and counts the digests in
    /// `tn_primary_digests_requeued_total{cause}`. `from_round` is the round of the oldest header
    /// the digests came from.
    fn prepend_requeued(
        &mut self,
        mut requeued: BTreeMap<WorkerId, VecDeque<ProposerDigest>>,
        from_round: Round,
        cause: RequeueCause,
    ) -> usize {
        let count: usize = requeued.values().map(VecDeque::len).sum();
        // a header without digests leaves nothing to re-send
        if count == 0 {
            return 0;
        }
        info!(
            target: "consensus::metrics",
            count,
            from_round,
            round = self.round,
            cause = cause.as_str(),
            "digests resent"
        );
        self.digests_requeued.counter(cause).increment(u64::try_from(count).unwrap_or(u64::MAX));

        // a re-queue moves digests out of a header that is no longer tracked, so they enter the
        // pending index without a duplicate check
        if let Some(dedupe) = self.digest_dedupe.as_mut() {
            requeued.values().flatten().for_each(|requeued| dedupe.queued(requeued.digest));
        }

        // prepend the re-queued digests to each worker's pending queue and update `self`
        std::mem::take(&mut self.digests).into_iter().for_each(|(worker_id, mut pending)| {
            requeued.entry(worker_id).or_default().append(&mut pending);
        });
        self.digests = requeued;
        count
    }

    /// Total number of digests pending across all workers.
    ///
    /// The `enough_digests` header trigger counts the total, not any single worker's queue.
    fn pending_digests_len(&self) -> usize {
        self.digests.values().map(VecDeque::len).sum()
    }

    /// Select up to `max_header_num_of_batches` digests for the next header.
    ///
    /// Header slots are distributed uniformly across the workers with pending digests: rounds
    /// of one digest per worker in ascending worker-id order, FIFO within each worker, until
    /// the cap is reached or every queue is empty. A worker's unused share flows to the
    /// workers that still have digests (issue #556). The walk starts at [`Self::next_lane`]
    /// and the cursor advances past the last lane served, so slots rotate across headers
    /// and no lane is excluded permanently when more workers have digests than a header
    /// holds. With one worker this is exactly the old single-queue FIFO drain.
    fn drain_digests_for_header(&mut self) -> VecDeque<ProposerDigest> {
        let queues = &self.digests;
        let max_depth = queues.values().map(VecDeque::len).max().unwrap_or(0);
        // Ascending worker ids, rotated to start at the cursor.
        let lanes: Vec<WorkerId> = queues
            .range(self.next_lane..)
            .chain(queues.range(..self.next_lane))
            .map(|(id, _)| *id)
            .collect();
        // One entry per (round, worker) pair with a digest at that queue depth, in slot
        // order; each worker id appears exactly its queue length before the cap applies.
        let order: Vec<WorkerId> = (0..max_depth)
            .flat_map(|depth| {
                lanes
                    .iter()
                    .copied()
                    .filter(move |id| queues.get(id).is_some_and(|queue| queue.len() > depth))
            })
            .take(self.max_header_num_of_batches)
            .collect();
        self.next_lane = order.last().map_or(self.next_lane, |last| last.wrapping_add(1));
        let drained: VecDeque<ProposerDigest> = order
            .into_iter()
            .filter_map(|worker_id| self.digests.get_mut(&worker_id).and_then(VecDeque::pop_front))
            .collect();
        if let Some(dedupe) = self.digest_dedupe.as_mut() {
            dedupe.drained(drained.iter().map(|drained| drained.digest));
        }
        // the header's payload keeps no time, so each digest's first receipt waits here until the
        // digest commits or comes back in a re-queue
        drained.iter().for_each(|drained| {
            self.proposed_first_received
                .entry(drained.digest)
                .and_modify(|first| *first = (*first).min(drained.first_received))
                .or_insert(drained.first_received);
        });
        drained
    }

    /// Queue `digest`, just reported by its worker, for a future header.
    ///
    /// With `digest_dedupe` off the digest is always queued, as before the switch existed. With it
    /// on, a digest this proposer already holds is dropped instead: one pending in any worker's
    /// lane, or one in the payload of the own header being built or of an own header still tracked
    /// in [`Self::proposed_headers`] (proposed, and neither committed nor re-queued yet). Each drop
    /// is counted in `tn_primary_duplicate_digest_suppressed_total` and logged once at debug.
    ///
    /// Not deduplicated, on purpose:
    /// - a digest whose own header was committed: the commit removed that header from the tracked
    ///   headers, so a later report queues it again. Its transactions may already have executed;
    ///   the worker's own check (the worker half of this switch) and execution's nonce checks
    ///   handle that copy, and it can be the copy that lands transactions the first one skipped.
    /// - the digests of an own header that was cancelled or fell behind a commit: both re-queue
    ///   paths remove the header from the tracked headers before they prepend its digests, so the
    ///   digests move to the pending lanes instead of being copied.
    /// - a digest reported before a re-send of the stored header that carries it (after a restart,
    ///   or by the max-delay tick): that header enters the tracked headers again without passing
    ///   through here.
    ///
    /// The caller acks the report before this runs, whatever happens to the digest.
    fn queue_digest(&mut self, digest: ProposerDigest) {
        if let Some(dedupe) = self.digest_dedupe.as_mut() {
            if let Some(held) = dedupe.held(&digest.digest, &self.proposed_headers) {
                dedupe.suppressed.increment(1);
                debug!(
                    target: "primary::proposer",
                    digest = %digest.digest,
                    worker_id = digest.worker_id,
                    "where" = held.as_str(),
                    "batch digest already queued"
                );
                return;
            }
            dedupe.queued(digest.digest);
        }
        self.digests.entry(digest.worker_id).or_default().push_back(digest);
    }

    /// Conditions are met to propose the next header.
    ///
    /// This method ensures proposer is protected against equivocation and sends the next header to
    /// the Certifier.
    ///
    /// If a different header was already produced for the same round, then
    /// this method returns the earlier header. Otherwise the newly created header is returned.
    ///
    /// `reason` is the propose-condition branch that fired; a new header reports it, a re-sent
    /// header ignores it.
    fn propose_next_header(&mut self, reason: ProposeReason) -> ProposerResult<PendingHeaderTask> {
        // Advance to the next round.
        self.round += 1;
        let updated_round = self.consensus_bus.app().primary_round() + 1;
        if updated_round > self.round {
            self.round = updated_round;
        }
        self.consensus_bus.app().primary_round_updates().send_replace(self.round);

        debug!(target: "primary::proposer", authority=?self.authority_id, round=self.round, "advanced round - proposing next block...");

        // oneshot channel to spawn a task
        let (tx, rx) = oneshot::channel();
        let current_epoch = self.committee.epoch();
        let current_round = self.round;

        // check if proposer store's last header is from this round
        let last_proposed = self
            .proposer_store
            .get_last_proposed()
            .map_err(|e| ProposerError::StoreError(e.to_string()))?;
        let possible_header_to_repropose =
            last_proposed.filter(|h| h.round() == current_round && h.epoch() == current_epoch);
        let proposer_store = self.proposer_store.clone();

        match possible_header_to_repropose {
            // resend header
            Some(header) => {
                let app = self.consensus_bus.app();
                // copy out of the watch so its read guard is released before the log write
                let newest_round = app
                    .last_consensus_header()
                    .borrow()
                    .as_ref()
                    .map_or(0, |newest| newest.sub_dag.leader_round());
                warn!(
                    target: "primary::proposer",
                    authority=?self.authority_id,
                    current_round,
                    current_epoch,
                    header_round = header.round(),
                    // the round watch before this proposal advanced it to the header round
                    primary_round = updated_round.saturating_sub(1),
                    committed_round = app.committed_round(),
                    newest_round,
                    header=?header,
                    "reproposing header"
                );
                // clear parents if reproposing after restart
                self.last_parents.clear();

                let consensus_bus = self.consensus_bus.clone();
                self.task_spawner.spawn_task("re-propose header", async move {
                    // use this instead of store_and_send to because rx always expects a Header
                    let res =
                        Proposer::repropose_header(header, proposer_store, &consensus_bus).await;
                    let _ = tx.send(res);
                    Ok(())
                });
            }
            // create new header
            None => {
                // collect values from &mut self for this header
                let trigger = ProposalTrigger {
                    reason,
                    pending_before: self.pending_digests_len(),
                    proposed: self.headers_proposed.counter(reason),
                    grace: self.parent_grace.in_flight_wait(),
                };
                let digests = self.drain_digests_for_header();
                let parents = std::mem::take(&mut self.last_parents);
                let authority_id = self.authority_id.clone();
                let prior_epoch_record = self.prior_epoch_record;
                let key_config = self.key_config.clone();
                // an own certificate among the parents is the previous own header, certified
                if let Some(own) = parents
                    .iter()
                    .find(|parent| parent.round() > 0 && parent.origin() == &authority_id)
                {
                    self.exec_block_policy.on_certified(own.round());
                }
                let exec_inputs = ExecBlockInputs::new(
                    self.exec_block_policy.for_round(current_round),
                    &self.committee,
                    &authority_id,
                    &parents,
                );

                let consensus_bus = self.consensus_bus.clone();
                // spawn tokio task to create, store, and send new header to certifier
                self.task_spawner.spawn_task("propose header", async move {
                    let identity = HeaderIdentity {
                        round: current_round,
                        epoch: current_epoch,
                        author: authority_id,
                        prior_epoch_record,
                        key_config,
                    };
                    let proposal = Proposer::propose_header(
                        identity,
                        proposer_store,
                        &consensus_bus,
                        parents,
                        digests,
                        trigger,
                        exec_inputs,
                    )
                    .await;

                    let _ = tx.send(proposal);
                    Ok(())
                });
            }
        }

        // return receiver to advance task
        Ok(rx)
    }

    /// Process the result from proposing the header.
    ///
    /// The oneshot channel is ready, indicating a result from the header proposal process. Update
    /// `self` to track latest header, reset the header timeout, min/max delay intervals, insert the
    /// proposed header, and indicate round should not be advanced yet.
    fn handle_proposal_result(&mut self, result: ProposerResult<Header>) -> ProposerResult<()> {
        // receive result from oneshot channel
        let header = result?;

        // track latest header
        self.opt_latest_header = Some(header.clone());
        // Reset advance flag.
        self.advance_round = false;
        // reschedule intervals
        self.min_delay_interval.reset_after(self.calc_min_delay());
        self.max_delay_interval.reset_after(self.calc_max_delay());
        // the parents this header actually carries are who the next round's grace waits for
        self.parent_grace.on_proposed(&header);
        // track header so proposer can repropose the digests and system messages
        // if this header fails to be committed for some reason
        self.proposed_headers.insert(header.round(), header);
        // the digests drained for this header are now found in its tracked payload
        if let Some(dedupe) = self.digest_dedupe.as_mut() {
            dedupe.landed();
        }

        Ok(())
    }

    pub(crate) fn spawn(mut self, task_manager: &TaskManager) {
        if self.consensus_bus.is_active_cvv() {
            // Subscribe before spawning so channels are active before any messages are sent.
            let rx_our_digests = self.consensus_bus.subscribe_our_digests();
            let rx_parents = self.consensus_bus.subscribe_parents();
            let rx_committed_own_headers = self.consensus_bus.subscribe_committed_own_headers();
            let rx_uncertified_own_headers = self.consensus_bus.subscribe_uncertified_own_headers();
            task_manager.spawn_critical_task("proposer task", async move {
                info!(target: "primary::proposer", "Starting proposer");
                Ok(self
                    .run(
                        rx_our_digests,
                        rx_parents,
                        rx_committed_own_headers,
                        rx_uncertified_own_headers,
                    )
                    .await?)
            });
        }
        // If not an active CVV then don't propose anything.
    }

    /// The stored `LastProposed` header the max-delay tick may re-send, if any.
    ///
    /// Only a header of this epoch at or above the proposer's round qualifies. A restart starts
    /// the round at `P - 1` below the stored round-`P` header, so requiring the round to match
    /// would drop the restart re-send. A header below the round was left behind by a jump to the
    /// network head (a promotion, or the certificate validator's empty-parents jump): the proposer
    /// has passed its round, so a re-send cannot help. The record is never deleted here: it stays
    /// the equivocation guard for its round until the epoch close clears it.
    ///
    /// A store read error is logged and skips this re-send; the next proposal reads the record
    /// again in `Self::propose_next_header`, which fails on the same error.
    fn resendable_last_proposed(&self) -> Option<Header> {
        let current_epoch = self.committee.epoch();
        self.proposer_store
            .get_last_proposed()
            .inspect_err(|err| {
                warn!(
                    target: "primary::proposer",
                    ?err,
                    "could not read LastProposed for the max-delay re-send"
                );
            })
            .ok()
            .flatten()
            .filter(|header| header.epoch() == current_epoch && header.round() >= self.round)
    }

    /// Wrapper async function to either query the pending header or never resolve.
    async fn pending_header(
        pending_header: &mut Option<PendingHeaderTask>,
    ) -> ProposerResult<Header> {
        if let Some(pending_header) = pending_header {
            pending_header.await?
        } else {
            std::future::pending().await
        }
    }

    /// Wait for the armed parent grace's deadline, or never resolve when none is armed.
    ///
    /// No timer exists while no grace is armed, so with `parent_grace` at zero this future is
    /// always pending.
    async fn grace_deadline(deadline: Option<tokio::time::Instant>) {
        match deadline {
            Some(deadline) => tokio::time::sleep_until(deadline).await,
            None => std::future::pending().await,
        }
    }

    /// Run the proposer task.
    /// Returns Ok on shutdown or an error to indicate a fatal condition.
    async fn run(
        &mut self,
        mut rx_our_digests: impl TnReceiver<OurDigestMessage>,
        mut rx_parents: impl TnReceiver<(Vec<Certificate>, Round)>,
        mut rx_committed_own_headers: impl TnReceiver<(Round, Vec<Round>)>,
        mut rx_uncertified_own_headers: impl TnReceiver<(Round, HeaderDigest)>,
    ) -> ProposerResult<()> {
        let mut pending_header = None;
        let mut max_delay_timed_out = false;
        let mut min_delay_timed_out = false;
        loop {
            tokio::select! {
                _ = &self.rx_shutdown => {
                    return Ok(())
                }
                // check for new digests from workers and send ack back to worker
                //
                // ack to worker implies that the block is recorded on the primary
                // and will be tracked until the block is included
                // ie) primary will attempt to propose this digest until it is
                // committed/sequenced in the DAG or the epoch concludes
                //
                // NOTE: this will not persist primary restarts
                //
                // with `digest_dedupe` on, a digest the proposer already holds is acked and
                // dropped: the copy it holds is the one that rides a header
                Some(msg) = rx_our_digests.recv() =>
                {
                    debug!(target: "primary::proposer", authority=?self.authority_id, round=self.round, "received digest");

                    // parse message into parts
                    let (ack, digest) = msg.process();
                    let _ = ack.send(());
                    self.queue_digest(digest);
                }
                // check for new parent certificates
                // synchronizer sends collection of certificates when there is quorum (2f+1)
                Some((certs, round)) = rx_parents.recv() => {
                    debug!(target: "primary::proposer", authority=?self.authority_id, this_round=self.round, parent_round=round, num_parents=certs.len(), "received parents");
                    self.process_parents(certs, round)?;
                }
                Some((commit_round, committed_headers)) = rx_committed_own_headers.recv() => {
                    debug!(target: "primary::proposer", authority=?self.authority_id, round=self.round, "received committed update for own header");
                    self.process_committed_headers(commit_round, committed_headers);
                }
                // the certifier cancelled an own header's vote collection for a newer header
                Some((round, digest)) = rx_uncertified_own_headers.recv() => {
                    self.process_uncertified_header(round, digest);
                }
                res = Self::pending_header(&mut pending_header) => {
                    pending_header = None;
                    debug!(target: "primary::proposer", authority=?self.authority_id, "pending header task complete!");
                    self.handle_proposal_result(res)?;
                }
                // tick intervals to ensure they advance
                _ = self.max_delay_interval.tick() => {
                    max_delay_timed_out = true;
                }
                _ = self.min_delay_interval.tick() => {
                    min_delay_timed_out = true;
                }
                _ = Self::grace_deadline(self.parent_grace.deadline()) => {
                    self.parent_grace.expire();
                }
            }
            // gauge the backlog of digests waiting for inclusion in a header
            self.consensus_bus
                .app()
                .metrics()
                .proposer_pending_digests
                .set(self.pending_digests_len() as f64);

            if pending_header.is_some() {
                // continue the loop, don't try to propose a header since we are already working
                // on one.
                continue;
            }

            // proposer doesn't have a pending header
            // Check if conditions are met for proposing a new header
            //
            // New headers are proposed when:
            //
            // 1) a quorum of parents (certificates) received for the current round
            // 2) the execution layer successfully executed the previous round (parent
            //    `BlockNumHash`)
            // 3) One of the following:
            // - the interval expired:
            //      - this primary timed out on the leader
            //      - or quit trying to gather enough votes for the leader
            // - the worker created enough blocks (header_num_of_batches_threshold)
            //      - this is happy path
            //      - vote for leader or leader already has enough votes to trigger commit
            let enough_parents = self.has_parents_for_round();
            let enough_digests = self.pending_digests_len() >= self.header_num_of_batches_threshold;

            // evaluate conditions for bool value
            let should_create_header = enough_parents
                && (max_delay_timed_out
                    || (self.advance_round && (enough_digests || min_delay_timed_out)));
            // If we have not proposed a header in more than a max_header_delay time then repropose.
            // We may be in a race condition on a network restart...
            let should_repropose_header = !should_create_header && max_delay_timed_out;

            debug!(
                target: "primary::proposer",
                authority=?self.authority_id,
                round=self.round,
                enough_parents,
                enough_digests,
                self.advance_round,
                min_delay_timed_out,
                max_delay_timed_out,
                should_create_header,
                "polled...",
            );

            // a jump to a newer round's parents ends a grace armed for the older round
            self.parent_grace.end_if_round_moved(&self.last_parents);

            // if all conditions are met, create the next header
            if should_create_header {
                // the parent grace can only hold a header that is ready now: it never proposes
                // one that is not, and the max-delay path neither waits nor lets it run on
                let grace = if max_delay_timed_out {
                    self.parent_grace.end(GraceOutcome::Preempted)
                } else {
                    match self.parent_grace.hold(&self.last_parents) {
                        GraceHold::Wait => continue,
                        GraceHold::Propose(waited) => waited,
                    }
                };

                if max_delay_timed_out {
                    // expect this interval to expire occassionally
                    //
                    // if it expires too often, it either means some validators are Byzantine or
                    // that the network is experiencing periods of asynchrony
                    //
                    // periods of asynchrony possibly caused by misconfigured `max_header_delay`
                    //
                    // the interval's period stays at the configured delay; the delay in force is
                    // the one it was last reset to, which is halved when this node leads next round
                    warn!(target: "primary::proposer", interval=?self.calc_max_delay(), "max delay interval expired for round {}", self.round);
                }

                // the branch that fired, reported on the `header proposed` line
                let reason = ProposeReason::select(max_delay_timed_out, enough_digests);

                debug!(target: "primary::proposer", authority=?self.authority_id, reason = reason.as_str(), "proposing next header!");

                // propose header
                self.parent_grace.on_propose(&self.last_parents, grace);
                pending_header = Some(self.propose_next_header(reason)?);
                max_delay_timed_out = false;
                min_delay_timed_out = false;
            } else if should_repropose_header {
                if let Some(last_proposed) = self.resendable_last_proposed() {
                    warn!(target: "primary::proposer", interval=?self.calc_max_delay(), "re-proposing last header after max delay interval expired for round {}", self.round);
                    let (tx, rx) = oneshot::channel();
                    let consensus_bus = self.consensus_bus.clone();
                    let proposer_store = self.proposer_store.clone();
                    self.task_spawner.spawn_task("re-propose header after delay", async move {
                        // use this instead of store_and_send to because rx always expects a Header
                        let res = Proposer::repropose_header(
                            last_proposed,
                            proposer_store,
                            &consensus_bus,
                        )
                        .await;
                        let _ = tx.send(res);
                        Ok(())
                    });
                    max_delay_timed_out = false;
                    min_delay_timed_out = false;
                    pending_header = Some(rx);
                }
            }
        }
    }
}

/// The digests of a new header for its `header proposed` line: `[0x…,0x…]`, each the full
/// lowercase hex of a batch digest, in payload order.
///
/// Formatted only when the line is written, and a header holds at most
/// `max_header_num_of_batches` digests, so the line stays bounded.
struct HeaderDigests<'a>(&'a VecDeque<ProposerDigest>);

impl fmt::Display for HeaderDigests<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        for (i, proposed) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{}", proposed.digest)?;
        }
        f.write_str("]")
    }
}

/// Append the batch digests of `header`, in payload order, to their workers' lanes in `lanes`,
/// each stamped as queued at `requeued_at` and marked as re-queued.
///
/// Each digest takes its first receipt back out of `first_received`, so its wait from the worker's
/// report keeps counting across the re-queue. A digest without one (it rode a stored header
/// re-sent after a restart, so this proposer never received it) starts at `requeued_at`.
fn push_payload(
    lanes: &mut BTreeMap<WorkerId, VecDeque<ProposerDigest>>,
    header: &Header,
    requeued_at: Instant,
    first_received: &mut BTreeMap<BlockHash, Instant>,
) {
    header.payload().iter().for_each(|(digest, worker_id)| {
        lanes.entry(*worker_id).or_default().push_back(ProposerDigest {
            digest: *digest,
            worker_id: *worker_id,
            received: requeued_at,
            first_received: first_received.remove(digest).unwrap_or(requeued_at),
            requeued: true,
        });
    });
}

/// Whole milliseconds in `duration`, saturating at `u64::MAX`, for the timing fields of the
/// proposer's log lines.
fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// How far below its own tip the quorum rule may name a block, in entries of the recent-blocks
/// ring (executed outputs, not block numbers).
///
/// The ring's capacity is `gc_depth`, so the floor is its own constant. Under the `always` policy
/// every proposer of an in-step fleet settles exactly here: the blocks its parents name are earlier
/// picks of the same rule, not tips, so the quorum value can only rise through the floor.
pub(crate) const QUORUM_EXEC_BLOCK_FLOOR_ENTRIES: usize = 8;

/// The bound on the adaptive policy's window, in rounds: repeated cancels double the configured
/// `quorum_exec_block_starved_rounds` up to this value.
///
/// Each failed return to the own tip costs one or two cancelled headers: the probe, and the next
/// header if it fails too (it is proposed before the probe's cancel report arrives). With p the
/// chance that a header naming the own tip certifies, a proposer that spends a share f of its
/// rounds on the tip loses about f × (1 − p) of its headers, never more than with the switch off
/// (f = 1) as long as headers under the quorum rule certify. Holding the loss under the 2 % the
/// bench-6 certification target (98 %) leaves needs f ≤ 0.02 ÷ (1 − p). At this bound a proposer
/// whose tip never certifies loses 2 of every 130 headers; the model in the proposer tests loses
/// about 1 % over 4000 rounds at p = 0.5 to 0.9 (1.3 % at p = 0.6).
///
/// What the policy cannot do: know p. Its only evidence is a streak of certified tip headers (see
/// [`QUORUM_EXEC_BLOCK_RESET_STREAK`]). Between p = 0.9 and 0.98 such a streak is common enough to
/// reset the window, so f grows and the share lost moves toward 1 − p: about 1 % at p = 0.97 in
/// the model, 1.5 % in its worst 4000-round sample. Every climb to the bound also costs one or two
/// headers per doubling (five doublings from a window of 4), so a measurement that starts as a
/// regime starts loses 5 to 10 headers before the steady state: the e2e test, about 100 rounds
/// long, lost 7 or 8 and certified 0.92 to 0.93. A configured value above the bound is used as is
/// and never doubled.
pub(crate) const QUORUM_EXEC_BLOCK_MAX_STARVED_ROUNDS: Round = 128;

/// How many consecutive own headers, proposed outside a window (under the tip policy) and
/// certified, reset the adaptive policy's next window to the configured value.
///
/// One certified tip header says little about the chance that the tip certifies: where it
/// certifies every other round, a reset on each success brought the proposer back to its tip every
/// six rounds and lost one header in six (0.81 certified in the e2e test). A streak of 64 happens
/// in about 0.1 % of returns at p = 0.9 (0.9^64), so the windows stay long while the tip fails
/// often and reset once it certifies reliably. Waiting for the streak costs no headers: a policy
/// back on a tip that keeps certifying stays there, since only a cancel opens a window. It only
/// makes a cancel within 64 rounds of the return open a long window. The streak counts the own
/// certificates found among the next header's parents, so a header certified after the proposer
/// moved on neither counts nor breaks it, and the reset can take longer than 64 rounds.
pub(crate) const QUORUM_EXEC_BLOCK_RESET_STREAK: u32 = 64;

/// The rule that names a header's execution block, as the `quorum_exec_block` switches and the
/// adaptive window select it for one header. Reported as `exec_policy` on `header proposed`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecPolicy {
    /// `quorum_exec_block: false`: the own tip, read exactly as before the switch existed.
    Off,
    /// `quorum_exec_block: true`, `quorum_exec_block_starved_rounds: 0`: the quorum rule on every
    /// header.
    Always,
    /// The adaptive policy outside a window: the own tip.
    Tip,
    /// The adaptive policy inside a window opened by a cancelled own header: the quorum rule.
    Starved,
}

impl ExecPolicy {
    /// The value of the `exec_policy` field on `header proposed`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Always => "always",
            Self::Tip => "tip",
            Self::Starved => "starved",
        }
    }

    /// Whether the header's block comes from the quorum rule rather than the own tip.
    fn applies_quorum_rule(self) -> bool {
        matches!(self, Self::Always | Self::Starved)
    }
}

/// Why a header names the execution block it names. Reported as `exec_basis` on `header proposed`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecBasis {
    /// The own tip: the policy names the tip, the ring is empty, fewer than a quorum of voting
    /// power named usable blocks, or the quorum value is at or above the own tip (the cap).
    Tip,
    /// The quorum value is below the own tip and no older than the floor; the block is the newest
    /// own ring entry at or below it.
    Quorum,
    /// The quorum value is older than the floor: the ring entry
    /// [`QUORUM_EXEC_BLOCK_FLOOR_ENTRIES`] back from the tip, or the oldest entry of a shorter
    /// ring.
    Floor,
}

impl ExecBasis {
    /// The value of the `exec_basis` field on `header proposed`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Tip => "tip",
            Self::Quorum => "quorum",
            Self::Floor => "floor",
        }
    }
}

/// The proposer's state for the `quorum_exec_block` switches.
///
/// With the switch off every header is [`ExecPolicy::Off`]; with a window of 0 every header is
/// [`ExecPolicy::Always`]. Otherwise headers name the own tip until the certifier reports an own
/// header cancelled uncertified; the next rounds, as many as the current window, use the quorum
/// rule, and the policy returns to the tip when those rounds have elapsed. The return is by round
/// number, never by comparing blocks: once several proposers use the rule, the blocks they name
/// ratchet down, so a test such as "the quorum value reached my tip" would never pass.
///
/// A cancel that is counted doubles the next window, up to
/// [`QUORUM_EXEC_BLOCK_MAX_STARVED_ROUNDS`]. The next window returns to the configured value only
/// after [`QUORUM_EXEC_BLOCK_RESET_STREAK`] consecutive own headers proposed outside a window
/// (under the tip policy) certified; every cancel report restarts the count. A single certified
/// tip header is not enough where the tip certifies part of the time. A header certified inside a
/// window does not count, even when its quorum value reached the tip: it shows the quorum rule
/// works, not that the tip would, and counting it would make every return to the tip fail at the
/// configured window's rate (about one probe and two lost headers every `K + 2` rounds) while the
/// proposer stays ahead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExecBlockPolicy {
    /// `quorum_exec_block`.
    enabled: bool,
    /// `quorum_exec_block_starved_rounds`: 0 for the quorum rule on every header, otherwise the
    /// window a first cancel opens.
    base_window: Round,
    /// The window the next counted cancel opens.
    next_window: Round,
    /// The proposer's round when the current run of windows opened; rounds above it and at or
    /// below [`Self::starved_through`] use the quorum rule. A header at or below it was built
    /// before that run, so its cancel belongs to the episode that opened the run and is not
    /// counted again.
    window_start: Round,
    /// The last round of the current window; 0 before any window.
    starved_through: Round,
    /// Own headers proposed outside a window and certified since the last cancel report.
    certified_streak: u32,
}

impl ExecBlockPolicy {
    /// The policy for the switch values `quorum_exec_block` and
    /// `quorum_exec_block_starved_rounds`.
    pub(crate) fn new(enabled: bool, starved_rounds: u32) -> Self {
        Self {
            enabled,
            base_window: starved_rounds,
            next_window: starved_rounds,
            window_start: 0,
            starved_through: 0,
            certified_streak: 0,
        }
    }

    /// Whether the adaptive policy (own tip, quorum rule in windows) is in force.
    fn adaptive(&self) -> bool {
        self.enabled && self.base_window > 0
    }

    /// Whether a header at `round` falls in the current window.
    fn in_window(&self, round: Round) -> bool {
        round > self.window_start && round <= self.starved_through
    }

    /// The rule for the header proposed at `round`.
    pub(crate) fn for_round(&self, round: Round) -> ExecPolicy {
        if !self.enabled {
            ExecPolicy::Off
        } else if self.base_window == 0 {
            ExecPolicy::Always
        } else if self.in_window(round) {
            ExecPolicy::Starved
        } else {
            ExecPolicy::Tip
        }
    }

    /// The certifier cancelled the own header at `header_round` uncertified while the proposer is
    /// at `proposer_round` (the round of its newest header, whose block is already chosen).
    ///
    /// A header built before the current run of windows opened is ignored: the cancel of the
    /// header that opened the run lags the next header by a round, and that next header named its
    /// tip too. Any other cancel, of a tip header after a window or of a header inside one, opens
    /// a window for the rounds after `proposer_round` (or extends the current one) and doubles the
    /// next window. Every report, ignored or not, restarts the streak of certified tip headers.
    pub(crate) fn on_cancelled(&mut self, header_round: Round, proposer_round: Round) {
        if !self.adaptive() {
            return;
        }
        self.certified_streak = 0;
        if header_round <= self.window_start {
            return;
        }
        // a cancel that arrives at the window's last round extends the run instead: that round's
        // header was built under the rule and must stay inside it, or its certificate would count
        // toward the streak as a certified tip header
        if proposer_round > self.starved_through {
            // the previous window is spent: a new run starts after the proposer's round
            self.window_start = proposer_round;
        }
        self.starved_through =
            self.starved_through.max(proposer_round.saturating_add(self.next_window));
        self.next_window = self
            .next_window
            .saturating_mul(2)
            .min(QUORUM_EXEC_BLOCK_MAX_STARVED_ROUNDS)
            .max(self.base_window);
    }

    /// The own header at `header_round` certified: its certificate is a parent of the next header.
    ///
    /// A header proposed outside a window, under the tip policy, extends the streak of certified
    /// tip headers; once the streak reaches [`QUORUM_EXEC_BLOCK_RESET_STREAK`] the next window
    /// returns to the configured value. A header inside a window, even one whose quorum value
    /// reached the own tip (`exec_basis=tip`), neither counts nor breaks the streak.
    pub(crate) fn on_certified(&mut self, header_round: Round) {
        if !self.adaptive() || self.in_window(header_round) {
            return;
        }
        self.certified_streak = self.certified_streak.saturating_add(1);
        if self.certified_streak >= QUORUM_EXEC_BLOCK_RESET_STREAK {
            self.next_window = self.base_window;
        }
    }
}

/// One parent certificate's named execution block, as the quorum rule reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ParentExecBlock {
    /// The certificate's author.
    pub(crate) author: AuthorityIdentifier,
    /// The author's voting power in the proposer's committee.
    pub(crate) power: VotingPower,
    /// The certificate's epoch.
    pub(crate) epoch: Epoch,
    /// The certificate's round; 0 for a genesis certificate.
    pub(crate) round: Round,
    /// The number of the execution block the certificate's header names.
    pub(crate) number: BlockNumber,
}

impl ParentExecBlock {
    /// The named block of `parent`, weighted by its author's power in `committee`.
    fn new(parent: &Certificate, committee: &Committee) -> Self {
        Self {
            author: parent.origin().clone(),
            power: committee.voting_power_by_id(parent.origin()),
            epoch: parent.epoch(),
            round: parent.round(),
            number: parent.header().latest_execution_block().number,
        }
    }
}

/// Everything the execution-block choice needs except the ring, fixed when the proposal is
/// spawned.
#[derive(Clone, Debug)]
pub(crate) struct ExecBlockInputs {
    /// The rule for this header.
    pub(crate) policy: ExecPolicy,
    /// This proposer's authority: its tip stands for it, and its own certificate is skipped.
    pub(crate) author: AuthorityIdentifier,
    /// This proposer's voting power.
    pub(crate) own_power: VotingPower,
    /// The header's epoch: parents from another epoch are skipped.
    pub(crate) epoch: Epoch,
    /// The committee's quorum threshold (2f+1) in voting power.
    pub(crate) quorum_threshold: VotingPower,
    /// The header's parents' named blocks; empty unless the policy applies the quorum rule.
    pub(crate) parents: Vec<ParentExecBlock>,
}

impl ExecBlockInputs {
    /// The inputs for a header by `author` on `parents` under `policy`.
    fn new(
        policy: ExecPolicy,
        committee: &Committee,
        author: &AuthorityIdentifier,
        parents: &[Certificate],
    ) -> Self {
        let parents = if policy.applies_quorum_rule() {
            parents.iter().map(|parent| ParentExecBlock::new(parent, committee)).collect()
        } else {
            Vec::new()
        };
        Self {
            policy,
            author: author.clone(),
            own_power: committee.voting_power_by_id(author),
            epoch: committee.epoch(),
            quorum_threshold: committee.quorum_threshold(),
            parents,
        }
    }

    /// Choose the header's execution block from the bus's recent-blocks ring.
    ///
    /// Synchronous on purpose: the ring is read once per proposal and the watch's read guard is
    /// dropped on return, before the caller reaches an await.
    fn choose(&self, consensus_bus: &ConsensusBus) -> ExecBlockChoice {
        if self.policy == ExecPolicy::Off {
            // the switch is off: the call headers have always used, so their bytes do not change
            let tip = consensus_bus.app().latest_execution_block_num_hash();
            return ExecBlockChoice { block: tip, tip, basis: ExecBasis::Tip };
        }
        let ring = consensus_bus.app().recent_blocks().borrow();
        choose_exec_block(&ring, self)
    }
}

/// The execution block a header names, the own tip it was chosen against and why.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ExecBlockChoice {
    /// The block the header names.
    pub(crate) block: BlockNumHash,
    /// The proposer's own executed tip when the block was chosen.
    pub(crate) tip: BlockNumHash,
    /// Why this block.
    pub(crate) basis: ExecBasis,
}

/// Choose the execution block for a header from this node's recent-blocks `ring` (its newest
/// entry is the own tip) and the header's parents (issue #1804, design D).
///
/// Under a policy that applies the quorum rule:
/// - the own tip counts once, for the own authority; each parent from another author counts when it
///   is of the header's epoch and not genesis (round 0). An own certificate among the parents is
///   skipped, and an author is counted at most once (its first certificate);
/// - the values are ranked by number and weighted by voting power: the quorum value is the highest
///   number that holders of at least `quorum_threshold` power have reached. With less usable power
///   than that the own tip is named;
/// - the quorum value is capped at the own tip: a higher block is one this node cannot vouch for,
///   and letting f Byzantine parents pick it would bring the starvation back;
/// - it is floored at the ring entry [`QUORUM_EXEC_BLOCK_FLOOR_ENTRIES`] back from the tip (the
///   oldest entry of a shorter ring);
/// - the block is the newest own ring entry at or below the value, so the number and the hash are
///   always this node's own execution, never a parent's hash.
///
/// Peer numbers are only compared, never used in arithmetic or as an index; power sums saturate.
/// An empty ring names the default block, as the tip read did before the switch existed.
pub(crate) fn choose_exec_block(ring: &RecentBlocks, inputs: &ExecBlockInputs) -> ExecBlockChoice {
    let tip = ring.latest_execution_block_num_hash();
    let own_tip = ExecBlockChoice { block: tip, tip, basis: ExecBasis::Tip };
    if !inputs.policy.applies_quorum_rule() || ring.is_empty() {
        return own_tip;
    }

    let mut counted = BTreeSet::from([&inputs.author]);
    let mut values = vec![(tip.number, inputs.own_power)];
    for parent in &inputs.parents {
        if parent.epoch == inputs.epoch && parent.round > 0 && counted.insert(&parent.author) {
            values.push((parent.number, parent.power));
        }
    }
    // highest first; the order among equal numbers cannot change the number the walk returns
    values.sort_unstable_by_key(|(number, _)| Reverse(*number));
    let mut holders: VotingPower = 0;
    let quorum_value = values.iter().find_map(|(number, power)| {
        holders = holders.saturating_add(*power);
        (holders >= inputs.quorum_threshold).then_some(*number)
    });
    let Some(quorum_value) = quorum_value.filter(|value| *value < tip.number) else {
        // too little usable power, or the cap: the own tip
        return own_tip;
    };

    let Some(floor) = ring.nth_newest(QUORUM_EXEC_BLOCK_FLOOR_ENTRIES) else {
        return own_tip;
    };
    if quorum_value < floor.number {
        return ExecBlockChoice { block: floor, tip, basis: ExecBasis::Floor };
    }
    // the floor entry is at or below the value, so the lookup always finds an entry
    match ring.newest_at_or_below(quorum_value) {
        Some(block) => ExecBlockChoice { block, tip, basis: ExecBasis::Quorum },
        None => ExecBlockChoice { block: floor, tip, basis: ExecBasis::Floor },
    }
}

/// What ended an armed parent grace: the `outcome` label of `tn_primary_parent_grace_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GraceOutcome {
    /// Every author the grace waited for delivered its certificate.
    Complete,
    /// The deadline fired with at least one of them still missing.
    Expired,
    /// The max-delay path proposed, or the round moved on, before either.
    Preempted,
}

impl GraceOutcome {
    /// Every outcome, the order of [`ParentGraceHandles::outcomes`].
    pub(crate) const ALL: [Self; 3] = [Self::Complete, Self::Expired, Self::Preempted];

    /// The static metric label for this outcome.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Expired => "expired",
            Self::Preempted => "preempted",
        }
    }

    /// This outcome's position in [`Self::ALL`].
    fn index(self) -> usize {
        match self {
            Self::Complete => 0,
            Self::Expired => 1,
            Self::Preempted => 2,
        }
    }
}

/// What the run loop does with a header that is ready to propose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GraceHold {
    /// Keep collecting parents: a grace is armed and still running.
    Wait,
    /// Propose now. The value is how long a grace held the header, zero when none was armed.
    Propose(Duration),
}

/// How far behind the proposer's round a dropped parent certificate is: the `origin` label of
/// `tn_primary_late_parents_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LateParentOrigin {
    /// One round behind the proposer's round. In a running round this is a certificate of the
    /// round the proposer's newest header built on that arrived after the header went out, so
    /// that header does not carry it. Right after a round jump or a restart it can also be a
    /// certificate of a round no own header built on.
    OwnRoundBehind,
    /// Two or more rounds behind: a replay or a far later delivery.
    OlderRound,
}

impl LateParentOrigin {
    /// The origin of a certificate of `parents_round` that reaches a proposer at `own_round`.
    pub(crate) fn of(parents_round: Round, own_round: Round) -> Self {
        if parents_round.saturating_add(1) == own_round {
            Self::OwnRoundBehind
        } else {
            Self::OlderRound
        }
    }

    /// The static metric label for this origin.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::OwnRoundBehind => "own_round_behind",
            Self::OlderRound => "older_round",
        }
    }
}

/// The parent grace's metric handles, resolved once per epoch with the proposer.
struct ParentGraceHandles {
    /// `tn_primary_parent_grace_wait_seconds`: how long each armed grace held its header.
    wait: Histogram,
    /// `tn_primary_parent_grace_total{outcome}`, in [`GraceOutcome::ALL`] order.
    outcomes: [Counter; GraceOutcome::ALL.len()],
    /// `tn_primary_late_parents_total{origin="own_round_behind"}`.
    late_own_round_behind: Counter,
    /// `tn_primary_late_parents_total{origin="older_round"}`.
    late_older_round: Counter,
}

impl ParentGraceHandles {
    /// Resolve the counters; `wait` is the histogram on the bus's [`crate::PrimaryMetrics`].
    fn new(wait: Histogram) -> Self {
        let late = |origin: LateParentOrigin| -> Counter {
            metrics::counter!("tn_primary.late_parents_total", "origin" => origin.as_str())
        };
        Self {
            wait,
            outcomes: GraceOutcome::ALL.map(|outcome| {
                metrics::counter!("tn_primary.parent_grace_total", "outcome" => outcome.as_str())
            }),
            late_own_round_behind: late(LateParentOrigin::OwnRoundBehind),
            late_older_round: late(LateParentOrigin::OlderRound),
        }
    }
}

/// The authors whose certificates the last own header carried as parents, and its round.
#[derive(Debug)]
struct GraceBasis {
    /// The round of the last own header.
    round: Round,
    /// The authors of its parents, all of round `round - 1`.
    authors: BTreeSet<AuthorityIdentifier>,
}

/// The proposal in flight: the parents it was given and how long a grace held it.
#[derive(Debug)]
struct GraceInFlight {
    /// The digest and author of every non-genesis parent handed to the proposal. A restart's
    /// re-proposal sends the stored header instead, so only the digests that header carries count.
    parents: Vec<(HeaderDigest, AuthorityIdentifier)>,
    /// How long a grace held the proposal, reported as `grace_ms` on `header proposed`.
    waited: Duration,
}

/// A grace armed for one round of parents.
#[derive(Debug)]
struct ArmedGrace {
    /// The round of the parents being collected.
    round: Round,
    /// The authors of the last own header's parents that had no certificate among them.
    waiting_for: BTreeSet<AuthorityIdentifier>,
    /// When the grace was armed.
    started: tokio::time::Instant,
    /// When the grace gives up on `waiting_for`.
    deadline: tokio::time::Instant,
    /// Whether the deadline has fired.
    expired: bool,
}

/// The proposer's parent grace (the `parent_grace` switch, issue #1803).
///
/// When the proposer has a header ready (the existing propose condition is true and the max-delay
/// path did not fire), it looks for the authors of the certificates its last own header used as
/// parents that have no certificate among the parents collected now. If there are any, it arms one
/// deadline at now + `parent_grace` and keeps collecting, and proposes once they have all arrived
/// or the deadline fires, whichever comes first. The grace only ever delays a header that is ready:
///
/// - the max-delay path is unconditional and ends a running grace (`preempted`);
/// - a jump to a newer round's parents ends it too (`preempted`), and the new round has no basis;
/// - at most one grace is armed per round of parents;
/// - an author the last own header did not build on is never waited for;
/// - the grace is capped at `max_header_delay`, though the max-delay tick, reset at every proposal,
///   always fires first;
/// - with `parent_grace` at zero nothing is armed and no timer is created.
///
/// There is no grace without a last own header of the previous round to compare with: at the first
/// round of an epoch (round 1 builds on the genesis certificates, which say nothing about who is
/// live, so round 2 has no grace either), after a round this node skipped by jumping to newer
/// parents, after a restart until a header is built in this process, and after a header whose
/// parents could not be matched.
pub(crate) struct ParentGrace {
    /// The configured grace, capped at `max_header_delay`; zero is off.
    grace: Duration,
    /// Who the last own header built on.
    basis: Option<GraceBasis>,
    /// The proposal in flight, between [`Self::on_propose`] and [`Self::on_proposed`].
    in_flight: Option<GraceInFlight>,
    /// The grace armed for the round being collected.
    armed: Option<ArmedGrace>,
    /// The parents round of the last grace armed: one per round.
    last_armed_round: Option<Round>,
    /// The metric handles.
    handles: ParentGraceHandles,
}

impl ParentGrace {
    /// The grace of a new epoch's proposer: no basis, nothing armed.
    pub(crate) fn new(grace: Duration, max_header_delay: Duration, wait: Histogram) -> Self {
        Self {
            grace: grace.min(max_header_delay),
            basis: None,
            in_flight: None,
            armed: None,
            last_armed_round: None,
            handles: ParentGraceHandles::new(wait),
        }
    }

    /// The deadline of the armed grace, while it has not fired.
    pub(crate) fn deadline(&self) -> Option<tokio::time::Instant> {
        self.armed.as_ref().filter(|armed| !armed.expired).map(|armed| armed.deadline)
    }

    /// Mark the armed grace's deadline as fired; the next [`Self::hold`] proposes.
    pub(crate) fn expire(&mut self) {
        if let Some(armed) = self.armed.as_mut() {
            armed.expired = true;
        }
    }

    /// End an armed grace whose round is no longer the round of `parents`.
    ///
    /// Only the future-round jump of `Proposer::process_parents` replaces the parents while a grace
    /// runs: a proposal ends the grace before it takes them.
    pub(crate) fn end_if_round_moved(&mut self, parents: &[Certificate]) {
        let round = parents.first().map(Certificate::round);
        if self.armed.as_ref().is_some_and(|armed| Some(armed.round) != round) {
            self.end(GraceOutcome::Preempted);
        }
    }

    /// Decide whether a header that is ready over `parents` is proposed now.
    ///
    /// The caller has already found the header ready and the max-delay path not fired.
    pub(crate) fn hold(&mut self, parents: &[Certificate]) -> GraceHold {
        if self.grace.is_zero() {
            return GraceHold::Propose(Duration::ZERO);
        }
        if let Some(armed) = &self.armed {
            let present: BTreeSet<&AuthorityIdentifier> =
                parents.iter().map(Certificate::origin).collect();
            if armed.waiting_for.iter().all(|author| present.contains(author)) {
                return GraceHold::Propose(self.end(GraceOutcome::Complete));
            }
            if armed.expired {
                return GraceHold::Propose(self.end(GraceOutcome::Expired));
            }
            return GraceHold::Wait;
        }
        let Some(round) = parents.first().map(Certificate::round) else {
            return GraceHold::Propose(Duration::ZERO);
        };
        if self.last_armed_round == Some(round) {
            return GraceHold::Propose(Duration::ZERO);
        }
        let waiting_for = self.missing(round, parents);
        if waiting_for.is_empty() {
            return GraceHold::Propose(Duration::ZERO);
        }
        let started = tokio::time::Instant::now();
        debug!(
            target: "primary::proposer",
            parent_round = round,
            parents = parents.len(),
            missing = waiting_for.len(),
            grace_ms = duration_ms(self.grace),
            "parent grace armed",
        );
        self.armed = Some(ArmedGrace {
            round,
            waiting_for,
            started,
            deadline: started + self.grace,
            expired: false,
        });
        self.last_armed_round = Some(round);
        GraceHold::Wait
    }

    /// End the armed grace with `outcome` and return how long it ran; zero when none was armed.
    pub(crate) fn end(&mut self, outcome: GraceOutcome) -> Duration {
        let Some(armed) = self.armed.take() else {
            return Duration::ZERO;
        };
        let waited = armed.started.elapsed();
        self.handles.wait.record(waited.as_secs_f64());
        self.handles.outcomes[outcome.index()].increment(1);
        debug!(
            target: "primary::proposer",
            parent_round = armed.round,
            outcome = outcome.as_str(),
            waited_ms = duration_ms(waited),
            "parent grace ended",
        );
        waited
    }

    /// Note the parents handed to the next proposal and how long a grace held it.
    ///
    /// Genesis parents are left out: every authority has one, so they say nothing about who is
    /// live. Nothing is kept while the grace is off.
    pub(crate) fn on_propose(&mut self, parents: &[Certificate], waited: Duration) {
        if self.grace.is_zero() {
            return;
        }
        let parents = parents
            .iter()
            .filter(|parent| parent.round() > 0)
            .map(|parent| (parent.digest(), parent.origin().clone()))
            .collect();
        self.in_flight = Some(GraceInFlight { parents, waited });
    }

    /// How long a grace held the proposal in flight, zero when none did.
    pub(crate) fn in_flight_wait(&self) -> Duration {
        self.in_flight.as_ref().map_or(Duration::ZERO, |in_flight| in_flight.waited)
    }

    /// Take the authors of the parents `header` carries as the next round's basis.
    ///
    /// A header from the max-delay re-send has no proposal in flight and keeps the basis: it is the
    /// stored header again.
    pub(crate) fn on_proposed(&mut self, header: &Header) {
        let Some(in_flight) = self.in_flight.take() else {
            return;
        };
        let authors: BTreeSet<_> = in_flight
            .parents
            .into_iter()
            .filter(|(digest, _)| header.parents().contains(digest))
            .map(|(_, author)| author)
            .collect();
        self.basis = if authors.is_empty() {
            None
        } else {
            Some(GraceBasis { round: header.round(), authors })
        };
    }

    /// Count `count` parent certificates of `parents_round` dropped by a proposer at `own_round`.
    pub(crate) fn count_late_parents(&self, parents_round: Round, own_round: Round, count: usize) {
        let counter = match LateParentOrigin::of(parents_round, own_round) {
            LateParentOrigin::OwnRoundBehind => &self.handles.late_own_round_behind,
            LateParentOrigin::OlderRound => &self.handles.late_older_round,
        };
        counter.increment(u64::try_from(count).unwrap_or(u64::MAX));
    }

    /// The authors of the last own header's parents with no certificate among `parents`.
    ///
    /// Empty unless the last own header is of `round`, the round of `parents`, so it built on the
    /// round before them.
    fn missing(&self, round: Round, parents: &[Certificate]) -> BTreeSet<AuthorityIdentifier> {
        let Some(basis) = self.basis.as_ref().filter(|basis| basis.round == round) else {
            return BTreeSet::new();
        };
        let present: BTreeSet<&AuthorityIdentifier> =
            parents.iter().map(Certificate::origin).collect();
        basis.authors.iter().filter(|author| !present.contains(author)).cloned().collect()
    }
}

/// Where the proposer already holds a batch digest its worker reported again: the `where` field of
/// the `batch digest already queued` line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DigestHeld {
    /// In a worker's pending lane, waiting for a header.
    Pending,
    /// In the payload of the own header being built, or of an own header that is proposed and
    /// neither committed nor re-queued yet.
    Proposed,
}

impl DigestHeld {
    /// The static `where` value for this place.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Proposed => "proposed",
        }
    }
}

/// The `digest_dedupe` switch's index of the batch digests a proposer already holds (issue #1807).
///
/// A report is checked against three places, cheapest first:
/// - the pending lanes, through [`Self::pending`], a count per digest kept in step with the lanes:
///   one `BTreeMap` lookup, O(log n) in the n pending digests (about a dozen 32-byte comparisons at
///   4,096);
/// - the header being built, through [`Self::in_flight`]: a scan of at most
///   `max_header_num_of_batches` digests;
/// - each own header in `Proposer::proposed_headers`: one hash lookup in the header's payload
///   `IndexMap`, so linear in the own headers that are proposed and neither committed nor
///   re-queued. That is a few rounds' worth while own headers commit, and at most the epoch's
///   rounds when they do not (a header that is certified but never in a leader's history stays
///   tracked until the epoch ends).
///
/// It keeps nothing the proposer does not already hold: one entry per pending digest and the
/// payload of at most one header in flight, each released when the digest leaves the lanes or the
/// header's result returns. It is only ever looked up, so no iteration order reaches a header.
struct DigestDedupe {
    /// How many copies of each digest sit in the pending lanes. A count rather than a set keeps
    /// the index exact even if a lane holds a digest twice, which only a re-send of the stored
    /// header (after a restart, or by the max-delay tick) followed by its re-queue can cause.
    pending: BTreeMap<BlockHash, usize>,
    /// The digests drained into the header being built: out of the lanes, and not yet in
    /// `proposed_headers` until the proposal's result returns.
    in_flight: Vec<BlockHash>,
    /// `tn_primary_duplicate_digest_suppressed_total`.
    suppressed: Counter,
}

impl DigestDedupe {
    /// An empty index for a new epoch's proposer, counting drops in `suppressed`.
    fn new(suppressed: Counter) -> Self {
        Self { pending: BTreeMap::new(), in_flight: Vec::new(), suppressed }
    }

    /// Where `digest` is already held, or `None` when the report is to be queued.
    fn held(
        &self,
        digest: &BlockHash,
        proposed_headers: &BTreeMap<Round, Header>,
    ) -> Option<DigestHeld> {
        if self.pending.contains_key(digest) {
            return Some(DigestHeld::Pending);
        }
        let proposed = self.in_flight.contains(digest)
            || proposed_headers.values().any(|header| header.payload().contains_key(digest));
        proposed.then_some(DigestHeld::Proposed)
    }

    /// Note that `digest` entered a pending lane, from a report or a re-queue.
    fn queued(&mut self, digest: BlockHash) {
        *self.pending.entry(digest).or_default() += 1;
    }

    /// Note that `digests` left the pending lanes for the header being built.
    fn drained(&mut self, digests: impl Iterator<Item = BlockHash>) {
        self.in_flight.clear();
        for digest in digests {
            match self.pending.get_mut(&digest) {
                Some(count) if *count > 1 => *count -= 1,
                Some(_) => {
                    self.pending.remove(&digest);
                }
                // every digest enters a lane through `queued`, so a miss here means the index
                // drifted from the lanes
                None => warn!(
                    target: "primary::proposer",
                    %digest,
                    "drained batch digest missing from the dedupe index"
                ),
            }
            self.in_flight.push(digest);
        }
    }

    /// Note that the header being built is now tracked in `proposed_headers`, where its payload
    /// answers for its digests.
    fn landed(&mut self) {
        self.in_flight.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::duration_ms;
    use std::time::Duration;

    /// Sub-millisecond remainders round down and a duration past `u64::MAX` milliseconds
    /// saturates instead of wrapping.
    #[test]
    fn duration_ms_truncates_and_saturates() {
        assert_eq!(duration_ms(Duration::ZERO), 0);
        assert_eq!(duration_ms(Duration::from_micros(1_999)), 1);
        assert_eq!(duration_ms(Duration::from_secs(10)), 10_000);
        assert_eq!(duration_ms(Duration::MAX), u64::MAX);
    }
}
