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
    ConsensusBus,
};
use reth_metrics::metrics::Counter;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
    fmt,
    time::Instant,
};
use tn_config::{ConsensusConfig, KeyConfig};
use tn_storage::{tables::LastProposed, ProposerStore};
use tn_types::{
    forks::{seed_signature_active, subsecond_timestamp_active},
    now_ms, AuthorityIdentifier, BlockHash, Certificate, Committee, Database, Epoch, EpochDigest,
    EpochSeedMessage, Hash as _, Header, HeaderDigest, Noticer, Round, TaskManager, TaskSpawner,
    TnReceiver, TnSender, VotingPower, WorkerId,
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
        let digest = ProposerDigest { digest, worker_id, received: Instant::now() };
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
    /// the header that carried it was not committed. The `header proposed` line and
    /// `tn_primary_digest_wait_seconds` measure the oldest digest's wait from here.
    pub received: Instant,
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
            leader_schedule,
            advance_round: true,
            task_spawner,
            prior_epoch_record: config.prior_epoch_record(),
            key_config: config.key_config().clone(),
            headers_proposed: HeadersProposedHandles::new(),
            digests_requeued: DigestsRequeuedHandles::new(),
        }
    }

    /// Make a new header, store it in the proposer store, and forward it to the certifier.
    ///
    /// This task is spawned outside of `Self`.
    ///
    /// - current_header: caller checks to see if there is already a header built for this round. If
    ///   current_header.is_some() the proposer uses this header instead of building a new one.
    #[instrument(level = "debug", skip_all, fields(round = identity.round, epoch = identity.epoch, num_digests = digests.len()))]
    async fn propose_header(
        identity: HeaderIdentity,
        proposer_store: DB,
        consensus_bus: &ConsensusBus,
        parents: Vec<Certificate>,
        digests: VecDeque<ProposerDigest>,
        trigger: ProposalTrigger,
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

        let header = Header::new(
            author,
            current_round,
            current_epoch,
            digests.iter().map(|m| (m.digest, m.worker_id)).collect(),
            parents.iter().map(|x| x.header().digest()).collect(),
            consensus_bus.app().latest_execution_block_num_hash(),
            seed_signature,
            created_at,
        );

        // the oldest digest's wait ends here, once the header carrying it exists; a header
        // without digests has no wait to report and records no sample
        let oldest_digest_wait = digests.iter().map(|d| d.received).min().map(|r| r.elapsed());
        if let Some(wait) = oldest_digest_wait {
            consensus_bus.app().metrics().digest_wait_seconds.record(wait.as_secs_f64());
        }

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
            pending_before = trigger.pending_before,
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
    /// The guard checks the round, not the voting weight: the certificate aggregator sends a
    /// round's parents to the proposer only once they reach a quorum, and later certificates of
    /// that round only extend the set.
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
            if self.proposed_headers.remove(&round).is_none() {
                warn!("own committed header not found at round {round}, probably because of restarts.");
            };
        }

        // re-insert batches for any proposed header from a round below the current commit
        //
        // ensure batches stay FIFO within each worker to re-send them
        //
        // payloads: oldest -> newest, partitioned per worker (issue #556)
        let mut digests_to_resend: BTreeMap<WorkerId, VecDeque<ProposerDigest>> = BTreeMap::new();
        // Oldest to newest rounds.
        let mut retransmit_rounds = Vec::new();
        // a re-queued digest's wait restarts here: the proposer keeps no arrival time for digests
        // already in a header
        let requeued_at = Instant::now();

        // walk proposed headers in order by round, up to the committed round
        self.proposed_headers
            .iter()
            .take_while(|(header_round, _)| **header_round <= max_committed_round)
            .for_each(|(header_round, header)| {
                // add payloads from oldest to newest into each worker's lane
                push_payload(&mut digests_to_resend, header, requeued_at);
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
    fn process_uncertified_header(&mut self, round: Round, digest: HeaderDigest) {
        let mut requeued = BTreeMap::new();
        match self.proposed_headers.get(&round) {
            Some(header) if round < self.round && header.digest() == digest => {
                push_payload(&mut requeued, header, Instant::now());
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
        order
            .into_iter()
            .filter_map(|worker_id| self.digests.get_mut(&worker_id).and_then(VecDeque::pop_front))
            .collect()
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
                };
                let digests = self.drain_digests_for_header();
                let parents = std::mem::take(&mut self.last_parents);
                let authority_id = self.authority_id.clone();
                let prior_epoch_record = self.prior_epoch_record;
                let key_config = self.key_config.clone();

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
        // track header so proposer can repropose the digests and system messages
        // if this header fails to be committed for some reason
        self.proposed_headers.insert(header.round(), header);

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
                Some(msg) = rx_our_digests.recv() =>
                {
                    debug!(target: "primary::proposer", authority=?self.authority_id, round=self.round, "received digest");

                    // parse message into parts
                    let (ack, digest) = msg.process();
                    let _ = ack.send(());
                    self.digests.entry(digest.worker_id).or_default().push_back(digest);
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

            // if all conditions are met, create the next header
            if should_create_header {
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
/// each stamped as queued at `requeued_at`.
///
/// The proposer keeps no arrival time for a digest already in a header, so a re-queued digest's
/// wait restarts at `requeued_at`.
fn push_payload(
    lanes: &mut BTreeMap<WorkerId, VecDeque<ProposerDigest>>,
    header: &Header,
    requeued_at: Instant,
) {
    header.payload().iter().for_each(|(digest, worker_id)| {
        lanes.entry(*worker_id).or_default().push_back(ProposerDigest {
            digest: *digest,
            worker_id: *worker_id,
            received: requeued_at,
        });
    });
}

/// Whole milliseconds in `duration`, saturating at `u64::MAX`, for the timing fields of the
/// proposer's log lines.
fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
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
