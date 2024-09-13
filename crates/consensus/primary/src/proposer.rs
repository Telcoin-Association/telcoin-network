// Copyright (c) Telcoin, LLC
// Copyright(C) Facebook, Inc. and its affiliates.
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

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
//! The Proposer is also responsible for processing Worker block's that reach quorum.
//! Collections of worker blocks that reach quorum are included in each header. If the Proposer's
//! header fails to be committed, then block digests from the failed round are included in the next
//! header once the Proposer's round advances.

use crate::{
    consensus::LeaderSchedule,
    error::{ProposerError, ProposerResult},
};
use consensus_metrics::metered_channel::{Receiver, Sender};
use fastcrypto::hash::Hash as _;
use futures::{FutureExt, StreamExt};
use narwhal_primary_metrics::PrimaryMetrics;
use narwhal_storage::ProposerStore;
use narwhal_typed_store::traits::Database;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tn_types::{
    now, BlockHash, Certificate, ConditionalBroadcastReceiver, Header, Round, SystemMessage,
    TimestampSec,
};
use tn_types::{AuthorityIdentifier, Committee, Epoch, WorkerId};
use tokio::{
    sync::{
        oneshot::{self, error::RecvError},
        watch,
    },
    time::{sleep, Duration, Interval},
};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, enabled, error, trace, warn};

/// Type alias for the async task that creates, stores, and sends the proposer's new header.
type PendingHeaderTask = oneshot::Receiver<ProposerResult<Header>>;

/// Messages sent to the proposer about this primary's own workers' block digests
#[derive(Debug)]
pub struct OurDigestMessage {
    /// The digest for the worker's block that reached quorum.
    pub digest: BlockHash,
    /// The worker that produced this block.
    pub worker_id: WorkerId,
    /// The timestamp for when the block was created.
    pub timestamp: TimestampSec,
    /// A channel to send an () as an ack after this digest is processed by the primary.
    pub ack_channel: oneshot::Sender<()>,
}

impl OurDigestMessage {
    /// Process the message.
    ///
    /// Splits the message into components required for processing the batch.
    fn process(self) -> (oneshot::Sender<()>, ProposerDigest) {
        let OurDigestMessage { digest, worker_id, timestamp, ack_channel } = self;
        let digest = ProposerDigest { digest, worker_id, timestamp };
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
    /// The timestamp for when the block was created.
    pub timestamp: TimestampSec,
}

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// The default amount of time the proposer should wait after trying to forward the proposed header
/// to the certifier before returning an error.
const DEFAULT_FATAL_HEADER_TIMEOUT: Duration = Duration::from_secs(60);

/// The proposer creates new headers and send them to the core for broadcasting and further
/// processing.
pub struct Proposer<DB: Database> {
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
    /// The maximum delay the proposer will wait to send to certifier. This interval expires if the
    /// proposer cannot send to certifier within a certain amount of time.
    fatal_header_timeout: Interval,
    /// The latest header.
    opt_latest_header: Option<Header>,
    /// Receiver for shutdown.
    ///
    /// Also used to signal committee change.
    rx_shutdown_stream: BroadcastStream<()>,
    /// Receives the parents to include in the next header (along with their round number) from
    /// `Synchronizer`.
    rx_parents: Receiver<(Vec<Certificate>, Round)>,
    /// Receives the batches' digests from our workers.
    rx_our_digests: Receiver<OurDigestMessage>,
    /// Receives system messages to include in the next header.
    rx_system_messages: Receiver<SystemMessage>,
    /// Sends newly created headers to the `Certifier`.
    tx_headers: Sender<Header>,
    /// The proposer store for persisting the last header.
    proposer_store: ProposerStore<DB>,
    /// The current round of the dag.
    round: Round,
    /// Last time the round has been updated
    last_round_timestamp: Option<TimestampSec>,
    /// Signals a new narwhal round
    tx_narwhal_round_updates: watch::Sender<Round>,
    /// Holds the certificates' ids waiting to be included in the next header.
    last_parents: Vec<Certificate>,
    /// Holds the certificate of the last leader (if any).
    last_leader: Option<Certificate>,
    /// Holds the batches' digests waiting to be included in the next header.
    /// Digests are roughly oldest to newest, and popped in FIFO order from the front.
    digests: VecDeque<ProposerDigest>,
    /// Holds the system messages waiting to be included in the next header.
    system_messages: Vec<SystemMessage>,
    /// Holds the map of proposed previous round headers and their digest messages, to ensure that
    /// all batches' digest included will eventually be re-sent.
    proposed_headers: BTreeMap<Round, Header>,
    /// Receiver for updates when Self's headers were committed by consensus.
    ///
    /// NOTE: this does not mean the header was executed yet.
    rx_committed_own_headers: Receiver<(Round, Vec<Round>)>,
    /// Metrics handler
    metrics: Arc<PrimaryMetrics>,
    /// The consensus leader schedule to be used in order to resolve the leader needed for the
    /// protocol advancement.
    leader_schedule: LeaderSchedule,
    /// Flag if enough conditions are met to advance the round.
    advance_round: bool,
    /// The optional pending header that the proposer has decided to build.
    ///
    /// This value is `Some` when conditions are met to propose the next header.
    /// The task within is responsible for creating, storing, and sending the new header
    /// to the `Certifier`.
    pending_header: Option<PendingHeaderTask>,
}

impl<DB: Database + 'static> Proposer<DB> {
    /// Create a new instance of Self.
    ///
    /// The proposer's intervals and genesis certificate are created in this function.
    /// Also set `advance_round` to true.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        authority_id: AuthorityIdentifier,
        committee: Committee,
        proposer_store: ProposerStore<DB>,
        header_num_of_batches_threshold: usize,
        max_header_num_of_batches: usize,
        max_header_delay: Duration,
        min_header_delay: Duration,
        fatal_header_timeout: Option<Duration>,
        rx_shutdown: ConditionalBroadcastReceiver,
        rx_parents: Receiver<(Vec<Certificate>, Round)>,
        rx_our_digests: Receiver<OurDigestMessage>,
        rx_system_messages: Receiver<SystemMessage>,
        tx_headers: Sender<Header>,
        tx_narwhal_round_updates: watch::Sender<Round>,
        rx_committed_own_headers: Receiver<(Round, Vec<Round>)>,
        metrics: Arc<PrimaryMetrics>,
        leader_schedule: LeaderSchedule,
    ) -> Self {
        // TODO: include EL genesis hash in committee for epoch?
        //
        // NO: bc the first round should include EL genesis hash in primary proposed header.
        let genesis = Certificate::genesis(&committee);
        let fatal_header_timeout = fatal_header_timeout.unwrap_or(DEFAULT_FATAL_HEADER_TIMEOUT);
        // create min/max delay intervals
        let min_delay_interval = tokio::time::interval(min_header_delay);
        let max_delay_interval = tokio::time::interval(max_header_delay);
        let mut fatal_header_timeout = tokio::time::interval(fatal_header_timeout);
        // reset interval because first tick completes immediately
        fatal_header_timeout.reset();
        let rx_shutdown_stream = BroadcastStream::new(rx_shutdown.receiver);

        Self {
            authority_id,
            committee,
            header_num_of_batches_threshold,
            max_header_num_of_batches,
            min_header_delay,
            max_header_delay,
            min_delay_interval,
            max_delay_interval,
            fatal_header_timeout,
            opt_latest_header: None,
            rx_shutdown_stream,
            rx_parents,
            rx_our_digests,
            rx_system_messages,
            tx_headers,
            tx_narwhal_round_updates,
            proposer_store,
            round: 0,
            last_round_timestamp: None,
            last_parents: genesis,
            last_leader: None,
            digests: VecDeque::with_capacity(2 * max_header_num_of_batches),
            system_messages: Vec::new(),
            proposed_headers: BTreeMap::new(),
            rx_committed_own_headers,
            metrics,
            leader_schedule,
            advance_round: true,
            pending_header: None,
        }
    }

    /// Make a new header, store it in the proposer store, and forward it to the certifier.
    ///
    /// This task is spawned outside of `Self`.
    ///
    /// - current_header: caller checks to see if there is already a header built for this round. If
    ///   current_header.is_some() the proposer uses this header instead of building a new one.
    async fn propose_header(
        current_round: Round,
        current_epoch: Epoch,
        authority_id: AuthorityIdentifier,
        proposer_store: ProposerStore<DB>,
        tx_headers: Sender<Header>,
        parents: Vec<Certificate>,
        digests: VecDeque<ProposerDigest>,
        system_messages: Vec<SystemMessage>,
        reason: String,
        metrics: Arc<PrimaryMetrics>,
        leader_and_support: String,
        max_delay: Duration,
    ) -> ProposerResult<Header> {
        // make new header

        // check that the included timestamp is consistent with the parent's timestamp
        //
        // ie) the current time is *after* the timestamp in all included headers
        //
        // if not: log an error and sleep
        let parent_max_time = parents.iter().map(|c| *c.header().created_at()).max().unwrap_or(0);
        let current_time = now();
        if current_time < parent_max_time {
            let drift_sec = parent_max_time - current_time;
            error!(
                "Current time {} earlier than max parent time {}, sleeping for {}ms until max parent time.",
                current_time, parent_max_time, drift_sec,
            );
            metrics.header_max_parent_wait_ms.inc_by(drift_sec);
            sleep(Duration::from_secs(drift_sec)).await;
        }

        let header = Header::new(
            authority_id,
            current_round,
            current_epoch,
            digests.iter().map(|m| (m.digest, (m.worker_id, m.timestamp))).collect(),
            system_messages.clone(),
            parents.iter().map(|x| x.digest()).collect(),
        );

        // update metrics before sending/storing header
        metrics.headers_proposed.with_label_values(&[&leader_and_support]).inc();
        metrics.header_parents.observe(parents.len() as f64);

        if enabled!(tracing::Level::TRACE) {
            let mut msg = format!("Created header {header:?} with parent certificates:\n");
            for parent in parents.iter() {
                msg.push_str(&format!("{parent:?}\n"));
            }
            trace!(msg);
        } else {
            debug!(target: "primary::proposer", "created new header {header:?}");
        }

        // Update metrics related to latency
        let mut total_inclusion_secs = 0.0;
        for digest in &digests {
            let batch_inclusion_secs =
                Duration::from_secs(*header.created_at() - digest.timestamp).as_secs_f64();
            total_inclusion_secs += batch_inclusion_secs;

            // NOTE: This log entry is used to compute performance.
            debug!(
                "Batch {:?} from worker {} took {} seconds from creation to be included in a proposed header",
                digest.digest,
                digest.worker_id,
                batch_inclusion_secs
            );
            metrics.proposer_batch_latency.observe(batch_inclusion_secs);
        }

        // NOTE: This log entry is used to compute performance.
        let (header_creation_secs, avg_inclusion_secs) = if let Some(digest) = digests.front() {
            (
                Duration::from_secs(*header.created_at() - digest.timestamp).as_secs_f64(),
                total_inclusion_secs / digests.len() as f64,
            )
        } else {
            (max_delay.as_secs_f64(), 0.0)
        };

        //
        // TODO: !!! this math is wrong - unix timestamp way off
        //
        // ~~~~~!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

        debug!(
            target: "primary::proposer",
            "Header {:?} was created in {} seconds. Contains {} batches, with average delay {} seconds.",
            header.digest(),
            header_creation_secs,
            digests.len(),
            avg_inclusion_secs,
        );

        // store and send newly built header
        let _ =
            Proposer::store_and_send_header(&header, proposer_store, tx_headers, &reason, metrics)
                .await?;

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
        proposer_store: ProposerStore<DB>,
        tx_headers: Sender<Header>,
        reason: String,
        metrics: Arc<PrimaryMetrics>,
    ) -> ProposerResult<Header> {
        let _ =
            Proposer::store_and_send_header(&header, proposer_store, tx_headers, &reason, metrics)
                .await?;

        Ok(header)
    }

    /// Store the header in the `ProposerStore` and send to `Certifier`.
    ///
    /// If `fatal_header_timeout` expires, this method is responsible. All other code is sync.
    async fn store_and_send_header(
        header: &Header,
        proposer_store: ProposerStore<DB>,
        tx_headers: Sender<Header>,
        reason: &str,
        metrics: Arc<PrimaryMetrics>,
    ) -> ProposerResult<()> {
        // Store the last header.
        proposer_store
            .write_last_proposed(header)
            .map_err(|e| ProposerError::StoreError(e.to_string()))?;

        #[cfg(feature = "benchmark")]
        for digest in header.payload().keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        // Send the new header to the `Certifier` that will broadcast and certify it.
        let result = tx_headers.send(header.clone()).await.map_err(|e| e.into());
        let num_digests = header.payload().len();
        metrics
            .num_of_batch_digests_in_header
            .with_label_values(&[reason])
            .observe(num_digests as f64);

        result
    }

    /// Calculate the max delay to use when resetting the max_delay_interval.
    ///
    /// The max delay is reduced when this authority expects to become the leader of the next round.
    /// Reducing the max delay increases its chance of being included in the DAG. Leaders are only
    /// elected on even rounds, so the normal max delay interval is used for odd rounds.
    fn calc_max_delay(&self) -> Duration {
        // check next round
        let next_round = self.round + 1;

        if next_round % 2 == 0
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
    /// NOTE: If the next round is even, the leader schedule is used to identify the next leader. If
    /// the next round is odd, the whole committee is used in order to keep the proposal rate as
    /// high as possible (which leads to a higher round rates). Using the entire committee here also
    /// helps boost scores for weaker nodes that may be trying to resync.
    fn calc_min_delay(&self) -> Duration {
        // check next round
        let next_round = self.round + 1;

        // compare:
        // - leader schedule for even rounds
        // - entire committee for odd rounds
        //
        // NOTE: committee size is asserted >1 during Committee::load()
        if next_round % 2 == 0 && self.leader_schedule.leader(next_round).id() == self.authority_id
        {
            Duration::ZERO
        } else if next_round % 2 != 0 && self.committee.leader(next_round).id() == self.authority_id
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
            self.last_parents.iter().find(|cert| cert.origin() == leader.id()).cloned();

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
            let stake = self.committee.stake_by_id(certificate.origin());
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

    /// Process certificates received for this round.
    ///
    /// If the certificates are valid, include them as parents for the next header.
    fn process_parents(&mut self, parents: Vec<Certificate>, round: Round) -> ProposerResult<()> {
        // Sanity check: verify provided certs are of the correct round & epoch.
        for parent in parents.iter() {
            if parent.round() != round {
                error!(target: "primary::proposer", "Proposer received certificate {parent:?} that failed to match expected round {round}. This should not be possible.");
            }
        }

        // Compare the parents' round number with our current round.
        match round.cmp(&self.round) {
            Ordering::Greater => {
                // proposer accepts a future round then jumps ahead in case it was
                // late (or just joined the network).
                self.round = round;
                // broadcast new round
                let _ = self.tx_narwhal_round_updates.send(self.round);
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
                debug!(
                    target: "primary::proposer",
                    "Proposer ignoring older parents, round={} parent.round={}",
                    self.round, round
                );
                // Ignore parents from older rounds.
            }
            Ordering::Equal => {
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
        debug!(target: "primary::proposer", advance_round=self.advance_round, round=self.round, "proposer checking if self.ready()...");
        self.advance_round = self.ready();
        debug!(target: "primary::proposer", advance_round=self.advance_round, round=self.round, "round status after checking conditions");

        // update metrics
        let round_type = if self.round % 2 == 0 { "even" } else { "odd" };
        self.metrics
            .proposer_ready_to_advance
            .with_label_values(&[&self.advance_round.to_string(), round_type])
            .inc();
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
    /// This method ensures worker blocks that were previously proposed but weren't committed are
    /// added back to the queue so their transactions are included in the next proposal.
    fn process_committed_headers(&mut self, commit_round: Round, committed_headers: Vec<Round>) {
        // remove committed headers from pending
        let mut max_committed_round = 0;
        for round in committed_headers {
            max_committed_round = max_committed_round.max(round);
            // try to remove round - log warning if round is missing
            if self.proposed_headers.remove(&round).is_none() {
                warn!("Proposer's own committed header not found at round {round}, probably because of restarts.");
            };
        }

        // re-insert batches for any proposed header from a round below the current commit
        //
        // ensure batches are FIFO to re-send them
        //
        // payloads: oldest -> newest
        let mut digests_to_resend = VecDeque::new();
        // Oldest to newest system messages.
        let mut system_messages_to_resend = Vec::new();
        // Oldest to newest rounds.
        let mut retransmit_rounds = Vec::new();

        // loop through proposed headers in order by round
        for (header_round, header) in &mut self.proposed_headers {
            // break once headers pass the committed round
            if *header_round > max_committed_round {
                break;
            }

            let mut system_messages = header.system_messages().to_vec();
            let mut digests = header
                .payload()
                .into_iter()
                .map(|(k, v)| ProposerDigest { digest: *k, worker_id: v.0, timestamp: v.1 })
                .collect();

            // add payloads and system messages from oldest to newest
            digests_to_resend.append(&mut digests);
            system_messages_to_resend.append(&mut system_messages);
            retransmit_rounds.push(*header_round);
        }

        // process rounds that need to be retransmitted
        if !retransmit_rounds.is_empty() {
            let num_digests_to_resend = digests_to_resend.len();
            let num_system_messages_to_resend = system_messages_to_resend.len();

            // prepend missing batches from previous round and update `self`
            digests_to_resend.append(&mut self.digests);
            self.digests = digests_to_resend;
            system_messages_to_resend.append(&mut self.system_messages);
            self.system_messages = system_messages_to_resend;

            // remove the old headers that failed
            // the proposed blocks are included in the next header
            for round in &retransmit_rounds {
                self.proposed_headers.remove(round);
            }

            // TODO: observe this warning and possibly reduce it to a debug
            warn!(
                target: "primary::proposer",
                "Repropose {num_digests_to_resend} worker blocks and {num_system_messages_to_resend} system messages in undelivered headers {retransmit_rounds:?} at commit round {commit_round:?}, remaining headers {}",
                self.proposed_headers.len()
            );

            self.metrics.proposer_resend_headers.inc_by(retransmit_rounds.len() as u64);
            self.metrics.proposer_resend_batches.inc_by(num_digests_to_resend as u64);
        }
    }

    /// Conditions are met to propose the next header.
    ///
    /// This method ensures proposer is protected against equivocation and sends the next header to
    /// the Certifier.
    ///
    /// If a different header was already produced for the same round, then
    /// this method returns the earlier header. Otherwise the newly created header is returned.
    fn propose_next_header(&mut self, reason: String) -> ProposerResult<PendingHeaderTask> {
        // Advance to the next round.
        self.round += 1;
        let _ = self.tx_narwhal_round_updates.send(self.round);

        debug!(target: "primary::proposer", round=self.round, "Proposer advanced round");

        // Update the metrics
        self.metrics.current_round.set(self.round as i64);
        let current_timestamp = now();
        if let Some(t) = &self.last_round_timestamp {
            self.metrics
                .proposal_latency
                .with_label_values(&[&reason])
                .observe(Duration::from_millis(current_timestamp - t).as_secs_f64());
        }
        self.last_round_timestamp = Some(current_timestamp);
        debug!("Dag moved to round {}", self.round);

        // oneshot channel to spawn a task
        let (tx, rx) = oneshot::channel();
        let current_epoch = self.committee.epoch();
        let current_round = self.round;

        // TODO: is this an unnecessary call for every proposal?
        // check if proposer store's last header is from this round
        let last_proposed = self
            .proposer_store
            .get_last_proposed()
            .map_err(|e| ProposerError::StoreError(e.to_string()))?;
        let possible_header_to_repropose =
            last_proposed.filter(|h| h.round() == current_round && h.epoch() == current_epoch);
        let proposer_store = self.proposer_store.clone();
        let tx_headers = self.tx_headers.clone();
        let metrics = self.metrics.clone();

        match possible_header_to_repropose {
            // resend header
            Some(header) => {
                warn!(target: "primary::proposer", current_round, current_epoch, header=?header, "reproposing header");
                tokio::task::spawn(async move {
                    // use this instead of store_and_send to because rx always expects a Header
                    let res = Proposer::repropose_header(
                        header,
                        proposer_store,
                        tx_headers,
                        reason,
                        metrics,
                    )
                    .await;
                    let _ = tx.send(res);
                });
            }
            // create new header
            None => {
                // collect values from &mut self for this header
                let num_of_digests = self.digests.len().min(self.max_header_num_of_batches);
                let digests: VecDeque<_> = self.digests.drain(..num_of_digests).collect();
                let system_messages = std::mem::take(&mut self.system_messages);
                let parents = std::mem::take(&mut self.last_parents);
                let authority_id = self.authority_id;
                let min_delay = self.min_header_delay; // copy
                let leader_and_support = if current_round % 2 == 0 {
                    let authority = self.leader_schedule.leader(current_round);
                    if self.authority_id == authority.id() {
                        "even_round_is_leader"
                    } else {
                        "even_round_not_leader"
                    }
                } else {
                    let authority = self.leader_schedule.leader(current_round - 1);
                    if parents.iter().any(|c| c.origin() == authority.id()) {
                        "odd_round_gives_support"
                    } else {
                        "odd_round_no_support"
                    }
                };

                // spawn tokio task to create, store, and send new header to certifier
                tokio::task::spawn(async move {
                    let proposal = Proposer::propose_header(
                        current_round,
                        current_epoch,
                        authority_id,
                        proposer_store,
                        tx_headers,
                        parents,
                        digests,
                        system_messages,
                        reason,
                        metrics,
                        leader_and_support.to_string(),
                        min_delay,
                    )
                    .await;

                    let _ = tx.send(proposal);
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
    ///
    /// This is the only time `Self::header_resend_timeout` gets reset.
    fn handle_proposal_result(
        &mut self,
        result: std::result::Result<ProposerResult<Header>, RecvError>,
    ) -> ProposerResult<()> {
        // receive result from oneshot channel
        let header = result.map_err(Into::into).and_then(|res| res)?;

        // track latest header
        self.opt_latest_header = Some(header.clone());
        // reset interval for header timeout
        self.fatal_header_timeout.reset();
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
}

/// The Future impl for proposer.
///
/// The future does the following:
/// - listen for shutdown receiver
/// - receive system messages to include in next proposed header
/// - receive own workers' block digests for proposing in own header
/// - receive a quorum of parents from the synchronizer for the previous round
/// - handle this primary's own committed headers
/// - propose the next header when conditions are right
/// - return an error if unable to send next header to certifier
impl<DB> Future for Proposer<DB>
where
    DB: Database,
{
    type Output = ProposerResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        debug!(target: "primary::proposer", round=this.round, "begin loop...");
        loop {
            // tick intervals to ensure they advance
            let max_delay_timed_out = this.max_delay_interval.poll_tick(cx).is_ready();
            let min_delay_timed_out = this.min_delay_interval.poll_tick(cx).is_ready();

            // check for shutdown signal
            //
            // okay to shutdown here because other primary tasks are expected to shutdown too
            // ie) no point completing the proposal if certifier is down
            if let Poll::Ready(Some(_shutdown)) = this.rx_shutdown_stream.poll_next_unpin(cx) {
                debug!(target: "primary::proposer", round=this.round, "Proposer received shutdown signal...");
                return Poll::Ready(Ok(()));
            }

            // check for new system messages
            if let Poll::Ready(Some(msg)) = this.rx_system_messages.poll_recv(cx) {
                debug!(target: "primary::proposer", round=this.round, "Proposer received system message");
                this.system_messages.push(msg);
            }

            // check for new digests from workers and send ack back to worker
            //
            // ack to worker implies that the block is recorded on the primary
            // and will be tracked until the block is included
            // ie) primary will attempt to propose this digest until it is
            // committed/sequenced in the DAG or the epoch concludes
            //
            // NOTE: this will not persist primary restarts
            while let Poll::Ready(Some(msg)) = this.rx_our_digests.poll_recv(cx) {
                debug!(target: "primary::proposer", round=this.round, "Proposer received digest");

                // parse message into parts
                let (ack, digest) = msg.process();
                let _ = ack.send(());
                this.digests.push_back(digest);
            }

            // check for new parent certificates
            // synchronizer sends collection of certificates when there is quorum (2f+1)
            while let Poll::Ready(Some((certs, round))) = this.rx_parents.poll_recv(cx) {
                debug!(target: "primary::proposer", this_round=this.round, parent_round=round, num_parents=certs.len(), "Proposer received parents");
                this.process_parents(certs, round)?;
            }

            // check for previous headers that were committed
            while let Poll::Ready(Some((commit_round, committed_headers))) =
                this.rx_committed_own_headers.poll_recv(cx)
            {
                debug!(target: "primary::proposer", round=this.round, "received committed update for own header");
                this.process_committed_headers(commit_round, committed_headers);
            }

            // poll receiver that returns proposed header result
            //
            // if the pending header needs more time, break loop and return pending
            // NOTE: proposer only holds one pending header at a time
            if let Some(mut receiver) = this.pending_header.take() {
                match receiver.poll_unpin(cx) {
                    Poll::Ready(res) => {
                        debug!(target: "primary::proposer", "pending header task complete!");
                        this.handle_proposal_result(res)?;

                        // continue the loop to propose the next header
                        continue;
                    }
                    Poll::Pending => {
                        // if still pending, check the fatal header timeout
                        //
                        // if fatal_header_timeout interval expires, then proposer was unable to
                        // send to certifier which is considered fatal and
                        // should never happen
                        //
                        // the only way this interval expires is if tx_headers.send() hangs
                        if this.fatal_header_timeout.poll_tick(cx).is_ready() {
                            error!(target: "primary::proposer", round=this.round, "Proposer header_resent_timeout triggered");
                            return Poll::Ready(Err(ProposerError::FatalHeaderTimeout(
                                this.fatal_header_timeout.period(),
                            )));
                        }

                        this.pending_header = Some(receiver);

                        // skip checking conditions for proposing next header
                        // since only one header is proposed at a time, there is no need to check
                        // intervals, parents, execution progress, etc.
                        break;
                    }
                }
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
            let enough_parents = !this.last_parents.is_empty();
            let enough_digests = this.digests.len() >= this.header_num_of_batches_threshold;

            // evaluate conditions for bool value
            let should_create_header = (max_delay_timed_out
                || ((enough_digests || min_delay_timed_out) && this.advance_round))
                && enough_parents;

            debug!(
                target: "primary::proposer",
                round=this.round,
                enough_parents,
                enough_digests,
                this.advance_round,
                min_delay_timed_out,
                max_delay_timed_out,
                should_create_header,
                pending_header=this.pending_header.is_some(),
                "Proposer polled...",
            );

            // if both conditions are met, create the next header
            if should_create_header {
                debug!(target: "primary::proposer", "proposing next header!");
                if max_delay_timed_out {
                    // expect this interval to expire occassionally
                    //
                    // if it expires too often, it either means some validators are Byzantine or
                    // that the network is experiencing periods of asynchrony
                    //
                    // periods of asynchrony possibly caused by misconfigured `max_header_delay`
                    warn!(target: "primary::proposer", interval=?this.max_delay_interval.period(), "max delay interval expired for round {}", this.round);
                }

                // obtain reason for metrics
                let reason = if max_delay_timed_out {
                    "max_timeout"
                } else if enough_digests {
                    "threshold_size_reached"
                } else {
                    "min_timeout"
                };

                // propose header
                let pending_header = this.propose_next_header(reason.to_string())?;
                this.pending_header = Some(pending_header);

                // ensure everything is caught up before poll pending
                continue;
            }

            debug!(target: "primary::proposer", "nothing to do - breaking loop");
            // break if unable to propose header
            break;
        }

        debug!(target: "primary::proposer", "outside loop - return Poll::Pending");
        Poll::Pending
    }
}
