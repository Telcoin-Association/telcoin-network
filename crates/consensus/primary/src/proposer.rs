// Copyright (c) Telcoin, LLC
// Copyright(C) Facebook, Inc. and its affiliates.
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The Proposer is responsible for proposing the primary's next header when certain conditions are met.
//!
//! This is the first task in the primary's header cycle. The Proposer processes messages from the
//! `Primary::StateHandler` to track which proposed headers were successfully committed. If a header
//! is not committed before it's round advances, the failed header's block digests are included in a
//! fresh header in FIFO order.
//!
//! Successfully created Headers are sent to the `Primary::Certifier`, where they are reliably broadcast
//! to voting peers. Headers are stored in the `ProposerStore` before they are sent to the Certifier.
//!
//! The Proposer is also responsible for processing Worker block's that reach quorum.
//! Collections of worker blocks that reach quorum are included in each header. If the Proposer's header
//! fails to be committed, then block digests from the failed round are included in the next header
//! once the Proposer's round advances.

use crate::{
    consensus::LeaderSchedule,
    error::{ProposerError, ProposerResult},
};
use consensus_metrics::{
    metered_channel::{Receiver, Sender},
    spawn_logged_monitored_task,
};
use fastcrypto::hash::Hash as _;
use futures::{FutureExt, StreamExt};
use indexmap::IndexMap;
use narwhal_primary_metrics::PrimaryMetrics;
use narwhal_storage::ProposerStore;
use narwhal_typed_store::traits::Database;
use reth_primitives::{BlockNumHash, B256};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};
use tn_types::{AuthorityIdentifier, Committee, Epoch, WorkerId};
use tokio_stream::wrappers::BroadcastStream;

use tn_types::{
    error::{DagError, DagResult},
    now, BatchDigest, Certificate, CertificateAPI, ConditionalBroadcastReceiver, Header, HeaderAPI,
    HeaderV1, Round, SystemMessage, TimestampSec,
};
use tokio::{
    sync::{
        oneshot::{self, error::RecvError},
        watch,
    },
    task::JoinHandle,
    time::{sleep, sleep_until, Duration, Instant, Interval},
};
use tracing::{debug, enabled, error, info, trace, warn};

/// Type alias for the async task that creates, stores, and sends the proposer's new header.
type PendingHeaderTask = oneshot::Receiver<ProposerResult<Header>>;

/// Messages sent to the proposer about this primary's own workers' block digests
#[derive(Debug)]
pub struct OurDigestMessage {
    /// The digest for the worker's block that reached quorum.
    pub digest: BatchDigest,
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
    pub digest: BatchDigest,
    /// The worker that produced this block.
    pub worker_id: WorkerId,
    /// The timestamp for when the block was created.
    pub timestamp: TimestampSec,
}

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

const DEFAULT_HEADER_RESEND_TIMEOUT: Duration = Duration::from_secs(60);

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
    min_delay_timer: Interval,
    /// The maximum interval measured for conditions like having leader in parents.
    max_delay_timer: Interval,
    /// The delay to wait until resending the last proposed header if proposer
    /// hasn't proposed anything new since then.
    header_resend_timeout: Interval,
    /// The latest header.
    opt_latest_header: Option<Header>,
    /// Receiver for shutdown.
    rx_shutdown_stream: BroadcastStream<()>,
    /// Receives the parents to include in the next header (along with their round number) from
    /// core.
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
    /// The watch channel for observing execution results.
    ///
    /// Proposer must include the finalized parent hash from the previously executed round to ensure execution results are consistent.
    execution_result: watch::Receiver<BlockNumHash>,
    /// Flag if enough conditions are met to advance the round.
    advance_round: bool,
    /// The optional pending header that the proposer has decided to build.
    ///
    /// This value is `Some` when conditions are met to propose the next header.
    /// The task within is responsible for creating, storing, and sending the new header.
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
        header_resend_timeout: Option<Duration>,
        rx_shutdown: ConditionalBroadcastReceiver,
        rx_parents: Receiver<(Vec<Certificate>, Round)>,
        rx_our_digests: Receiver<OurDigestMessage>,
        rx_system_messages: Receiver<SystemMessage>,
        tx_headers: Sender<Header>,
        tx_narwhal_round_updates: watch::Sender<Round>,
        rx_committed_own_headers: Receiver<(Round, Vec<Round>)>,
        metrics: Arc<PrimaryMetrics>,
        leader_schedule: LeaderSchedule,
        execution_result: watch::Receiver<BlockNumHash>,
    ) -> Self {
        // TODO: include EL genesis hash in committee for epoch?
        //
        // NO: bc the first round should include EL genesis hash in primary proposed header.
        let genesis = Certificate::genesis(&committee);
        let header_resend_timeout = header_resend_timeout.unwrap_or(DEFAULT_HEADER_RESEND_TIMEOUT);
        // create min/max delay intervals
        let min_delay_timer = tokio::time::interval(min_header_delay);
        let max_delay_timer = tokio::time::interval(max_header_delay);
        let header_resend_timeout = tokio::time::interval(header_resend_timeout);
        let rx_shutdown_stream = BroadcastStream::new(rx_shutdown.receiver);

        Self {
            authority_id,
            committee,
            header_num_of_batches_threshold,
            max_header_num_of_batches,
            min_header_delay,
            max_header_delay,
            min_delay_timer,
            max_delay_timer,
            header_resend_timeout,
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
            execution_result,
            advance_round: true,
            pending_header: None,
        }
    }

    /// Make a new header, store it in the proposer store, and forward it to the certifier.
    ///
    /// This task is spawned outside of `Self`.
    ///
    /// - current_header: caller checks to see if there is already a header
    ///   built for this round. If current_header.is_some() the proposer
    ///   uses this header instead of building a new one.
    async fn g_propose_header(
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
            let drift_ms = parent_max_time - current_time;
            error!(
                "Current time {} earlier than max parent time {}, sleeping for {}ms until max parent time.",
                current_time, parent_max_time, drift_ms,
            );
            metrics.header_max_parent_wait_ms.inc_by(drift_ms);
            sleep(Duration::from_millis(drift_ms)).await;
        }

        let header: Header = HeaderV1::new(
            authority_id,
            current_round,
            current_epoch,
            digests.iter().map(|m| (m.digest, (m.worker_id, m.timestamp))).collect(),
            system_messages.clone(),
            parents.iter().map(|x| x.digest()).collect(),
        )
        .into();

        metrics.headers_proposed.with_label_values(&[&leader_and_support]).inc();
        metrics.header_parents.observe(parents.len() as f64);

        if enabled!(tracing::Level::TRACE) {
            let mut msg = format!("Created header {header:?} with parent certificates:\n");
            for parent in parents.iter() {
                msg.push_str(&format!("{parent:?}\n"));
            }
            trace!(msg);
        } else {
            debug!("Created header {header:?}");
        }

        // Update metrics related to latency
        let mut total_inclusion_secs = 0.0;
        for digest in &digests {
            let batch_inclusion_secs =
                Duration::from_millis(*header.created_at() - digest.timestamp).as_secs_f64();
            total_inclusion_secs += batch_inclusion_secs;

            // NOTE: This log entry is used to compute performance.
            tracing::debug!(
                    "Batch {:?} from worker {} took {} seconds from creation to be included in a proposed header",
                    digest.digest,
                    digest.worker_id,
                    batch_inclusion_secs
                );
            metrics.proposer_batch_latency.observe(batch_inclusion_secs);
        }

        // TODO: make this a metric if really necessary
        // otherwise, remove this calculation
        //
        // // NOTE: This log entry is used to compute performance.
        // let (header_creation_secs, avg_inclusion_secs) = if let Some(digest) = digests.front() {
        //     (
        //         Duration::from_millis(*header.created_at() - digest.timestamp).as_secs_f64(),
        //         total_inclusion_secs / digests.len() as f64,
        //     )
        // } else {
        //     (self.max_header_delay.as_secs_f64(), 0.0)
        // };
        // debug!(
        //     "Header {:?} was created in {} seconds. Contains {} batches, with average delay {} seconds.",
        //     header.digest(),
        //     header_creation_secs,
        //     digests.len(),
        //     avg_inclusion_secs,
        // );

        // TODO: is this metric measured elsewhere?
        // self.metrics.proposer_batch_latency.observe(batch_inclusion_secs);

        // store and send newly built header
        let _ = Proposer::g_store_and_send_header(
            &header,
            proposer_store,
            tx_headers,
            &reason,
            metrics,
        )
        .await?;

        Ok(header)
    }
    async fn g_repropose_header(
        header: Header,
        proposer_store: ProposerStore<DB>,
        tx_headers: Sender<Header>,
        reason: String,
        metrics: Arc<PrimaryMetrics>,
    ) -> ProposerResult<Header> {
        let _ = Proposer::g_store_and_send_header(
            &header,
            proposer_store,
            tx_headers,
            &reason,
            metrics,
        )
        .await?;

        Ok(header)
    }

    async fn g_store_and_send_header(
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

    fn max_delay(&self) -> Duration {
        // If this node is going to be the leader of the next round, we set a lower max
        // timeout value to increase its chance of being included in the dag. As leaders are elected
        // on even rounds only we apply the reduced max delay only for those ones.
        if (self.round + 1) % 2 == 0
            && self.leader_schedule.leader(self.round + 1).id() == self.authority_id
        {
            self.max_header_delay / 2
        } else {
            self.max_header_delay
        }
    }

    fn min_delay(&self) -> Duration {
        // TODO: consider even out the boost provided by the even/odd rounds so we avoid perpetually
        // boosting the nodes and affect the scores.
        // If this node is going to be the leader of the next round and there are more than
        // 1 primary in the committee, we use a lower min delay value to increase the chance
        // of committing the leader. Pay attention that we use here the leader_schedule to figure
        // out the next leader.
        let next_round = self.round + 1;
        if self.committee.size() > 1
            && next_round % 2 == 0
            && self.leader_schedule.leader(next_round).id() == self.authority_id
        {
            return Duration::ZERO;
        }

        // Give a boost on the odd rounds to a node by using the whole committee here, not just the
        // nodes of the leader_schedule. By doing this we keep the proposal rate as high as possible
        // which leads to higher round rate and also acting as a score booster to the less strong
        // nodes as well.
        if self.committee.size() > 1
            && next_round % 2 != 0
            && self.committee.leader(next_round).id() == self.authority_id
        {
            return Duration::ZERO;
        }
        self.min_header_delay
    }

    /// Update the last leader certificate.
    fn update_leader(&mut self) -> bool {
        let leader = self.leader_schedule.leader(self.round);
        self.last_leader = self
            .last_parents
            .iter()
            .find(|x| {
                if x.origin() == leader.id() {
                    debug!("Got leader {:?} for round {}", x, self.round);
                    true
                } else {
                    false
                }
            })
            .cloned();

        self.last_leader.is_some()
    }

    /// Check whether if this validator is the leader of the round, or if we have
    /// (i) f+1 votes for the leader, (ii) 2f+1 nodes not voting for the leader,
    /// (iii) there is no leader to vote for.
    fn enough_votes(&self) -> bool {
        if self.leader_schedule.leader(self.round + 1).id() == self.authority_id {
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

        let mut enough_votes = votes_for_leader >= self.committee.validity_threshold();
        enough_votes |= no_votes >= self.committee.quorum_threshold();
        enough_votes
    }

    /// Whether we can advance the DAG or need to wait for the leader/more votes.
    /// Note that if we timeout, we ignore this check and advance anyway.
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
                error!("Proposer received certificate {parent:?} that failed to match expected round {round}. This should not be possible.");
            }
        }

        // Compare the parents' round number with our current round.
        match round.cmp(&self.round) {
            Ordering::Greater => {
                // proposer accepts a future round then jumps ahead in case it was
                // late (or just joined the network).
                self.round = round;
                let _ = self.tx_narwhal_round_updates.send(self.round);
                self.last_parents = parents;

                // Reset advance flag.
                self.advance_round = false;

                // Extend max_delay_timer to properly wait for the previous round's leader
                //
                // min_delay_timer should not be extended: the network moves at
                // the interval of min_header_delay. Delaying header creation for
                // another min_header_delay after receiving parents from a higher
                // round and cancelling proposing, makes it very likely that higher
                // round parents will be received and header creation will be cancelled
                // again. So min_delay_timer is disabled to get the proposer in sync
                // with the quorum.
                // If the node becomes leader, disabling min_delay_timer to propose as
                // soon as possible is the right thing to do as well.
                let timer_start = Instant::now();

                // TODO: how to do this with interval instead of sleep?

                // self.max_delay_timer.reset();
                // self.min_delay_timer.reset();

                todo!()
            }
            Ordering::Less => {
                debug!(
                    "Proposer ignoring older parents, round={} parent.round={}",
                    self.round, round
                );
                // Ignore parents from older rounds.
            }
            Ordering::Equal => {
                // The core gives us the parents the first time they are enough to form a quorum.
                // Then it keeps giving us all the extra parents.
                self.last_parents.extend(parents);

                // As the schedule can change after an odd round proposal - when the new schedule algorithm is
                // enabled - make sure that the timer is reset correctly for the round leader. No harm doing
                // this here as well.
                if self.min_delay() == Duration::ZERO {
                    min_delay_timer.as_mut().reset(Instant::now());
                }
            }
        }

        // Check whether we can advance to the next round. Note that if we timeout,
        // we ignore this check and advance anyway.
        self.advance_round = if self.ready() {
            if !self.advance_round {
                debug!(target: "primary::proposer", "Ready to advance from round {}", self.round,);
            }
            true
        } else {
            false
        };
        debug!(target: "primary::proposer", advance_round=self.advance_round, round=self.round, "Proposer processed parents result");

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
    /// This method ensures worker blocks that were previously proposed but weren't committed are reproposed.
    fn process_committed_headers(&mut self, commit_round: Round, committed_headers: Vec<Round>) {
        // remove committed headers from pending
        let mut max_committed_round = 0;
        for round in committed_headers {
            max_committed_round = max_committed_round.max(round);
            // try to remove - log warning if round is missing
            if self.proposed_headers.remove(&round).is_none() {
                warn!("Proposer's own committed header not found at round {round}, probably because of restarts.");
            };
        }

        // Now for any round below the current commit round we re-insert
        // the batches into the digests we need to send, effectively re-sending
        // them in FIFO order.

        // re-insert batches for any proposed header from a round below the current commit
        //
        // ensure batches are FIFO as this is effectively re-sending them
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

            // Add payloads and system messages from oldest to newest.
            digests_to_resend.append(&mut digests);
            system_messages_to_resend.append(&mut system_messages);
            retransmit_rounds.push(*header_round);
        }

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
    /// Update Self and make the header to propose.
    ///
    /// This method ensures proposer is protected against equivocation.
    ///
    /// If a different header was already produced for the same round, then
    /// this method returns the earlier header. Otherwise the newly created header is returned.
    fn propose_next_header(&mut self, reason: String) -> ProposerResult<PendingHeaderTask> {
        //
        // TODO: borrow and update watch channel to include EL data
        //

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
                tokio::task::spawn(async move {
                    // use this instead of store_and_send to because rx always expects a Header
                    let res = Proposer::g_repropose_header(
                        header,
                        proposer_store,
                        tx_headers,
                        reason,
                        metrics,
                    )
                    .await;
                    tx.send(res);
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
                    let proposal = Proposer::g_propose_header(
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
                    )
                    .await;

                    tx.send(proposal);
                });
            }
        }

        // return receiver to advance task
        Ok(rx)
    }

    /// The oneshot channel is ready, indicating a result from the header proposal process.
    fn handle_proposal_result(
        &mut self,
        result: std::result::Result<ProposerResult<Header>, RecvError>,
    ) -> ProposerResult<()> {
        // receive result from oneshot channel
        let header = result.map_err(Into::into).and_then(|res| res)?;

        // track latest header
        self.opt_latest_header = Some(header.clone());

        // reset interval for header timeout
        self.header_resend_timeout.reset();

        // Reset advance flag.
        self.advance_round = false;

        //
        // TODO: ensure new methods for Intances are equivalent to old sleep approach:
        //
        // Reschedule the timer.
        // let timer_start = Instant::now();
        //
        // max_delay_timer.as_mut().reset(timer_start + self.max_delay());
        // min_delay_timer.as_mut().reset(timer_start + self.min_delay());

        self.min_delay_timer.reset();
        self.max_delay_timer.reset();

        // track header so proposer can repropose the digests and system messages
        // if this header fails to be committed for some reason
        self.proposed_headers.insert(header.round(), header);

        Ok(())
    }
}

/// The Future impl for proposer.
///
/// The future is responsible for:
/// - re-propose the header if the repeat timer expires
/// - handle this primary's own committed headers
/// - update parent count when peer certificates received
/// - process own workers' block digests for proposing in own header
/// - process system messages to include in proposed header
/// - listen for shutdown receiver
impl<DB> Future for Proposer<DB>
where
    DB: Database + Unpin,
{
    type Output = ProposerResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        info!(
            target: "primary::proposer",
            "Proposer on node {} has started successfully with header resend timeout {:?}.",
            this.authority_id, this.header_resend_timeout
        );

        loop {
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
            if let Poll::Ready(Some(msg)) = this.rx_our_digests.poll_recv(cx) {
                debug!(target: "primary::proposer", round=this.round, "Proposer received digest");

                // parse message into parts
                let (ack, digest) = msg.process();
                let _ = ack.send(());
                this.digests.push_back(digest);
            }

            // check for new parent certificates
            if let Poll::Ready(Some((certs, round))) = this.rx_parents.poll_recv(cx) {
                debug!(target: "primary::proposer", this_round=this.round, parent_round=round, num_parents=certs.len(), "Proposer received parents");
                this.process_parents(certs, round)?;
            }

            // TODO: use `while` instead of `if`?
            // - should be up-to-date all the time, but better to ensure Proposer is caught up
            //
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
                        this.handle_proposal_result(res);

                        // continue the loop to propose the next header
                        continue;
                    }
                    Poll::Pending => {
                        this.pending_header = Some(receiver);

                        // skip checking conditions for proposing next header
                        // since only one header is proposed at a time, there is no need to check
                        // timers, parents, execution progress, etc.
                        break;
                    }
                }
            }

            // proposer doesn't have a pending header
            //
            // TODO:
            //
            // if this timer goes off, then certifier confirmed receipt but
            // proposer never received that this header reached quorum ??? (committed??)
            //
            // where does `rx_committed_own_headers` get messages within this loop?
            //
            // check if the resent timeout has elapsed
            if this.header_resend_timeout.poll_tick(cx).is_ready() {
                warn!(target: "primary::proposer", round=this.round, "Proposer header_resent_timeout triggered");
                todo!()
            }

            // Check if conditions are met for proposing a new header
            //
            // New headers are proposed when:
            //
            // 1) a quorum of parents (certificates) received for the current round
            // 2) the execution layer successfully executed the previous round (parent hash)
            // 3) One of the following:
            // - the timer expired
            //  - this primary timed out on the leader
            //  - or quit trying to gather enough votes for the leader
            // - the worker created enough blocks (header_num_of_batches_threshold)
            //  - this is happy path
            //  - vote for leader or leader already has enough votes to trigger commit
            let enough_parents = !this.last_parents.is_empty();
            let execution_complete = this.execution_result.has_changed()?;
            let max_delay_timed_out = this.max_delay_timer.poll_tick(cx).is_ready();
            let min_delay_timed_out = this.min_delay_timer.poll_tick(cx).is_ready();
            let enough_digests = this.digests.len() >= this.header_num_of_batches_threshold;

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
                ?execution_complete,
                "Proposer polled...",
            );

            // if both conditions are met, create the next header
            if should_create_header && execution_complete {
                if max_delay_timed_out {
                    // It is expected that this timer expires from time to time. If it expires too
                    // often, it either means some validators are Byzantine or
                    // that the network is experiencing periods of asynchrony.
                    //
                    // In practice, the latter scenario means we misconfigured the parameter
                    // called `max_header_delay`.
                    warn!(target: "primary::proposer", "Timer expired for round {}", this.round);
                }

                // obtain reason for metrics
                let reason = if max_delay_timed_out {
                    "max_timeout"
                } else if enough_digests {
                    "threshold_size_reached"
                } else {
                    "min_timeout"
                };

                this.propose_next_header(reason.to_string());

                // Recheck condition and reset time out flags.
                continue;
            }
        }

        Poll::Pending
    }
}
