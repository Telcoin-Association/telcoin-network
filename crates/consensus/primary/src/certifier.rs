//! Certifier broadcasts headers and certificates for this primary.

use crate::{
    aggregators::VotesAggregator,
    network::{PrimaryNetworkHandle, RequestVoteResult},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use std::{sync::Arc, time::Duration};
use tn_config::{ConsensusConfig, KeyConfig};
use tn_network_libp2p::error::NetworkError;
use tn_storage::{tables::ProposedCertificates, CertificateStore};
use tn_types::{
    ensure,
    error::{DagError, DagResult},
    AuthorityIdentifier, BlsPublicKey, Certificate, Committee, Database, Header, HeaderDigest,
    Noticer, Notifier, TaskError, TaskManager, TaskResult, TaskSpawner, TnReceiver, Vote,
};
use tokio::sync::{mpsc, Mutex, Notify};
use tracing::{debug, enabled, error, info, instrument, warn};

#[cfg(test)]
#[path = "tests/certifier_tests.rs"]
mod certifier_tests;

/// The vote-request attempt from which the retry delay in [`Certifier::request_vote`] stays at
/// its 10 s ceiling.
///
/// A request still failing at this attempt has used up the fast retries and is logged once at
/// warn; the requests after it keep retrying at the ceiling until the proposal is superseded.
const VOTE_RETRY_CEILING_ATTEMPT: u32 = 7;

/// This component is responisble for proposing headers to peers, collecting votes on headers,
/// and certifying headers into certificates.
///
/// It receives headers to propose from Proposer via `rx_headers`, and publishes certificates to
/// gossip network.
#[derive(Clone)]
pub(crate) struct Certifier<DB> {
    /// The identifier of this primary.
    authority_id: AuthorityIdentifier,
    /// The committee information.
    committee: Committee,
    /// The persistent storage keyed to certificates.
    certificate_store: DB,
    /// Handles synchronization with other nodes and our workers.
    state_sync: StateSynchronizer<DB>,
    /// Service to sign headers.
    signature_service: KeyConfig,
    /// Consensus config to subscribe to shutdown.
    config: ConsensusConfig<DB>,
    /// A network sender to send the batches to the other workers.
    network: PrimaryNetworkHandle,
    /// Spawn epoch-related tasks.
    task_spawner: TaskSpawner,
    /// Notifier to cancel pending proposals and vote requests if a different header is received.
    new_proposal: Notifier,
    /// Lock to make sure we are only in one header proposal at a time.
    /// Should not generally happen but can lead to leader cert equivocation
    /// if it does so be really sure.
    proposal_lock: Arc<Mutex<()>>,
    /// The digest of the header whose proposal task is still running, if any, and the signal that
    /// asks that proposal to re-issue the vote requests that ended in an error.
    ///
    /// Set by [`Self::run`] before it spawns a proposal and cleared when that task ends, however
    /// it ends (see [`InFlightProposal`]).
    in_flight: InFlightMarker,
    /// Prometheus metrics for vote collection and certificate formation.
    metrics: crate::PrimaryMetrics,
}

impl<DB: Database> Certifier<DB> {
    /// Spawn the long-running certifier task.
    pub(crate) fn spawn(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        state_sync: StateSynchronizer<DB>,
        primary_network: PrimaryNetworkHandle,
        task_manager: &TaskManager,
    ) {
        // return early if not CVV
        let Some(authority_id) = config.authority_id() else {
            // If we don't have an authority id then we are not a validator and should not be
            // proposing anything...
            return;
        };

        // spawn long-running task to gossip own certificates
        let task_spawner = task_manager.get_spawner();
        let metrics = consensus_bus.app().metrics().clone();
        // Subscribe before spawning so the channel is active before any messages are sent.
        let rx_headers = consensus_bus.subscribe_headers();
        task_manager.spawn_critical_task("certifier task", async move {
            let highest_created_certificate = config
                .node_storage()
                .last_round(&authority_id)
                .expect("certificate store available");
            debug!(
                target: "epoch-manager",
                ?highest_created_certificate,
                "restoring certifier with highest created certificate for epoch {}",
                config.epoch(),
            );

            // publish last certificate on startup
            if let Some(cert) = highest_created_certificate {
                if let Err(e) = primary_network.publish_certificate(cert).await {
                    error!(target: "primary::certifier", ?e, "failed to publish highest created certificate gossip during startup");
                }
            }

            info!(target: "primary::certifier", "Certifier on node {:?} has started successfully.", authority_id);

            let res = Self {
                authority_id: authority_id.clone(),
                committee: config.committee().clone(),
                certificate_store: config.node_storage().clone(),
                state_sync,
                signature_service: config.key_config().clone(),
                config,
                network: primary_network,
                task_spawner,
                new_proposal: Notifier::new(),
                proposal_lock: Arc::new(Mutex::new(())),
                in_flight: Arc::default(),
                metrics,
            }
            .run(rx_headers)
            .await;
            info!(target: "primary::certifier", "Certifier on node {} has shutdown.", authority_id);
            res
        });
    }

    /// Requests a vote for a Header from the given peer. Retries indefinitely until either a
    /// vote is received, or a permanent error is returned.
    #[instrument(level = "debug", skip_all, fields(peer = ?authority, round = header.round()))]
    async fn request_vote(
        authority: AuthorityIdentifier,
        header: Header,
        peer_id: BlsPublicKey,
        certificate_store: DB,
        network: PrimaryNetworkHandle,
        committee: Committee,
        cancel_proposal: Noticer,
    ) -> DagResult<Vote> {
        let mut missing_parents: Option<Vec<HeaderDigest>> = None;
        let mut attempt: u32 = 0;
        debug!(target: "primary::certifier", ?authority, ?header, "requesting vote for header...");

        // loop until vote received
        let vote: Vote = loop {
            // increase attempt count
            attempt += 1;

            // peers may respond to a vote requesting missing parents
            let parents = missing_parents.map(|missing_parents| {
                // collect missing parents requested by peer in order to vote for this header
                let expected_count = missing_parents.len();
                let parents: Vec<_> = certificate_store
                    .read_all(
                        missing_parents
                            .into_iter()
                            // only provide certs that are parents for the requested vote
                            .filter(|parent| header.parents().contains(parent)),
                    )?
                    .into_iter()
                    .flatten()
                    .collect();

                // sanity check for missing parents
                if parents.len() != expected_count {
                    error!(
                        target: "primary::certifier",
                        "tried to read {expected_count} missing certificates requested by remote primary for vote request, but only found {}",
                        parents.len()
                    );
                    return Err(DagError::ProposedHeaderMissingCertificates);
                }

                Ok(parents)
            }).unwrap_or(Ok(vec![]))?;

            // listen for requests from peers
            tokio::select! {
                vote_result = network.request_vote(peer_id, header.clone(), parents) => {
                    // process response from peer
                    match vote_result {
                        Ok(RequestVoteResult::Vote(vote)) => {
                            debug!(target: "primary::certifier", ?authority, ?vote, "Ok response received after request vote");
                            // happy path - vote recieved
                            break vote;
                        }
                        Ok(RequestVoteResult::MissingParents(parents)) => {
                            debug!(target: "primary::certifier", ?authority, ?parents, "Ok missing parents response received after request vote");
                            // retrieve missing parents so peer can vote
                            missing_parents = Some(parents);
                        }
                        Err(error) => {
                            if let NetworkError::RPCError(error) = error {
                                error!(target: "primary::certifier", ?authority, ?error, ?header, "fatal request for requested vote");
                                return Err(DagError::NetworkError(format!(
                                    "irrecoverable error requesting vote for {header}: {error}"
                                )));
                            }
                            // retries are unbounded until the proposal is superseded, so a peer
                            // that keeps failing must not produce a line per attempt
                            if attempt == VOTE_RETRY_CEILING_ATTEMPT {
                                warn!(
                                    target: "primary::certifier",
                                    ?authority,
                                    ?error,
                                    header = %header.digest(),
                                    attempt,
                                    "vote request still failing after the fast retries; retrying every 10s until the proposal is superseded"
                                );
                            } else if attempt.is_power_of_two() {
                                debug!(
                                    target: "primary::certifier",
                                    ?authority,
                                    ?error,
                                    header = %header.digest(),
                                    attempt,
                                    "retryable error requesting vote"
                                );
                            }

                            missing_parents = None;
                        }
                    }
                }

                // cancel vote request
                _ = &cancel_proposal => {
                    return Err(DagError::Canceled);
                }
            }

            // Retry delay. Using custom values here because pure exponential backoff is hard to
            // configure without it being either too aggressive or too slow. We want the first
            // retry to be instantaneous, next couple to be fast, and to slow quickly thereafter.
            tokio::time::sleep(Duration::from_millis(match attempt {
                1 => 0,
                2 => 100,
                3 => 500,
                4 => 1_000,
                5 => 2_000,
                6 => 5_000,
                // from VOTE_RETRY_CEILING_ATTEMPT on
                _ => 10_000,
            }))
            .await;
        };

        // verify the vote (bls signature over header digest)
        ensure!(
            vote.header_digest() == header.digest()
                && vote.origin() == header.author()
                && vote.author() == &authority,
            DagError::UnexpectedVote(vote.header_digest())
        );

        // possible equivocations
        ensure!(
            header.epoch() == vote.epoch(),
            DagError::InvalidEpoch { expected: header.epoch(), received: vote.epoch() }
        );
        ensure!(
            header.round() == vote.round(),
            DagError::InvalidRound { expected: header.round(), received: vote.round() }
        );

        // ensure the vote is from the correct epoch
        ensure!(
            vote.epoch() == committee.epoch(),
            DagError::InvalidEpoch { expected: committee.epoch(), received: vote.epoch() }
        );

        // ensure the authority has voting rights
        ensure!(
            committee.voting_power_by_id(vote.author()) > 0,
            DagError::UnknownAuthority(vote.author().to_string())
        );

        Ok(vote)
    }

    /// Propose a header produced by this authority.
    ///
    /// Each wake of `reissue_failed` asks again every peer whose vote request has ended in an error
    /// since the last wake. The requests still in flight and the votes already received are kept.
    #[instrument(level = "debug", skip_all, fields(round = header.round(), epoch = header.epoch()))]
    async fn propose_header(
        &self,
        header: Header,
        reissue_failed: &Notify,
    ) -> DagResult<Certificate> {
        debug!(target: "primary::certifier", auth=?self.authority_id, "proposing header");
        let proposal_start = std::time::Instant::now();

        // only propose headers in current epoch
        if header.epoch() != self.committee.epoch() {
            error!(
                target: "primary::certifier",
                "Certifier received mismatched header proposal for epoch {}, currently at epoch {}",
                header.epoch(),
                self.committee.epoch()
            );
            return Err(DagError::InvalidEpoch {
                expected: self.committee.epoch(),
                received: header.epoch(),
            });
        }

        // subscribe early for shutdown notifications
        let cancel_proposal = self.new_proposal.subscribe();

        // reset the votes aggregator and sign own header
        let mut votes_aggregator = VotesAggregator::new();
        let vote = Vote::new(&header, self.authority_id.clone(), &self.signature_service);
        let mut certificate = votes_aggregator.append(vote, &self.committee, &header)?;

        // create a channel for receiving votes from peers. this method keeps a sender to re-issue
        // failed requests, so the channel never closes: `outstanding` counts the vote tasks that
        // have not reported yet
        let (tx_votes, mut rx_votes) = mpsc::unbounded_channel();
        let mut outstanding = 0usize;
        for (name, target) in self.committee.others_primaries_by_id(Some(&self.authority_id)) {
            self.spawn_vote_request(&header, name, target, &tx_votes);
            outstanding += 1;
        }

        // the peers whose vote request ended in an error since the last re-issue
        let mut failed = Vec::new();

        // loop through requests until complete or cancelled
        loop {
            // certificate created - no more votes needed, or every vote task has reported
            if certificate.is_some() || outstanding == 0 {
                break;
            }

            // receive votes or exit early if new proposal replaces this header before certification
            tokio::select! {
                Some((name, target, result)) = rx_votes.recv() => {
                    debug!(target: "primary::certifier", auth=?self.authority_id, ?result, "next request in unordered futures");
                    outstanding -= 1;

                    match result {
                        // happy path
                        Ok(vote) => {
                            let authority_id = vote.author.clone();
                            self.metrics.votes_received_total.increment(1);
                            // prevent invalid votes from derailing certification process
                            certificate = match votes_aggregator.append(
                                vote,
                                &self.committee,
                                &header,
                            ) {
                                Ok(cert) => cert,
                                Err(e) => {
                                    error!(target: "primary::certifier", "received an invalid vote from {authority_id:?}: {e:?}");
                                    self.metrics.invalid_votes_total.increment(1);
                                    None
                                }
                            }
                        },

                        // handle vote error
                        Err(e) => {
                            error!(
                                target: "primary::certifier",
                                auth=?self.authority_id,
                                "failed to get vote for header {header:?}: {e:?}"
                            );
                            self.metrics.vote_request_failures_total.increment(1);
                            // a cancelled request belongs to a proposal that is being replaced
                            if !matches!(e, DagError::Canceled) {
                                failed.push((name, target));
                            }
                        }
                    }
                },

                // the identical header was re-sent: a peer whose request ended in an error may
                // answer differently now, for example once it can map this node's network key
                _ = reissue_failed.notified() => {
                    if !failed.is_empty() {
                        debug!(target: "primary::certifier", auth=?self.authority_id, peers = failed.len(), "re-issuing failed vote requests");
                    }
                    for (name, target) in failed.drain(..) {
                        self.spawn_vote_request(&header, name, target, &tx_votes);
                        outstanding += 1;
                    }
                },

                // exit early when cancel notification received
                _ = &cancel_proposal => {
                    debug!(target: "primary::certifier", "new proposal received - aborting proposal...");
                    return Err(DagError::Canceled);
                }
            }
        }

        // log detailed header info if we failed to form a certificate
        let certificate = certificate.ok_or_else(|| {
            if enabled!(tracing::Level::WARN) {
                let mut msg = format!(
                    "Failed to form certificate from header {header:#?} with parent certificates:"
                );
                for parent_digest in header.parents().iter() {
                    let parent_msg = match self.certificate_store.read(*parent_digest) {
                        Ok(Some(cert)) => format!("{cert:#?}\n"),
                        Ok(None) => {
                            format!("missing certificate for digest {parent_digest:?}")
                        }
                        Err(e) => format!(
                            "error retrieving certificate for digest {parent_digest:?}: {e:?}"
                        ),
                    };
                    msg.push_str(&parent_msg);
                }
                error!(target: "primary::certifier", auth=?self.authority_id, msg, "inside propose_header");
            }
            DagError::CouldNotFormCertificate(header.digest())
        })?;

        debug!(target: "primary::certifier", auth=?self.authority_id, "Assembled {certificate:?}");

        self.metrics.certificates_formed_total.increment(1);
        self.metrics.certificate_form_duration_seconds.record(proposal_start.elapsed());

        Ok(certificate)
    }

    /// Spawn a task that asks the peer `name`, at network key `target`, for its vote on `header`.
    ///
    /// The task reports the outcome on `tx_votes` together with the peer, so a request that ended
    /// in an error can be issued again.
    fn spawn_vote_request(
        &self,
        header: &Header,
        name: AuthorityIdentifier,
        target: BlsPublicKey,
        tx_votes: &mpsc::UnboundedSender<(AuthorityIdentifier, BlsPublicKey, DagResult<Vote>)>,
    ) {
        let header = header.clone();
        let tx_votes = tx_votes.clone();
        let network = self.network.clone();
        let certificate_store = self.certificate_store.clone();
        let committee = self.committee.clone();
        let cancel_proposal = self.new_proposal.subscribe();
        let task_name = format!("vote-{header:?}-{name}");
        self.task_spawner.spawn_task(task_name, async move {
            // this will exit early on cancel_proposal
            let result = Self::request_vote(
                name.clone(),
                header,
                target,
                certificate_store,
                network,
                committee,
                cancel_proposal,
            )
            .await;
            let _ = tx_votes.send((name, target, result));
            Ok(())
        });
    }

    /// The method to spawn tasks related to a header proposal.
    ///
    /// This listens for new proposal notifications to exit early.
    /// The method returns once enough votes are processed to certify the proposal,
    /// or if a new proposal arrives.
    ///
    /// `new_proposal_noticer` must be subscribed to `new_proposal` before the task running this
    /// method is spawned (see [`Self::run`]), so a later header's notification cannot land before
    /// it exists. `reissue_failed` is passed on to [`Self::propose_header`].
    async fn spawn_header_proposal(
        self,
        header: Header,
        new_proposal_noticer: Noticer,
        reissue_failed: &Notify,
    ) -> TaskResult {
        // Make sure other proposal's are shutdown and done before we check if
        // this header is already certified.  Any existing proposals should have been cancelled
        // before this call.
        let _guard = self.proposal_lock.lock().await;
        let header_digest = header.digest();
        // a later header superseded this one while it waited for the lock. the select below
        // polls its branches in random order, so without this check a superseded header could
        // still spawn its vote requests
        if new_proposal_noticer.noticed() {
            debug!(target: "primary::certifier", %header_digest, "header superseded before its proposal started; skipping proposal");
            return Ok(());
        }
        if let Ok(Some(cert)) =
            self.config.node_storage().get::<ProposedCertificates>(&header_digest)
        {
            info!(target: "primary::certifier", "asked to propose a header that is already certified {header_digest}, skipping proposal and re-publishing");
            // We have already processed this certificate, doing so again could produce signature
            // equivocation and destroy deterministic randomness (based on the leader
            // signature). Since we got here try to re-publish the certificate on gossip
            // network, but only once the record is durable.
            //
            // The record can still be memory-only here. The proposal task that formed it releases
            // the proposal lock right after its insert, before its own barrier acks, so this task
            // can read the row while its disk write is still queued. The proposer's `LastProposed`
            // barrier for a same-digest reproposal does not cover that write: `persist` queues its
            // barrier when called and the runner acks in FIFO order, so a barrier covers only the
            // writes queued before it, and the reproposal's barrier can be queued before the
            // insert. Gossiping a memory-only record lets a crash or a failed commit lose it, so a
            // restart misses this guard and certifies the header again, possibly over a different
            // 2f+1 subset and with a different aggregate signature (#963, #964). This task read
            // the row, so the row's insert was queued before the barrier below, and the barrier
            // covers it. This is the same fence #979 put on the vote fast-recast (issue #1530).
            //
            // Release the lock first. This path inserts nothing, so it needs no exclusion, and
            // holding the lock across a whole-DB barrier would stall every other proposal (see
            // the comment where the formed-certificate path drops the lock).
            //
            // If the barrier fails, the epoch DB has latched a failed commit (or its runner is
            // gone), so the record can be lost on restart. Refuse to republish and fail-stop the
            // node. This task is not critical, so returning the error alone would not stop it.
            drop(_guard);
            self.config
                .node_storage()
                .persist::<ProposedCertificates>()
                .await
                .inspect_err(|e| {
                    error!(target: "primary::certifier", "durable barrier failed for already-certified certificate, refusing to re-publish; initiating node shutdown: {e}");
                    self.config.shutdown().notify();
                })
                .map_err(|e| TaskError::from_message(e.to_string()))?;
            if let Err(e) = self.network.publish_certificate(cert).await {
                error!(target: "primary::certifier", ?e, "failed to re-gossip certificate");
            }
            return Ok(());
        }
        tokio::select! {
            // listen for new_proposal notification to exit
            // NOTE: sub here is okay bc no loop
            _ = new_proposal_noticer => {
                debug!(target: "primary::certifier", "new proposal notification received");
                Ok(())
            },

            // receive enough votes for certification (or exit early)
            proposal_result = self.propose_header(header, reissue_failed) => {
                match proposal_result {
                    Ok(mut certificate) => {
                        // A failed insert leaves no guard record, so a later proposal of this header
                        // could certify it again with a different aggregate. Fail-stop the node:
                        // this task is spawned as a non-critical task, and the task manager
                        // discards a non-critical task's error, so returning it alone would not
                        // stop the node (issue #1530).
                        if let Err(e) = self.config.node_storage().insert::<ProposedCertificates>(&header_digest, &certificate) {
                            error!(target: "primary::certifier", "error accepting own certificate, unable to save the certificate; initiating node shutdown: {e}");
                            self.config.shutdown().notify();
                            return Err(TaskError::from_message(e.to_string()));
                        }

                        // Release the proposal lock now that the guard record is written.
                        //
                        // The lock exists to make the already-certified check at the top of this
                        // method atomic with the insert above, and that pair is now complete: the
                        // insert updates the in-memory layer synchronously, so any later proposal
                        // reaching that check already observes this certificate. Nothing below
                        // needs the exclusion.
                        //
                        // Holding it across the barrier would be actively harmful, because
                        // `persist` is a *whole-DB* barrier - the table parameter is unused
                        // and only selects which of the three DBs to target - so it defers while
                        // any write txn on the epoch DB is open and drains only at refcount zero,
                        // with no timeout. Under catch-up or epoch close that is tens to hundreds
                        // of milliseconds during which every other proposal would queue behind this
                        // lock. Do not widen this critical section back out.
                        drop(_guard);

                        // Wait for the `ProposedCertificates` record to be durable before
                        // externalizing. The epoch DB persists asynchronously, so the insert above
                        // returns before the record hits disk; `process_own_certificate` below already
                        // externalizes (it forwards the certificate on the parents bus and triggers
                        // fetching), and the gossip publish follows. A crash in that window loses the
                        // record, so on restart the guard at the top of this method misses and the
                        // certifier re-proposes the same header, re-collecting votes over an unordered
                        // channel, which can aggregate a different 2f+1 subset into a distinct aggregate
                        // signature and thereby perturb the leader-signature randomness. See #934, #963.
                        // If the barrier reports a failed commit (disk full, `EIO`, checksum), the
                        // record is not on disk, so the internal processing and gossip publish below
                        // would externalize a certificate whose guard record can be lost on restart -
                        // exactly the re-proposal and leader-signature perturbation this barrier
                        // exists to prevent. Refuse rather than externalize a non-durable certificate
                        // (issue #975), and fail-stop the node as the vote barriers do (#979):
                        // returning the error from this non-critical task alone would not stop it
                        // (issue #1530).
                        self.config
                            .node_storage()
                            .persist::<ProposedCertificates>()
                            .await
                            .map_err(|e| {
                                error!(target: "primary::certifier", "durable barrier failed for own certificate, refusing to externalize; initiating node shutdown: {e}");
                                self.config.shutdown().notify();
                                TaskError::from_message(e.to_string())
                            })?;

                        // pass to state_sync for internal processing
                        if let Err(e) = self.state_sync.process_own_certificate(&mut certificate).await {
                            error!(target: "primary::certifier", "error accepting own certificate: {e}");
                            return Err(e.into());
                        }

                        // try to publish the certificate on gossip network
                        if let Err(e) = self.network.publish_certificate(certificate).await {
                            error!(target: "primary::certifier", ?e, "failed to gossip certificate");
                        }
                        Ok(())
                    }

                    Err(e) => {
                        match e {
                            // ignore cancelled proposal errors - expected when new proposal arrives
                            DagError::Canceled => {
                                debug!(
                                    target: "primary::certifier",
                                    auth=?self.authority_id,
                                    "certifier cancelled proposed header task"
                                );
                                Ok(())
                            }
                            // log other errors loudly
                            e =>  {
                                error!(
                                    target: "primary::certifier",
                                    auth=?self.authority_id,
                                    "Certifier error on proposed header task: {e}"
                                );
                                Err(e.into())
                            }
                        }
                    }
                }
            }
        }
    }

    /// Execute the main certification task.  Will run until shutdown is signalled.
    /// If this exits outside of shutdown it will log an error and this will trigger a node
    /// shutdown.
    async fn run(self, mut rx_headers: impl TnReceiver<Header>) -> TaskResult {
        info!(target: "primary::certifier", "Certifier on node {} has started successfully.", &self.authority_id);
        let shutdown = &self.config.shutdown().subscribe();
        loop {
            tokio::select! {
                // receive headers from proposer
                Some(header) = rx_headers.recv() => {
                    debug!(target: "primary::certifier", ?header, "{:?} received header!", &self.authority_id);

                    // the proposer re-sends its last header unchanged while it waits for parents.
                    // restarting that proposal would cancel its vote requests and discard the votes
                    // already received, so a header whose votes take longer than the re-send
                    // interval to reach quorum could never be certified. the vote requests in
                    // flight retry on their own until answered; only the requests that already
                    // ended in an error are issued again
                    let digest = header.digest();
                    let in_flight = match InFlightProposal::start(&self.in_flight, digest) {
                        Ok(in_flight) => in_flight,
                        Err(reissue_failed) => {
                            debug!(target: "primary::certifier", %digest, "identical re-proposal while certification is in flight; keeping vote collection and re-issuing failed vote requests");
                            // stores a permit if the proposal is not waiting yet, so the wake is
                            // not lost
                            reissue_failed.notify_one();
                            continue;
                        }
                    };

                    // cancel any outstanding proposals and vote requests
                    self.new_proposal.notify();

                    // subscribe here, not in the task: `Notifier` does not latch, so the next
                    // header's notify can land before this task is first polled
                    let cancel = self.new_proposal.subscribe();

                    // spawn certifier task so new proposals can cancel
                    let certifier = self.clone();
                    self.task_spawner.spawn_task(
                        format!("propose-header-{digest:?}"),
                        async move {
                            // held until the task ends, however it ends
                            let in_flight = in_flight;
                            certifier
                                .spawn_header_proposal(header, cancel, &in_flight.reissue_failed)
                                .await
                        },
                    );
                },

                // listen for consensus shutdown
                _ = shutdown => {
                    debug!(target: "primary::certifier", "Certifier received shutdown signal");
                    // cancel any outstanding proposals and vote requests
                    // NOTE: this isn't strictly necessary but may help shutdown
                    self.new_proposal.notify();
                    break Ok(());
                }
            }
        }
    }
}

/// Marks a header as the certifier's proposal in flight for as long as this guard lives.
///
/// [`Certifier::run`] moves one into each proposal task it spawns, so the mark clears however the
/// task ends: a certificate, a cancellation, an error, the already-certified republish, or the task
/// being dropped before it runs. A later copy of the same header then starts a new proposal.
struct InFlightProposal {
    /// The certifier's record of the header in flight and its re-issue signal.
    marker: InFlightMarker,
    /// The digest this guard marked.
    digest: HeaderDigest,
    /// Asks this proposal to re-issue the vote requests that ended in an error.
    ///
    /// Each proposal gets its own, so a wake meant for one header never reaches the next.
    reissue_failed: Arc<Notify>,
}

impl InFlightProposal {
    /// Mark `digest` as in flight with a new re-issue signal, or, if it already is, return the
    /// re-issue signal of the proposal in flight.
    fn start(marker: &InFlightMarker, digest: HeaderDigest) -> Result<Self, Arc<Notify>> {
        let mut current = marker.lock();
        if let Some((_, reissue_failed)) =
            current.as_ref().filter(|(in_flight, _)| *in_flight == digest)
        {
            return Err(reissue_failed.clone());
        }
        let reissue_failed = Arc::new(Notify::new());
        *current = Some((digest, reissue_failed.clone()));
        Ok(Self { marker: marker.clone(), digest, reissue_failed })
    }
}

impl Drop for InFlightProposal {
    fn drop(&mut self) {
        let mut current = self.marker.lock();
        // a different header may have replaced this one; its mark is not ours to clear
        if current.as_ref().is_some_and(|(in_flight, _)| *in_flight == self.digest) {
            *current = None;
        }
    }
}

/// The certifier's record of its proposal in flight: the header's digest and the signal that asks
/// that proposal to re-issue the vote requests that ended in an error.
type InFlightMarker = Arc<parking_lot::Mutex<Option<(HeaderDigest, Arc<Notify>)>>>;
