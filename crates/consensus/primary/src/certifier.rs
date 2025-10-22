//! Certifier broadcasts headers and certificates for this primary.

use crate::{
    aggregators::VotesAggregator,
    network::{PrimaryNetworkHandle, RequestVoteResult},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use consensus_metrics::monitored_future;
use std::{sync::Arc, time::Duration};
use tn_config::{ConsensusConfig, KeyConfig};
use tn_network_libp2p::error::NetworkError;
use tn_primary_metrics::PrimaryMetrics;
use tn_storage::CertificateStore;
use tn_types::{
    ensure,
    error::{DagError, DagResult},
    AuthorityIdentifier, BlsPublicKey, Certificate, CertificateDigest, Committee, Database, Header,
    Noticer, Notifier, TaskManager, TaskSpawner, TnReceiver, TnSender, Vote,
};
use tracing::{debug, enabled, error, info};

#[cfg(test)]
#[path = "tests/certifier_tests.rs"]
pub mod certifier_tests;

/// This component is responisble for proposing headers to peers, collecting votes on headers,
/// and certifying headers into certificates.
///
/// It receives headers to propose from Proposer via `rx_headers`, and publishes certificates to
/// gossip network.
pub struct Certifier<DB> {
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
    /// Receiver for shutdown.
    rx_shutdown: Noticer,
    /// Consensus channels.
    consensus_bus: ConsensusBus,
    /// A network sender to send the batches to the other workers.
    network: PrimaryNetworkHandle,
    /// Metrics handler
    metrics: Arc<PrimaryMetrics>,
    /// Spawn epoch-related tasks.
    task_spawner: TaskSpawner,
    /// Notifier to cancel pending vote requests if proposer sends new header.
    new_proposal: Notifier,
}

impl<DB: Database> Certifier<DB> {
    /// Spawn the long-running certifier task.
    pub fn spawn(
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

        let rx_shutdown = config.shutdown().subscribe();
        let primary_metrics = consensus_bus.primary_metrics().node_metrics.clone();

        // spawn long-running task to gossip own certificates
        let task_spawner = task_manager.get_spawner();
        task_manager.spawn_critical_task("certifier task", monitored_future!(
            async move {
                let highest_created_certificate = config.node_storage().last_round(&authority_id).expect("certificate store available");
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

                Self {
                    authority_id: authority_id.clone(),
                    committee: config.committee().clone(),
                    certificate_store: config.node_storage().clone(),
                    state_sync,
                    signature_service: config.key_config().clone(),
                    rx_shutdown,
                    consensus_bus,
                    network: primary_network,
                    metrics: primary_metrics,
                    task_spawner,
                    new_proposal: Notifier::new(),
                }
                .run()
                .await;
                info!(target: "primary::certifier", "Certifier on node {} has shutdown.", authority_id);
            },
            "CertifierTask"
        ));
    }

    /// Requests a vote for a Header from the given peer. Retries indefinitely until either a
    /// vote is received, or a permanent error is returned.
    async fn request_vote(
        authority: AuthorityIdentifier,
        header: Header,
        peer_id: BlsPublicKey,
        certificate_store: DB,
        network: PrimaryNetworkHandle,
        committee: Committee,
        cancel_proposal: Noticer,
    ) -> DagResult<Vote> {
        let mut missing_parents: Vec<CertificateDigest> = Vec::new();
        let mut attempt: u32 = 0;
        debug!(target: "primary::certifier", ?authority, ?header, "requesting vote for header...");

        // loop until vote received
        let vote: Vote = loop {
            // increase attempt count
            attempt += 1;

            // peers may respond to a vote requesting missing parents
            let parents = if missing_parents.is_empty() {
                Vec::new()
            } else {
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

                parents
            };

            tokio::select! {
                vote_result = network.request_vote(peer_id, header.clone(), parents) => {
                    // match peer's response for vote request
                    match vote_result {
                        Ok(RequestVoteResult::Vote(vote)) => {
                            debug!(target: "primary::certifier", ?authority, ?vote, "Ok response received after request vote");
                            // happy path - vote recieved
                            break vote;
                        }
                        Ok(RequestVoteResult::MissingParents(parents)) => {
                            debug!(target: "primary::certifier", ?authority, ?parents, "Ok missing parents response received after request vote");
                            // retrieve missing parents so peer can vote
                            missing_parents = parents;
                        }
                        Err(error) => {
                            if let NetworkError::RPCError(error) = error {
                                error!(target: "primary::certifier", ?authority, ?error, ?header, "fatal request for requested vote");
                                return Err(DagError::NetworkError(format!(
                                    "irrecoverable error requesting vote for {header}: {error}"
                                )));
                            } else {
                                error!(target: "primary::certifier", ?authority, ?error, ?header, "network error requesting vote");
                            }

                            missing_parents = Vec::new();
                        }
                    }
                }

                // cancel proposal
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
    async fn propose_header(
        header: Header,
        authority_id: AuthorityIdentifier,
        committee: Committee,
        metrics: Arc<PrimaryMetrics>,
        signature_service: KeyConfig,
        network: PrimaryNetworkHandle,
        certificate_store: DB,
        task_spawner: TaskSpawner,
        new_proposal: Notifier,
    ) -> DagResult<Certificate> {
        debug!(target: "primary::certifier", ?authority_id, "proposing header");

        // only propose headers in current epoch
        if header.epoch() != committee.epoch() {
            error!(
                target: "primary::certifier",
                "Certifier received mismatched header proposal for epoch {}, currently at epoch {}",
                header.epoch(),
                committee.epoch()
            );
            return Err(DagError::InvalidEpoch {
                expected: committee.epoch(),
                received: header.epoch(),
            });
        }

        metrics.proposed_header_round.set(header.round() as i64);

        // reset the votes aggregator and sign own header
        let mut votes_aggregator = VotesAggregator::new(metrics.clone());
        let vote = Vote::new(&header, authority_id.clone(), &signature_service);
        let mut certificate = votes_aggregator.append(vote, &committee, &header)?;

        // create a channel for receiving votes from peers
        let (tx_votes, mut rx_votes) = tokio::sync::mpsc::unbounded_channel();

        // create network requests for votes from peers
        let peers = committee.others_primaries_by_id(Some(&authority_id)).into_iter();
        for (name, target) in peers {
            let header_clone = header.clone();
            let tx_votes = tx_votes.clone();
            let network = network.clone();
            let certificate_store = certificate_store.clone();
            let committee = committee.clone();
            let cancel_proposal = new_proposal.subscribe();
            let task_name = format!("vote-{header:?}-{name}");
            task_spawner.spawn_task(task_name, async move {
                // process request for vote
                tx_votes.send(
                    // this will exit early on cancel_proposal
                    Self::request_vote(
                        name,
                        header_clone,
                        target,
                        certificate_store,
                        network,
                        committee,
                        cancel_proposal,
                    )
                    .await,
                )
            });
        }

        // drop sender so channel closes when all vote tasks complete
        drop(tx_votes);

        // loop through requests until complete or cancelled
        loop {
            // certificate created - no more votes needed
            if certificate.is_some() {
                break;
            }

            // receive votes or exit early if new proposal replaces this header before certification
            tokio::select! {
                result = rx_votes.recv() => {
                    debug!(target: "primary::certifier", ?authority_id, ?result, "next request in unordered futures");

                    match result {
                        // happy path
                        Some(Ok(vote)) => {
                            let authority_id = vote.author.clone();
                            // Make sure not to let an error here stop certification (else a crafted vote could break consensus).
                            certificate = match votes_aggregator.append(
                                vote,
                                &committee,
                                &header,
                            ) {
                                Ok(cert) => cert,
                                Err(e) => {
                                    error!(target: "primary::certifier", ?authority_id, "received an invalid vote: {e:?}");
                                    None
                                }
                            }
                        },

                        // handle vote error
                        Some(Err(e)) => {
                            error!(target: "primary::certifier", ?authority_id, "failed to get vote for header {header:?}: {e:?}");
                        }

                        // all sending channels have dropped
                        None => {
                            break;
                        }
                    }
                },

                // exit early when new proposal arrives
                _ = new_proposal.subscribe() => {
                    debug!(target: "primary::certifier", "new proposal received - aborting proposal...");
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
                    let parent_msg = match certificate_store.read(*parent_digest) {
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
                error!(target: "primary::certifier", ?authority_id, msg, "inside propose_header");
            }
            DagError::CouldNotFormCertificate(header.digest())
        })?;

        debug!(target: "primary::certifier", ?authority_id, "Assembled {certificate:?}");

        Ok(certificate)
    }

    /// Execute the main certification task.  Will run until shutdown is signalled.
    /// If this exits outside of shutdown it will log an error and this will trigger a node
    /// shutdown.
    async fn run(self) {
        info!(target: "primary::certifier", "Certifier on node {} has started successfully.", &self.authority_id);
        let mut rx_headers = self.consensus_bus.headers().subscribe();
        loop {
            tokio::select! {
                // receive headers from proposer
                Some(header) = rx_headers.recv() => {
                    debug!(target: "primary::certifier", ?header, "{:?} received header!", &self.authority_id);

                    // cancel any outstanding proposals and vote requests
                    self.new_proposal.notify();

                    // spawn task to propose header or be cancelled
                    let authority_id = self.authority_id.clone();
                    let committee = self.committee.clone();
                    let certificate_store = self.certificate_store.clone();
                    let state_sync = self.state_sync.clone();
                    let signature_service = self.signature_service.clone();
                    let metrics = self.metrics.clone();
                    let network = self.network.clone();
                    let task_spawner = self.task_spawner.clone();
                    let cancel_proposal = self.new_proposal.subscribe();
                    let new_proposal_notifier = self.new_proposal.clone();

                    // spawn proposer task that is cancelled when a new header is received
                    self.task_spawner.spawn_task(
                        format!("propose-header-{:?}", header),
                        async move {
                            tokio::select!{
                                // listen for new_proposal notification to exit
                                _ = cancel_proposal => {},

                                // receive enough votes for certification
                                proposal_result = Self::propose_header(
                                    header,
                                    authority_id.clone(),
                                    committee,
                                    metrics,
                                    signature_service,
                                    network.clone(),
                                    certificate_store,
                                    task_spawner,
                                    new_proposal_notifier,
                                ) => {
                                    match proposal_result {
                                        Ok(certificate) => {
                                            // pass to state_sync for internal processing
                                            if let Err(e) = state_sync.process_own_certificate(certificate.clone()).await {
                                                error!(target: "primary::certifier", "error accepting own certificate: {e}");
                                                return;
                                            }

                                            // try to publish the certificate on gossip network
                                            if let Err(e) = network.publish_certificate(certificate).await {
                                                error!(target: "primary::certifier", ?e, "failed to gossip certificate");
                                            }
                                        }

                                        Err(e) => {
                                            match e {
                                                // ignore errors when the proposal is cancelled
                                                DagError::Canceled => debug!(target: "primary::certifier", authority=?authority_id, "certifier cancelled proposed header task"),
                                                // log other errors
                                                e =>  error!(target: "primary::certifier", authority=?authority_id, "Certifier error on proposed header task: {e}"),
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    );
                },

                // epoch shutdown
                _ = &self.rx_shutdown => {
                    debug!(target: "primary::certifier", "Certifier received shutdown signal");
                    break;
                }
            }
        }
    }
}
