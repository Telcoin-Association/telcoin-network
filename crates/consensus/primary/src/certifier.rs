//! Certifier broadcasts headers and certificates for this primary.

use crate::{
    aggregators::VotesAggregator,
    network::{PrimaryNetworkHandle, RequestVoteResult},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use consensus_metrics::monitored_future;
use futures::{
    stream::{FuturesOrdered, FuturesUnordered},
    StreamExt,
};
use std::{cmp::min, sync::Arc, time::Duration};
use tn_config::{ConsensusConfig, KeyConfig};
use tn_network_libp2p::{error::NetworkError, types::NetworkResult};
use tn_primary_metrics::PrimaryMetrics;
use tn_storage::CertificateStore;
use tn_types::{
    ensure,
    error::{DagError, DagResult},
    AuthorityIdentifier, Certificate, CertificateDigest, Committee, Database, Header, Noticer,
    TaskManager, TnReceiver, TnSender, Vote, CHANNEL_CAPACITY,
};
use tokio::sync::broadcast;
use tracing::{debug, enabled, error, info, instrument, trace, warn};

#[cfg(test)]
#[path = "tests/certifier_tests.rs"]
pub mod certifier_tests;

/// This component is responisble for proposing headers to peers, collecting votes on headers,
/// and certifying headers into certificates.
///
/// It receives headers to propose from Proposer via `rx_headers`, and sends out certificates to be
/// broadcasted by calling `Synchronizer::accept_own_certificate()`.
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
    /// Send own certificates to be broadcasted to all other peers.
    tx_own_certificate_broadcast: broadcast::Sender<Certificate>,
}

impl<DB: Database> Certifier<DB> {
    fn highest_created_certificate(config: &ConsensusConfig<DB>) -> Option<Certificate> {
        if let Some(id) = config.authority_id() {
            debug!(target: "epoch-manager", ?id, "reading last round for authority id");
            config.node_storage().last_round(&id).expect("certificate store available")
        } else {
            debug!(target: "epoch-manager", "node is not an authority - returning `None` for highest created certificate");
            None
        }
    }

    pub fn spawn(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        state_sync: StateSynchronizer<DB>,
        primary_network: PrimaryNetworkHandle,
        task_manager: &TaskManager,
    ) {
        let Some(authority_id) = config.authority_id() else {
            // If we don't have an authority id then we are not a validator and should not be
            // proposing anything...
            return;
        };
        let rx_shutdown = config.shutdown().subscribe();
        let primary_metrics = consensus_bus.primary_metrics().node_metrics.clone();
        // These channels are used internally to this module (file) and don't need to go in the
        // consensus bus. If this changes they can move.  Note there can be issues receiving
        // certs over the broadcast if not subscribed early.
        let (tx_own_certificate_broadcast, _rx_own_certificate_broadcast) =
            broadcast::channel(CHANNEL_CAPACITY);

        // prevents race condition during startup when first proposed header fails during
        // tx_own_certificate_broadcast.send()
        let broadcast_targets: Vec<(_, _)> = config
            .committee()
            .others_primaries_by_id(Some(&authority_id))
            .into_iter()
            .map(|(name, _addr, _network_key)| (name, tx_own_certificate_broadcast.subscribe()))
            .collect();

        let highest_created_certificate = Self::highest_created_certificate(&config);
        debug!(
            target: "epoch-manager",
            ?highest_created_certificate,
            "restoring certifier with highest created certificate for epoch {}",
            config.epoch(),
        );

        for (name, rx_own_certificate_broadcast) in broadcast_targets.into_iter() {
            trace!(target: "primary::synchronizer::broadcast_certificates", ?name, "spawning sender for peer");
            task_manager.spawn_task(
                format!("broadcast certificates to {name}"),
                Self::push_certificates(
                    primary_network.clone(),
                    name,
                    rx_own_certificate_broadcast,
                ),
            );
        }

        if let Some(cert) = highest_created_certificate {
            // Error can be ignored.
            if let Err(e) = tx_own_certificate_broadcast.send(cert) {
                error!(target: "primary::certifier", ?e, "failed to broadcast highest created certificate during startup");
            }
        }

        task_manager.spawn_critical_task("certifier task", monitored_future!(
            async move {
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
                    tx_own_certificate_broadcast: tx_own_certificate_broadcast.clone(),
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
    #[instrument(level = "debug", skip_all, fields(header_digest = ?header.digest()))]
    async fn request_vote(
        &self,
        authority: AuthorityIdentifier,
        header: Header,
    ) -> DagResult<Vote> {
        debug!(target: "primary::certifier", ?authority, ?header, "requesting vote for header...");
        let peer_id = authority.peer_id();

        let mut missing_parents: Vec<CertificateDigest> = Vec::new();
        let mut attempt: u32 = 0;
        let vote: Vote = loop {
            attempt += 1;

            let parents = if missing_parents.is_empty() {
                Vec::new()
            } else {
                let expected_count = missing_parents.len();
                let parents: Vec<_> = self
                    .certificate_store
                    .read_all(
                        missing_parents
                            .into_iter()
                            // Only provide certs that are parents for the requested vote.
                            .filter(|parent| header.parents().contains(parent)),
                    )?
                    .into_iter()
                    .flatten()
                    .collect();
                if parents.len() != expected_count {
                    error!("tried to read {expected_count} missing certificates requested by remote primary for vote request, but only found {}", parents.len());
                    return Err(DagError::ProposedHeaderMissingCertificates);
                }
                parents
            };

            match self.network.request_vote(peer_id, header.clone(), parents).await {
                Ok(RequestVoteResult::Vote(vote)) => {
                    debug!(target: "primary::certifier", ?authority, ?vote, "Ok response received after request vote");
                    break vote;
                }
                Ok(RequestVoteResult::MissingParents(parents)) => {
                    debug!(target: "primary::certifier", ?authority, ?parents, "Ok missing parents response received after request vote");
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

        // Verify the vote. Note that only the header digest is signed by the vote.
        ensure!(
            vote.header_digest() == header.digest()
                && vote.origin() == header.author()
                && vote.author() == &authority,
            DagError::UnexpectedVote(vote.header_digest())
        );
        // Possible equivocations.
        ensure!(
            header.epoch() == vote.epoch(),
            DagError::InvalidEpoch { expected: header.epoch(), received: vote.epoch() }
        );
        ensure!(
            header.round() == vote.round(),
            DagError::InvalidRound { expected: header.round(), received: vote.round() }
        );

        // Ensure the header is from the correct epoch.
        ensure!(
            vote.epoch() == self.committee.epoch(),
            DagError::InvalidEpoch { expected: self.committee.epoch(), received: vote.epoch() }
        );

        // Ensure the authority has voting rights.
        ensure!(
            self.committee.voting_power_by_id(vote.author()) > 0,
            DagError::UnknownAuthority(vote.author().to_string())
        );

        Ok(vote)
    }

    #[instrument(level = "debug", skip_all, fields(header_digest = ?header.digest()))]
    async fn propose_header<RXH: TnReceiver<Header>>(
        &self,
        header: Header,
        rx_headers: &mut RXH,
    ) -> DagResult<Certificate> {
        let authority_id = &self.authority_id;
        debug!(target: "primary::certifier", ?authority_id, "proposing header");
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

        self.metrics.proposed_header_round.set(header.round() as i64);

        // Reset the votes aggregator and sign our own header.
        let mut votes_aggregator = VotesAggregator::new(self.metrics.clone());
        let vote = Vote::new(&header, self.authority_id.clone(), &self.signature_service).await;
        let mut certificate = votes_aggregator.append(vote, &self.committee, &header)?;

        // Trigger vote requests.
        let peers = self
            .committee
            .others_primaries_by_id(Some(&self.authority_id))
            .into_iter()
            .map(|(name, _, network_key)| (name, network_key));
        let mut requests: FuturesUnordered<_> = peers
            .map(|(name, _target)| {
                let header = header.clone();
                self.request_vote(name, header)
            })
            .collect();
        loop {
            if certificate.is_some() {
                break;
            }
            let mut next_request = requests.next();
            tokio::select! {
                result = &mut next_request => {
                    debug!(target: "primary::certifier", ?authority_id, ?result, "next request in unordered futures");

                    match result {
                        Some(Ok(vote)) => {
                            certificate = votes_aggregator.append(
                                vote,
                                &self.committee,
                                &header,
                            )?;
                        },
                        Some(Err(e)) => error!(target: "primary::certifier", ?authority_id, "failed to get vote for header {header:?}: {e:?}"),
                        None => {
                            break;
                        }
                    }
                },
                _ = rx_headers.recv() => {
                    warn!(target: "primary::certifier", ?authority_id, "canceling Header proposal {header} for round {}", header.round());
                    // This allows us to inturupt the propose_header future- just put it back on the headers channel to get picked up in outer select.
                    let _ = self.consensus_bus.headers().send(header).await;
                    return Err(DagError::Canceled)
                },
            }
        }

        let certificate = certificate.ok_or_else(|| {
            // Log detailed header info if we failed to form a certificate.
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
                error!(target: "primary::certifier", ?authority_id, msg, "inside propose_header");
            }
            DagError::CouldNotFormCertificate(header.digest())
        })?;
        debug!(target: "primary::certifier", ?authority_id, "Assembled {certificate:?}");

        Ok(certificate)
    }

    /// Pushes new certificates received from the rx_own_certificate_broadcast channel
    /// to the target peer continuously. Only exits when the primary is shutting down.
    async fn push_certificates(
        network: PrimaryNetworkHandle,
        authority_id: AuthorityIdentifier,
        mut rx_own_certificate_broadcast: broadcast::Receiver<Certificate>,
    ) {
        // Older broadcasts return early, so the last broadcast must be the latest certificate.
        // This will contain at most certificates created within the last PUSH_TIMEOUT.
        let mut requests = FuturesOrdered::new();
        // Back off and retry only happen when there is only one certificate to be broadcasted.
        // Otherwise no retry happens.
        const BACKOFF_INTERVAL: Duration = Duration::from_millis(100);
        const MAX_BACKOFF_MULTIPLIER: u32 = 100;
        let mut backoff_multiplier: u32 = 0;

        async fn send_certificate(
            network: &PrimaryNetworkHandle,
            cert: Certificate,
        ) -> (Certificate, NetworkResult<()>) {
            let resp = network.publish_certificate(cert.clone()).await;
            (cert, resp)
        }

        loop {
            trace!(target: "primary::certifier", authority=?authority_id, "start loop for push certificate");
            tokio::select! {
                result = rx_own_certificate_broadcast.recv() => {
                    trace!(target: "primary::certifier", authority=?authority_id, "rx_own_certificate_broadcast received");
                    let cert = match result {
                        Ok(cert) => cert,
                        Err(broadcast::error::RecvError::Closed) => {
                            info!(target: "primary::certifier", "Certificate sender {authority_id} is shutting down!");
                            return;
                        }
                        Err(broadcast::error::RecvError::Lagged(e)) => {
                            warn!(target: "primary::certifier", "Certificate broadcaster {authority_id} lagging! {e}");
                            // Re-run the loop to receive again.
                            continue;
                        }
                    };
                    trace!(target: "primary::certifier", authority=?authority_id, ?cert, "successfully received own cert broadcast");
                    requests.push_back(send_certificate(&network, cert));
                }
                Some((cert, resp)) = requests.next() => {
                    trace!(target: "primary::certifier", authority=?authority_id, ?resp, ?cert, "next cert request");
                    backoff_multiplier = match resp {
                        Ok(_) => {
                            0
                        },
                        Err(_) => {
                            if requests.is_empty() {
                                // Retry broadcasting the latest certificate, to help the network stay alive.
                                requests.push_back(send_certificate(&network, cert));
                                min(backoff_multiplier * 2 + 1, MAX_BACKOFF_MULTIPLIER)
                            } else {
                                // TODO: add backoff and retries for transient & retriable errors.
                                0
                            }
                        },
                    };
                    if backoff_multiplier > 0 {
                        tokio::time::sleep(BACKOFF_INTERVAL * backoff_multiplier).await;
                    }
                }
            };
        }
    }

    /// Execute the main certification task.  Will run until shutdown is signalled.
    /// If this exits outside of shutdown it will log an error and this will trigger a node
    /// shutdown.
    async fn run(self) {
        info!(target: "primary::certifier", "Certifier on node {} has started successfully.", self.authority_id);
        let mut rx_headers = self.consensus_bus.headers().subscribe();
        loop {
            tokio::select! {
                Some(header) = rx_headers.recv() => {
                    debug!(target: "primary::certifier", authority=?self.authority_id, ?header, "header received!");

                    match self.propose_header(header, &mut rx_headers).await {
                        Ok(certificate) => {
                            let state_sync = self.state_sync.clone();
                            let tx_own_certificate_broadcast = self.tx_own_certificate_broadcast.clone();
                            // pass to state_sync for internal processing
                            if let Err(e) = state_sync.process_own_certificate(certificate.clone()).await {
                                error!(target: "primary::certifier", "error accepting own certificate: {e}");
                                return;
                            }

                            // Broadcast the certificate once the synchronizer is ok
                            if let Err(e) = tx_own_certificate_broadcast.send(certificate.clone()) {
                                error!(target: "primary::certifier", ?certificate, ?e, "failed to broadcast certificate to self!");
                                return;
                            }
                        }
                        Err(e) => {
                            match e {
                                // ignore errors when the propsal is cancelled - this is expected
                                DagError::Canceled => debug!(target: "primary::certifier", authority=?self.authority_id, "Certifier error on proposed header task: {e}"),
                                // log other errors
                                e =>  error!(target: "primary::certifier", authority=?self.authority_id, "Certifier error on proposed header task: {e}"),
                            }
                        }
                    }
                },

                _ = &self.rx_shutdown => {
                    debug!(target: "primary::certifier", "Certifier received shutdown signal");
                    break;
                }
            }
        }
    }
}
