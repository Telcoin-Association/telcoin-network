//! Certifier broadcasts headers and certificates for this primary.

// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{aggregators::VotesAggregator, synchronizer::Synchronizer, ConsensusBus};
use anemo::{rpc::Status, Request, Response};
use consensus_metrics::{monitored_future, spawn_logged_monitored_task};
use consensus_network::anemo_ext::{NetworkExt, WaitingPeer};
use consensus_network_types::{
    PrimaryToPrimaryClient, RequestVoteRequest, SendCertificateRequest, SendCertificateResponse,
};
use futures::{
    stream::{FuturesOrdered, FuturesUnordered},
    StreamExt,
};
use std::{cmp::min, future::Future, pin::pin, sync::Arc, task::Poll, time::Duration};
use tn_config::{ConsensusConfig, KeyConfig};
use tn_primary_metrics::PrimaryMetrics;
use tn_storage::{traits::Database, CertificateStore};
use tn_types::{
    ensure,
    error::{DagError, DagResult},
    AuthorityIdentifier, BlsSigner, Certificate, CertificateDigest, Committee, Header,
    NetworkPublicKey, Noticer, TnReceiver, TnSender, Vote, CHANNEL_CAPACITY,
};
use tokio::{
    sync::{broadcast, oneshot},
    task::{JoinHandle, JoinSet},
};
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
    certificate_store: CertificateStore<DB>,
    /// Handles synchronization with other nodes and our workers.
    synchronizer: Arc<Synchronizer<DB>>,
    /// Service to sign headers.
    signature_service: KeyConfig,
    /// Receiver for shutdown.
    rx_shutdown: Noticer,
    /// Consensus channels.
    consensus_bus: ConsensusBus,
    /// Used to cancel vote requests for a previously-proposed header that is being replaced
    /// before a certificate could be formed.
    cancel_proposed_header: Option<oneshot::Sender<()>>,
    /// Handle to propose_header task. Our target is to have only one task running always, thus
    /// we cancel the previously running before we spawn the next one. However, we don't wait for
    /// the previous to finish to spawn the new one, so we might temporarily have more that one
    /// parallel running, which should be fine though.
    propose_header_tasks: JoinSet<DagResult<Certificate>>,
    /// A network sender to send the batches to the other workers.
    network: anemo::Network,
    /// Metrics handler
    metrics: Arc<PrimaryMetrics>,
    /// Send own certificates to be broadcasted to all other peers.
    tx_own_certificate_broadcast: broadcast::Sender<Certificate>,
    /// Background tasks broadcasting newly formed certificates.
    ///
    /// The set must be "owned" as it runs in the background.
    _certificate_senders: JoinSet<()>,
}

impl<DB: Database> Certifier<DB> {
    #[must_use]
    pub fn spawn(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        synchronizer: Arc<Synchronizer<DB>>,
        primary_network: anemo::Network,
    ) -> JoinHandle<()> {
        let rx_shutdown = config.subscribe_shutdown();
        let metrics = consensus_bus.primary_metrics().node_metrics.clone();
        // These channels are used internally to this module (file) and don't need to go in the
        // consensus bus. If this changes they can move.  Note there can be issues receiving
        // certs over the broadcast if not subscribed early.
        let (tx_own_certificate_broadcast, _rx_own_certificate_broadcast) =
            broadcast::channel(CHANNEL_CAPACITY);

        // prevents race condition during startup when first proposed header fails during
        // tx_own_certificate_broadcast.send()
        let broadcast_targets: Vec<(_, _, _)> = config
            .committee()
            .others_primaries_by_id(config.authority().id())
            .into_iter()
            .map(|(name, _addr, network_key)| {
                (name, tx_own_certificate_broadcast.subscribe(), network_key)
            })
            .collect();

        let highest_created_certificate = config
            .node_storage()
            .certificate_store
            .last_round(config.authority().id())
            .expect("certificate store available");

        let mut _certificate_senders = JoinSet::new();
        for (name, rx_own_certificate_broadcast, network_key) in broadcast_targets.into_iter() {
            trace!(target:"primary::synchronizer::broadcast_certificates", ?name, "spawning sender for peer");
            _certificate_senders.spawn(Self::push_certificates(
                primary_network.clone(),
                name,
                network_key,
                rx_own_certificate_broadcast,
            ));
        }
        if let Some(cert) = highest_created_certificate {
            // Error can be ignored.
            if let Err(e) = tx_own_certificate_broadcast.send(cert) {
                error!(target: "primary::certifier", ?e, "failed to broadcast highest created certificate during startup");
            }
        }

        spawn_logged_monitored_task!(
            async move {
                info!(target: "primary::certifier", "Certifier on node {} has started successfully.", config.authority().id());
                Self {
                    authority_id: config.authority().id(),
                    committee: config.committee().clone(),
                    certificate_store: config.node_storage().certificate_store.clone(),
                    synchronizer,
                    signature_service: config.key_config().clone(),
                    rx_shutdown,
                    consensus_bus,
                    cancel_proposed_header: None,
                    propose_header_tasks: JoinSet::new(),
                    network: primary_network,
                    metrics,
                    tx_own_certificate_broadcast: tx_own_certificate_broadcast.clone(),
                    _certificate_senders,
                }
                .await;
                info!(target: "primary::certifier", "Certifier on node {} has shutdown.", config.authority().id());
            },
            "CertifierTask"
        )
    }

    /// Requests a vote for a Header from the given peer. Retries indefinitely until either a
    /// vote is received, or a permanent error is returned.
    #[instrument(level = "debug", skip_all, fields(header_digest = ?header.digest()))]
    async fn request_vote(
        network: anemo::Network,
        committee: Committee,
        certificate_store: CertificateStore<DB>,
        authority: AuthorityIdentifier,
        target: NetworkPublicKey,
        header: Header,
    ) -> DagResult<Vote> {
        debug!(target: "primary::certifier", ?authority, ?header, "requesting vote for header...");
        let peer_id = anemo::PeerId(target.0.to_bytes());
        let peer = network.waiting_peer(peer_id);

        let mut client = PrimaryToPrimaryClient::new(peer);

        let mut missing_parents: Vec<CertificateDigest> = Vec::new();
        let mut attempt: u32 = 0;
        let vote: Vote = loop {
            attempt += 1;

            let parents = if missing_parents.is_empty() {
                Vec::new()
            } else {
                let expected_count = missing_parents.len();
                let parents: Vec<_> = certificate_store
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

            let request =
                anemo::Request::new(RequestVoteRequest { header: header.clone(), parents })
                    .with_timeout(Duration::from_secs(30));
            match client.request_vote(request).await {
                Ok(response) => {
                    let response = response.into_body();
                    debug!(target: "primary::certifier", ?authority, ?response, "Ok response received after request vote");
                    if response.vote.is_some() {
                        break response.vote.unwrap();
                    }
                    missing_parents = response.missing;
                }
                Err(status) => {
                    // TODO: why does this error out so much?
                    error!(target: "primary::certifier", ?authority, ?status, ?header, "bad request for requested vote");
                    if status.status() == anemo::types::response::StatusCode::BadRequest {
                        error!(target: "primary::certifier", ?authority, ?status, ?header, "fatal request for requested vote");
                        return Err(DagError::NetworkError(format!(
                            "irrecoverable error requesting vote for {header}: {status:?}"
                        )));
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
                && vote.author() == authority,
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
            vote.epoch() == committee.epoch(),
            DagError::InvalidEpoch { expected: committee.epoch(), received: vote.epoch() }
        );

        // Ensure the authority has voting rights.
        ensure!(
            committee.stake_by_id(vote.author()) > 0,
            DagError::UnknownAuthority(vote.author().to_string())
        );

        Ok(vote)
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(level = "debug", skip_all, fields(header_digest = ?header.digest()))]
    async fn propose_header<BLS: BlsSigner>(
        authority_id: AuthorityIdentifier,
        committee: Committee,
        certificate_store: CertificateStore<DB>,
        signature_service: BLS,
        metrics: Arc<PrimaryMetrics>,
        network: anemo::Network,
        header: Header,
        mut cancel: oneshot::Receiver<()>,
    ) -> DagResult<Certificate> {
        debug!(target: "primary::certifier", ?authority_id, "proposing header");
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

        // Reset the votes aggregator and sign our own header.
        let mut votes_aggregator = VotesAggregator::new(metrics.clone());
        let vote = Vote::new(&header, &authority_id, &signature_service).await;
        let mut certificate = votes_aggregator.append(vote, &committee, &header)?;

        // Trigger vote requests.
        let peers = committee
            .others_primaries_by_id(authority_id)
            .into_iter()
            .map(|(name, _, network_key)| (name, network_key));
        let mut requests: FuturesUnordered<_> = peers
            .map(|(name, target)| {
                let header = header.clone();
                Self::request_vote(
                    network.clone(),
                    committee.clone(),
                    certificate_store.clone(),
                    name,
                    target,
                    header,
                )
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
                                &committee,
                                &header,
                            )?;
                        },
                        Some(Err(e)) => error!(target: "primary::certifier", ?authority_id, "failed to get vote for header {header:?}: {e:?}"),
                        None => {
                            break;
                        }
                    }
                },
                _ = &mut cancel => {
                    warn!(target: "primary::certifier", ?authority_id, "canceling Header proposal {header} for round {}", header.round());
                    return Err(DagError::Canceled)
                },
            }
        }

        let certificate = certificate.ok_or_else(|| {
            // Log detailed header info if we failed to form a certificate.
            if enabled!(tracing::Level::WARN) {
                let mut msg = format!(
                    "Failed to form certificate from header {header:?} with parent certificates:\n"
                );
                for parent_digest in header.parents().iter() {
                    let parent_msg = match certificate_store.read(*parent_digest) {
                        Ok(Some(cert)) => format!("{cert:?}\n"),
                        Ok(None) => {
                            format!("!!!missing certificate for digest {parent_digest:?}!!!\n")
                        }
                        Err(e) => format!(
                            "!!!error retrieving certificate for digest {parent_digest:?}: {e:?}\n"
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
        network: anemo::Network,
        authority_id: AuthorityIdentifier,
        network_key: NetworkPublicKey,
        mut rx_own_certificate_broadcast: broadcast::Receiver<Certificate>,
    ) {
        const PUSH_TIMEOUT: Duration = Duration::from_secs(10);
        let peer_id = anemo::PeerId(network_key.0.to_bytes());
        let peer = network.waiting_peer(peer_id);
        let client = PrimaryToPrimaryClient::new(peer);
        // Older broadcasts return early, so the last broadcast must be the latest certificate.
        // This will contain at most certificates created within the last PUSH_TIMEOUT.
        let mut requests = FuturesOrdered::new();
        // Back off and retry only happen when there is only one certificate to be broadcasted.
        // Otherwise no retry happens.
        const BACKOFF_INTERVAL: Duration = Duration::from_millis(100);
        const MAX_BACKOFF_MULTIPLIER: u32 = 100;
        let mut backoff_multiplier: u32 = 0;

        async fn send_certificate(
            mut client: PrimaryToPrimaryClient<WaitingPeer>,
            request: Request<SendCertificateRequest>,
            cert: Certificate,
        ) -> (Certificate, Result<Response<SendCertificateResponse>, Status>) {
            let resp = client.send_certificate(request).await;
            (cert, resp)
        }

        loop {
            trace!(target: "primary::synchronizer", authority=?authority_id, "start loop for push certificate");
            tokio::select! {
                result = rx_own_certificate_broadcast.recv() => {
                    trace!(target: "primary::synchronizer", authority=?authority_id, "rx_own_certificate_broadcast received");
                    let cert = match result {
                        Ok(cert) => cert,
                        Err(broadcast::error::RecvError::Closed) => {
                            trace!(target: "primary::synchronizer", "Certificate sender {authority_id} is shutting down!");
                            return;
                        }
                        Err(broadcast::error::RecvError::Lagged(e)) => {
                            warn!(target: "primary::synchronizer", "Certificate broadcaster {authority_id} lagging! {e}");
                            // Re-run the loop to receive again.
                            continue;
                        }
                    };
                    trace!(target: "primary::synchronizer", authority=?authority_id, ?cert, "successfully received own cert broadcast");
                    let request = Request::new(SendCertificateRequest { certificate: cert.clone() }).with_timeout(PUSH_TIMEOUT);
                    requests.push_back(send_certificate(client.clone(),request, cert));
                }
                Some((cert, resp)) = requests.next() => {
                    trace!(target: "primary::synchronizer", authority=?authority_id, ?resp, ?cert, "next cert request");
                    backoff_multiplier = match resp {
                        Ok(_) => {
                            0
                        },
                        Err(_) => {
                            if requests.is_empty() {
                                // Retry broadcasting the latest certificate, to help the network stay alive.
                                let request = Request::new(SendCertificateRequest { certificate: cert.clone() }).with_timeout(PUSH_TIMEOUT);
                                requests.push_back(send_certificate(client.clone(), request, cert));
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
}

impl<DB: Database> Future for Certifier<DB> {
    // Errors are either loggable events or show stoppers so we don't return an error type.
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();

        // Are we done?
        if pin!(&this.rx_shutdown).poll(cx).is_ready() {
            return Poll::Ready(());
        }

        {
            let mut rx_headers = this.consensus_bus.headers().borrow_subscriber();
            // We also receive here our new headers created by the `Proposer`.
            while let Poll::Ready(Some(header)) = rx_headers.poll_recv(cx) {
                debug!(target: "primary::certifier", authority=?this.authority_id, ?header, "header received!");
                let (tx_cancel, rx_cancel) = oneshot::channel();
                if let Some(cancel) = this.cancel_proposed_header.take() {
                    let _ = cancel.send(());
                }
                this.cancel_proposed_header = Some(tx_cancel);

                let name = this.authority_id;
                let committee = this.committee.clone();
                let certificate_store = this.certificate_store.clone();
                let signature_service = this.signature_service.clone();
                let metrics = this.metrics.clone();
                let network = this.network.clone();
                debug!(target: "primary::certifier", authority=?this.authority_id, "spawning proposer header task...");
                this.propose_header_tasks.spawn(monitored_future!(Self::propose_header(
                    name,
                    committee,
                    certificate_store,
                    signature_service,
                    metrics,
                    network,
                    header,
                    rx_cancel,
                )));
            }
        }

        // Process certificates formed after receiving enough votes.
        let join_next = this.propose_header_tasks.join_next();
        if let Poll::Ready(Some(result)) = pin!(join_next).poll(cx) {
            debug!(target: "primary::certifier", authority=?this.authority_id, ?result, "joining next propose_header_task");
            match result {
                Ok(Ok(certificate)) => {
                    let synchronizer = this.synchronizer.clone();
                    let tx_own_certificate_broadcast = this.tx_own_certificate_broadcast.clone();
                    // TODO: both of these should be fatal and cause shutdown
                    tokio::spawn(async move {
                        // pass to synchronizer for internal processing
                        if let Err(e) =
                            synchronizer.accept_own_certificate(certificate.clone()).await
                        {
                            error!(target: "primary::certifier", "error accepting own certificate: {e}");
                        }

                        // Broadcast the certificate once the synchronizer is ok
                        if let Err(e) = tx_own_certificate_broadcast.send(certificate.clone()) {
                            error!(target: "primary::certifier", ?certificate, ?e, "failed to broadcast certificate to self!");
                        }
                    });
                }
                Ok(Err(e)) => {
                    error!(target: "primary::certifier", authority=?this.authority_id, "Certifier error on proposed header task: {e}");
                }
                Err(e) => {
                    if e.is_cancelled() {
                        // Ungraceful task shutdown.
                        error!(target: "primary::certifier", authority=?this.authority_id, "Certifier error: task cancelled! Shutting down...");
                        //Err(DagError::ShuttingDown);
                    } else if e.is_panic() {
                        error!(target: "primary::certifier", authority=?this.authority_id, "PANIC");
                        // propagate panics.
                        std::panic::resume_unwind(e.into_panic());
                    } else {
                        error!(target: "primary::certifier", authority=?this.authority_id, "PANIC");
                        panic!("propose header task failed: {:?} - {e}", this.authority_id);
                    }
                }
            }
        }

        Poll::Pending
    }
}
