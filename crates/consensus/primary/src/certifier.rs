// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{aggregators::VotesAggregator, synchronizer::Synchronizer};

use consensus_metrics::{metered_channel::Receiver, monitored_future, spawn_logged_monitored_task};
use fastcrypto::signature_service::SignatureService;
use futures::{stream::FuturesUnordered, StreamExt};
use narwhal_network::anemo_ext::NetworkExt;
use narwhal_primary_metrics::PrimaryMetrics;
use narwhal_storage::CertificateStore;
use narwhal_typed_store::traits::Database;
use std::{future::Future, pin::pin, sync::Arc, task::Poll, time::Duration};
use tn_types::{AuthorityIdentifier, Committee, Noticer};

use narwhal_network_types::{PrimaryToPrimaryClient, RequestVoteRequest};
use tn_types::{
    ensure,
    error::{DagError, DagResult},
    BlsSignature, Certificate, CertificateDigest, Header, NetworkPublicKey, Vote,
};
use tokio::{
    sync::oneshot,
    task::{JoinHandle, JoinSet},
};
use tracing::{debug, enabled, error, info, instrument, warn};

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
    signature_service: SignatureService<BlsSignature, { tn_types::INTENT_MESSAGE_LENGTH }>,
    /// Receiver for shutdown.
    rx_shutdown: Noticer,
    /// Receives our newly created headers from the `Proposer`.
    rx_headers: Receiver<Header>,
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
}

impl<DB: Database> Certifier<DB> {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn spawn(
        authority_id: AuthorityIdentifier,
        committee: Committee,
        certificate_store: CertificateStore<DB>,
        synchronizer: Arc<Synchronizer<DB>>,
        signature_service: SignatureService<BlsSignature, { tn_types::INTENT_MESSAGE_LENGTH }>,
        rx_shutdown: Noticer,
        rx_headers: Receiver<Header>,
        metrics: Arc<PrimaryMetrics>,
        primary_network: anemo::Network,
    ) -> JoinHandle<()> {
        spawn_logged_monitored_task!(
            async move {
                info!(target: "primary::certifier", "Certifier on node {} has started successfully.", authority_id);
                Self {
                    authority_id,
                    committee,
                    certificate_store,
                    synchronizer,
                    signature_service,
                    rx_shutdown,
                    rx_headers,
                    cancel_proposed_header: None,
                    propose_header_tasks: JoinSet::new(),
                    network: primary_network,
                    metrics,
                }
                .await;
                info!(target: "primary::certifier", "Certifier on node {} has shutdown.", authority_id);
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
    async fn propose_header(
        authority_id: AuthorityIdentifier,
        committee: Committee,
        certificate_store: CertificateStore<DB>,
        signature_service: SignatureService<BlsSignature, { tn_types::INTENT_MESSAGE_LENGTH }>,
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
                        None => break,
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
                warn!(target: "primary::certifier", ?authority_id, msg, "inside propose_header");
            }
            DagError::CouldNotFormCertificate(header.digest())
        })?;
        debug!(target: "primary::certifier", ?authority_id, "Assembled {certificate:?}");

        Ok(certificate)
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

        // We also receive here our new headers created by the `Proposer`.
        while let Poll::Ready(Some(header)) = this.rx_headers.poll_recv(cx) {
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

        // Process certificates formed after receiving enough votes.
        let join_next = this.propose_header_tasks.join_next();
        if let Poll::Ready(Some(result)) = pin!(join_next).poll(cx) {
            debug!(target: "primary::certifier", authority=?this.authority_id, ?result, "joining next propose_header_task");
            match result {
                Ok(Ok(certificate)) => {
                    let synchronizer = this.synchronizer.clone();
                    tokio::spawn(async move {
                        if let Err(e) = synchronizer.accept_own_certificate(certificate).await {
                            error!("error accepting own certificate: {e}");
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
