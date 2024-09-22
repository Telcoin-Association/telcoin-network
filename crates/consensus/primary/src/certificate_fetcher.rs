// Copyright (c) Telcoin, LLC
// Copyright (c) 2021, Facebook, Inc. and its affiliates
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Fetch missing certificates.
use crate::{consensus::ConsensusRound, synchronizer::Synchronizer};
use anemo::Request;
use consensus_metrics::{
    metered_channel::Receiver, monitored_future, monitored_scope, spawn_logged_monitored_task,
};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use narwhal_network::PrimaryToPrimaryRpc;
use narwhal_primary_metrics::PrimaryMetrics;
use narwhal_storage::CertificateStore;
use narwhal_typed_store::traits::Database;
use rand::{rngs::ThreadRng, seq::SliceRandom};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tn_types::{AuthorityIdentifier, Committee, NetworkPublicKey};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::BroadcastStream;

use narwhal_network_types::{FetchCertificatesRequest, FetchCertificatesResponse};
use tn_types::{
    error::{DagError, DagResult},
    validate_received_certificate_version, Certificate, ConditionalBroadcastReceiver, Round,
};
use tokio::{
    sync::watch,
    task::{JoinHandle, JoinSet},
    time::{sleep, timeout, Instant},
};
use tracing::{debug, error, instrument, trace, warn};

#[cfg(test)]
#[path = "tests/certificate_fetcher_tests.rs"]
pub mod certificate_fetcher_tests;

/// The maximum number of certificates to fetch in a single request.
const MAX_CERTIFICATES_TO_FETCH: usize = 2_000;
/// The number of seconds to wait for a response before issuing another fetch request in parallel.
const PARALLEL_FETCH_REQUEST_INTERVAL_SECS: Duration = Duration::from_secs(5);
/// An additional amount of time to wait before timing out on fetch requests.
///
/// The timeout for an iteration of parallel fetch requests over all peers is:
/// num peers * PARALLEL_FETCH_REQUEST_INTERVAL_SECS + PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT
const PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT: Duration = Duration::from_secs(15);
/// Convenience type that contains all information needed to fetch missing certificates.
struct FetchArgs<DB> {
    /// Identity of the current authority.
    authority_id: AuthorityIdentifier,
    /// Network client to fetch certificates from other primaries.
    network: anemo::Network,
    /// Accepts Certificates into local storage.
    synchronizer: Synchronizer<DB>,
    /// The metrics handler
    metrics: Arc<PrimaryMetrics>,
    /// The committee information.
    committee: Committee,
}

#[derive(Clone, Debug)]
/// Variations of the type of certificate to fetch.
pub enum CertificateFetcherCommand {
    /// Fetch the certificate and its ancestors.
    Ancestors(Certificate),
    /// Fetch once from a random primary.
    ///
    /// This command is used as a fallback attempt to receive any certificate from any primary after a period of time without receiving any certificates from the network.
    Any,
}

/// The CertificateFetcher is responsible for fetching certificates that this primary is missing
/// from peers. It operates a loop which listens for commands to fetch a specific certificate's
/// ancestors, or just to start one fetch attempt.
///
/// In each fetch, the CertificateFetcher first scans locally available certificates. Then it sends
/// this information to a random peer. The peer would reply with the missing certificates that can
/// be accepted by this primary. After a fetch completes, another one will start immediately if
/// there are more certificates missing ancestors.
pub(crate) struct CertificateFetcher<DB> {
    /// Identity of the current authority.
    authority_id: AuthorityIdentifier,
    /// Network client to fetch certificates from other primaries.
    network: anemo::Network,
    /// Accepts Certificates into local storage.
    synchronizer: Synchronizer<DB>,
    /// The metrics handler
    metrics: Arc<PrimaryMetrics>,
    /// The committee information.
    committee: Committee,
    /// Persistent storage for certificates. Read-only usage.
    certificate_store: CertificateStore<DB>,
    /// Receiver for signal of round changes.
    rx_consensus_round_updates: watch::Receiver<ConsensusRound>,
    /// Receiver for shutdown.
    rx_shutdown_stream: BroadcastStream<()>,
    /// Receives certificates with missing parents from the `Synchronizer`.
    rx_certificate_fetcher: Receiver<CertificateFetcherCommand>,
    /// Map of validator to target rounds that local store must catch up to.
    /// The targets are updated with each certificate missing parents sent from the core.
    /// Each fetch task may satisfy some / all / none of the targets.
    /// TODO: rethink the stopping criteria for fetching, balance simplicity with completeness
    /// of certificates (for avoiding jitters of voting / processing certificates instead of
    /// correctness).
    targets: BTreeMap<AuthorityIdentifier, Round>,
    /// Keeps the handle to the (at most one) inflight fetch certificates task.
    fetch_certificates_task: JoinSet<()>,
}

impl<DB: Database> CertificateFetcher<DB> {
    /// Create a new instance of `Self`.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub(crate) fn new(
        authority_id: AuthorityIdentifier,
        committee: Committee,
        network: anemo::Network,
        certificate_store: CertificateStore<DB>,
        rx_consensus_round_updates: watch::Receiver<ConsensusRound>,
        rx_shutdown: ConditionalBroadcastReceiver,
        rx_certificate_fetcher: Receiver<CertificateFetcherCommand>,
        synchronizer: Synchronizer<DB>,
        metrics: Arc<PrimaryMetrics>,
    ) -> Self {
        let rx_shutdown_stream = BroadcastStream::new(rx_shutdown.receiver);
        Self {
            authority_id,
            network,
            synchronizer,
            metrics,
            committee,
            certificate_store,
            rx_consensus_round_updates,
            rx_shutdown_stream,
            rx_certificate_fetcher,
            targets: BTreeMap::new(),
            fetch_certificates_task: JoinSet::new(),
        }
    }

    /// Convenience method for obtaining all the information needed to fetch missing certificates.
    fn fetch_args(&self) -> FetchArgs<DB> {
        // Skip fetching certificates that already exist locally.
        let mut written_rounds = BTreeMap::<AuthorityIdentifier, BTreeSet<Round>>::new();
        for authority in self.committee.authorities() {
            // Initialize written_rounds for all authorities, because the handler only sends back
            // certificates for the set of authorities here.
            written_rounds.insert(authority.id(), BTreeSet::new());
        }
        FetchArgs {
            authority_id: self.authority_id,
            network: self.network.clone(),
            synchronizer: self.synchronizer.clone(),
            metrics: self.metrics.clone(),
            committee: self.committee.clone(),
        }
    }

    /// Fallback attempt to retrieve certificates from the network.
    ///
    /// This method is triggered by `Synchronizer` after a period of time where no commits have happened in consensus.
    ///
    /// !!!!!!!!!!!!!!!!!!!!!!!!!!
    /// TODO: cleanup comment here
    ///
    /// Starts a task to fetch missing certificates from other primaries.
    /// A call to kickstart() can be triggered by a certificate with missing parents or the end of a
    /// fetch task. Each iteration of kickstart() updates the target rounds, and iterations will
    /// continue until there are no more target rounds to catch up to.
    #[allow(clippy::mutable_key_type)]
    fn kickstart(&mut self) {
        // Skip fetching certificates at or below the gc round.
        let gc_round = self.gc_round();
        // Skip fetching certificates that already exist locally.
        let mut written_rounds = BTreeMap::<AuthorityIdentifier, BTreeSet<Round>>::new();
        for authority in self.committee.authorities() {
            // Initialize written_rounds for all authorities, because the handler only sends back
            // certificates for the set of authorities here.
            written_rounds.insert(authority.id(), BTreeSet::new());
        }
        // NOTE: origins_after_round() is inclusive.
        match self.certificate_store.origins_after_round(gc_round + 1) {
            Ok(origins) => {
                for (round, origins) in origins {
                    for origin in origins {
                        written_rounds.entry(origin).or_default().insert(round);
                    }
                }
            }
            Err(e) => {
                error!(target: "primary::cert_fetcher", ?e, "failed to read from certificate store");
                return;
            }
        };

        self.targets.retain(|origin, target_round| {
            let last_written_round = written_rounds
                .get(origin)
                .map_or(gc_round, |rounds| rounds.last().unwrap_or(&gc_round).to_owned());
            // Drop sync target when cert store already has an equal or higher round for the origin.
            // This applies GC to targets as well.
            //
            // NOTE: even if the store actually does not have target_round for the origin,
            // it is ok to stop fetching without this certificate.
            // If this certificate becomes a parent of other certificates, another
            // fetch will be triggered eventually because of missing certificates.
            last_written_round < *target_round
        });

        if self.targets.is_empty() {
            debug!(target: "primary::cert_fetcher", "Certificates have caught up. Skip fetching.");
            return;
        }

        let committee = self.committee.clone();

        debug!(
            target: "primary::cert_fetcher",
            "Starting task to fetch missing certificates: max target {}, gc round {:?}",
            self.targets.values().max().unwrap_or(&0),
            gc_round
        );
        self.fetch_certificates_task.spawn(monitored_future!(async move {
            let _scope = monitored_scope("CertificatesFetching");
            state.metrics.certificate_fetcher_inflight_fetch.inc();

            let now = Instant::now();
            match run_fetch_task(state.clone(), committee, gc_round, written_rounds).await {
                Ok(_) => {
                    debug!(
                        target: "primary::cert_fetcher",
                        "Finished task to fetch certificates successfully, elapsed = {}s",
                        now.elapsed().as_secs_f64()
                    );
                }
                Err(e) => {
                    error!(target: "primary::cert_fetcher", ?e, "Error from fetch certificates task");
                }
            };

            state.metrics.certificate_fetcher_inflight_fetch.dec();
        }));
    }

    fn gc_round(&self) -> Round {
        self.rx_consensus_round_updates.borrow().gc_round
    }
}

#[allow(clippy::mutable_key_type)]
#[instrument(level = "debug", skip_all)]
async fn run_fetch_task<DB: Database>(
    state: FetchArgs<DB>,
    committee: Committee,
    gc_round: Round,
    written_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
) -> DagResult<()> {
    // Send request to fetch certificates.
    let request = FetchCertificatesRequest::default()
        .set_bounds(gc_round, written_rounds)
        .set_max_items(MAX_CERTIFICATES_TO_FETCH);
    let Some(response) =
        fetch_certificates_helper(state.authority_id, &state.network, &committee, request).await
    else {
        error!(target: "primary::cert_fetcher", "error awaiting fetch_certificates_helper");
        return Err(DagError::NoCertificateFetched);
    };

    // Process and store fetched certificates.
    let num_certs_fetched = response.certificates.len();
    process_certificates_helper(response, &state.synchronizer, state.metrics.clone()).await?;
    state.metrics.certificate_fetcher_num_certificates_processed.inc_by(num_certs_fetched as u64);

    debug!(target: "primary::cert_fetcher", "Successfully fetched and processed {num_certs_fetched} certificates");
    Ok(())
}

/// Fetches certificates from other primaries concurrently, with ~5 sec interval between each
/// request. Terminates after the 1st successful response is received.
#[instrument(level = "debug", skip_all)]
async fn fetch_certificates_helper(
    name: AuthorityIdentifier,
    network: &anemo::Network,
    committee: &Committee,
    request: FetchCertificatesRequest,
) -> Option<FetchCertificatesResponse> {
    let _scope = monitored_scope("FetchingCertificatesFromPeers");
    trace!(target: "primary::cert_fetcher", "Start sending fetch certificates requests");

    // TODO: make this a config parameter.
    let request_interval = PARALLEL_FETCH_REQUEST_INTERVAL_SECS;
    let mut peers: Vec<NetworkPublicKey> = committee
        .others_primaries_by_id(name)
        .into_iter()
        .map(|(_, _, network_key)| network_key)
        .collect();
    peers.shuffle(&mut ThreadRng::default());
    let fetch_timeout = PARALLEL_FETCH_REQUEST_INTERVAL_SECS * peers.len().try_into().unwrap()
        + PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT;

    let fetch_callback = async move {
        debug!(target: "primary::cert_fetcher", "Starting to fetch certificates");
        let mut fut = FuturesUnordered::new();
        // Loop until one peer returns with certificates, or no peer does.
        loop {
            if let Some(peer) = peers.pop() {
                let request = Request::new(request.clone())
                    .with_timeout(PARALLEL_FETCH_REQUEST_INTERVAL_SECS * 2);
                fut.push(monitored_future!(async move {
                    debug!(target: "primary::cert_fetcher", "Sending out fetch request in parallel to {peer}");
                    let result = network.fetch_certificates(&peer, request).await;
                    if let Ok(resp) = &result {
                        debug!(target: "primary::cert_fetcher", "Fetched {} certificates from peer {peer}", resp.certificates.len());
                    }
                    result
                }));
            }

            let mut interval = Box::pin(sleep(request_interval));

            tokio::select! {
                res = fut.next() => match res {
                    Some(Ok(resp)) => {
                        if resp.certificates.is_empty() {
                            // Issue request to another primary immediately.
                            continue;
                        }
                        return Some(resp);
                    }
                    Some(Err(e)) => {
                        debug!(target: "primary::cert_fetcher", "Failed to fetch certificates: {e}");
                        // Issue request to another primary immediately.
                        continue;
                    }
                    None => {
                        debug!(target: "primary::cert_fetcher", "No peer can be reached for fetching certificates!");
                        // Last or all requests to peers may have failed immediately, so wait
                        // before returning to avoid retrying fetching immediately.
                        sleep(request_interval).await;
                        return None;
                    }
                },
                _ = &mut interval => {
                    // Not response received in the last interval. Send out another fetch request
                    // in parallel, if there is a peer that has not been sent to.
                }
            };
        }
    };

    match timeout(fetch_timeout, fetch_callback).await {
        Ok(result) => result,
        Err(e) => {
            debug!(target: "primary::cert_fetcher", "Timed out fetching certificates: {e}");
            None
        }
    }
}

#[instrument(level = "debug", skip_all)]
async fn process_certificates_helper<DB: Database>(
    response: FetchCertificatesResponse,
    synchronizer: &Synchronizer<DB>,
    _metrics: Arc<PrimaryMetrics>,
) -> DagResult<()> {
    trace!(target: "primary::cert_fetcher", "Start sending fetched certificates to processing");
    if response.certificates.len() > MAX_CERTIFICATES_TO_FETCH {
        return Err(DagError::TooManyFetchedCertificatesReturned(
            response.certificates.len(),
            MAX_CERTIFICATES_TO_FETCH,
        ));
    }

    // We should not be getting mixed versions of certificates from a
    // validator, so any individual certificate with mismatched versions
    // should cancel processing for the entire batch of fetched certificates.
    let certificates = response
        .certificates
        .into_iter()
        .map(|cert| {
            validate_received_certificate_version(cert).map_err(|err| {
                error!(target: "primary::cert_fetcher", "fetched certficate processing error: {err}");
                DagError::InvalidCertificateVersion
            })
        })
        .collect::<DagResult<Vec<Certificate>>>()?;

    // In PrimaryReceiverHandler, certificates already in storage are ignored.
    // The check is unnecessary here, because there is no concurrent processing of older
    // certificates. For byzantine failures, the check will not be effective anyway.
    let _scope = monitored_scope("ProcessingFetchedCertificates");

    synchronizer.try_accept_fetched_certificates(certificates).await?;

    trace!(target: "primary::cert_fetcher", "Fetched certificates have been processed");

    Ok(())
}

impl<DB> Future for CertificateFetcher<DB>
where
    DB: Database,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // check for shutdown signal
        //
        // okay to shutdown here because other primary tasks are expected to shutdown too
        // ie) no point completing the proposal if certifier is down
        if let Poll::Ready(Some(_shutdown)) = this.rx_shutdown_stream.poll_next_unpin(cx) {
            warn!(target: "primary::cert_fetcher", authority=?this.authority_id, "received shutdown signal...");
            return Poll::Ready(());
        }

        // process requests to fetch certificates
        while let Poll::Ready(Some(fetch)) = this.rx_certificate_fetcher.poll_recv(cx) {
            todo!()
        }

        // poll certificate fetch task
        // if let Some(mut receiver) = this.fetch_certificate_task.take() {
        //     match receiver.poll_unpin(cx) {
        //         Poll::Ready(res) => {
        //             debug!(target: "primary::proposer", authority=?this.authority_id, "pending header task complete!");
        //             this.handle_proposal_result(res)?;
        //             // continue the loop to propose the next header
        //             continue;
        //         }
        //     }
        // }

        Poll::Pending
    }
}
