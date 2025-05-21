//! Fetch missing certificates from peers and verify them.

use crate::{
    error::{CertManagerError, CertManagerResult},
    network::{MissingCertificatesRequest, PrimaryNetworkHandle},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use consensus_metrics::{monitored_future, monitored_scope};
use futures::{stream::FuturesUnordered, StreamExt};
use rand::{rngs::ThreadRng, seq::SliceRandom};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tn_config::ConsensusConfig;
use tn_network_libp2p::PeerId;
use tn_network_types::FetchCertificatesResponse;
use tn_primary_metrics::PrimaryMetrics;
use tn_storage::CertificateStore;
use tn_types::{
    validate_received_certificate, AuthorityIdentifier, Certificate, Committee, Database, Noticer,
    Round, TaskManager, TnReceiver, TnSender,
};
use tokio::{
    task::JoinSet,
    time::{sleep, timeout, Instant},
};
use tracing::{debug, error, instrument, trace};

#[cfg(test)]
#[path = "tests/certificate_fetcher_tests.rs"]
pub mod certificate_fetcher_tests;

// Maximum number of certificates to fetch with one request.
const MAX_CERTIFICATES_TO_FETCH: usize = 2_000;
// Seconds to wait for a response before issuing another parallel fetch request.
const PARALLEL_FETCH_REQUEST_INTERVAL_SECS: Duration = Duration::from_secs(5);
// The timeout for an iteration of parallel fetch requests over all peers would be
// num peers * PARALLEL_FETCH_REQUEST_INTERVAL_SECS + PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT
const PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
pub enum CertificateFetcherCommand {
    /// Fetch the certificate and its ancestors.
    Ancestors(Arc<Certificate>),
    /// Fetch once from a random primary.
    Kick,
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
    /// Internal state of CertificateFetcher.
    state: Arc<CertificateFetcherState<DB>>,
    /// The committee information.
    committee: Committee,
    /// Persistent storage for certificates. Read-only usage.
    certificate_store: DB,
    /// Used to get Receiver for signal of round changes.
    consensus_bus: ConsensusBus,
    /// Receiver for shutdown.
    rx_shutdown: Noticer,
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

/// Thread-safe internal state of CertificateFetcher shared with its fetch task.
struct CertificateFetcherState<DB> {
    /// Identity of the current authority.
    authority_id: Option<AuthorityIdentifier>,
    /// Network client to fetch certificates from other primaries.
    network: PrimaryNetworkHandle,
    /// Accepts Certificates into local storage.
    state_sync: StateSynchronizer<DB>,
    /// The metrics handler
    metrics: Arc<PrimaryMetrics>,
}

impl<DB: Database> CertificateFetcher<DB> {
    pub fn spawn(
        config: ConsensusConfig<DB>,
        network: PrimaryNetworkHandle,
        consensus_bus: ConsensusBus,
        state_sync: StateSynchronizer<DB>,
        task_manager: &TaskManager,
    ) {
        let authority_id = config.authority_id();
        let committee = config.committee().clone();
        let certificate_store = config.node_storage().clone();
        let rx_shutdown = config.shutdown().subscribe();
        let state = Arc::new(CertificateFetcherState {
            authority_id,
            network,
            state_sync,
            metrics: consensus_bus.primary_metrics().node_metrics.clone(),
        });

        task_manager.spawn_critical_task(
            "certificate fetcher task",
            monitored_future!(
                async move {
                    Self {
                        state,
                        committee,
                        certificate_store,
                        consensus_bus,
                        rx_shutdown,
                        targets: BTreeMap::new(),
                        fetch_certificates_task: JoinSet::new(),
                    }
                    .run()
                    .await
                },
                "CertificateFetcherTask"
            ),
        );
    }

    async fn run(&mut self) {
        let cb_clone = self.consensus_bus.clone();
        let mut rx_certificate_fetcher = cb_clone.certificate_fetcher().subscribe();
        loop {
            tokio::select! {
                Some(command) = rx_certificate_fetcher.recv() => {
                    let certificate = match command {
                        CertificateFetcherCommand::Ancestors(certificate) => certificate,
                        CertificateFetcherCommand::Kick => {
                            // Kick start a fetch task if there is no other task running.
                            if self.fetch_certificates_task.is_empty() {
                                self.kickstart();
                            }
                            continue;
                        }
                    };
                    let header = &certificate.header();
                    if header.epoch() != self.committee.epoch() {
                        continue;
                    }
                    // Unnecessary to validate the header and certificate further, since it has
                    // already been validated.

                    if let Some(r) = self.targets.get(header.author()) {
                        if header.round() <= *r {
                            // Ignore fetch request when we already need to sync to a later
                            // certificate from the same authority. Although this certificate may
                            // not be the parent of the later certificate, this should be ok
                            // because eventually a child of this certificate will miss parents and
                            // get inserted into the targets.
                            //
                            // Basically, it is ok to stop fetching without this certificate.
                            // If this certificate becomes a parent of other certificates, another
                            // fetch will be triggered eventually because of missing certificates.
                            continue;
                        }
                    }

                    // The header should have been verified as part of the certificate.
                    match self.certificate_store.last_round_number(header.author()) {
                        Ok(r) => {
                            if header.round() <= r.unwrap_or(0) {
                                // Ignore fetch request. Possibly the certificate was processed
                                // while the message is in the queue.
                                continue;
                            }
                            // Otherwise, continue to update fetch targets.
                        }
                        Err(e) => {
                            // If this happens, it is most likely due to serialization error.
                            error!("Failed to read latest round for {}: {}", header.author(), e);
                            continue;
                        }
                    };

                    // Update the target rounds for the authority.
                    self.targets.insert(header.author().clone(), header.round());

                    // Kick start a fetch task if there is no other task running.
                    if self.fetch_certificates_task.is_empty() {
                        self.kickstart();
                    }
                },
                Some(result) = self.fetch_certificates_task.join_next(), if !self.fetch_certificates_task.is_empty() => {
                    match result {
                        Ok(()) => {},
                        Err(e) => {
                            if e.is_cancelled() {
                                // avoid crashing on ungraceful shutdown
                            } else if e.is_panic() {
                                // propagate panics.
                                std::panic::resume_unwind(e.into_panic());
                            } else {
                                panic!("fetch certificates task failed: {e}");
                            }
                        },
                    };

                    // Kick start another fetch task after the previous one terminates.
                    // If all targets have been fetched, the new task will clean up the targets and exit.
                    if self.fetch_certificates_task.is_empty() {
                        self.kickstart();
                    }
                },
                _ = &self.rx_shutdown => {
                    return
                }
            }
        }
    }

    // Starts a task to fetch missing certificates from other primaries.
    // A call to kickstart() can be triggered by a certificate with missing parents or the end of a
    // fetch task. Each iteration of kickstart() updates the target rounds, and iterations will
    // continue until there are no more target rounds to catch up to.
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

        let state = self.state.clone();
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
                    debug!(target: "primary::cert_fetcher",
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
        *self.consensus_bus.gc_round_updates().borrow()
    }
}

#[allow(clippy::mutable_key_type)]
#[instrument(level = "debug", skip_all)]
async fn run_fetch_task<DB: Database>(
    state: Arc<CertificateFetcherState<DB>>,
    committee: Committee,
    gc_round: Round,
    written_rounds: BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
) -> CertManagerResult<()> {
    // Send request to fetch certificates.
    let request = MissingCertificatesRequest::default()
        .set_bounds(gc_round, written_rounds)
        .map_err(|e| CertManagerError::RequestBounds(e.to_string()))?
        .set_max_items(MAX_CERTIFICATES_TO_FETCH);
    let Some(response) = fetch_certificates_helper(
        state.authority_id.as_ref(),
        state.network.clone(),
        &committee,
        request,
    )
    .await
    else {
        error!(target: "primary::cert_fetcher", "error awaiting fetch_certificates_helper");
        return Err(CertManagerError::NoCertificateFetched);
    };

    // Process and store fetched certificates.
    let num_certs_fetched = response.certificates.len();
    process_certificates_helper(response, &state.state_sync, state.metrics.clone()).await?;
    state.metrics.certificate_fetcher_num_certificates_processed.inc_by(num_certs_fetched as u64);

    debug!(target: "primary::cert_fetcher", "Successfully fetched and processed {num_certs_fetched} certificates");
    Ok(())
}

/// Fetches certificates from other primaries concurrently, with ~5 sec interval between each
/// request. Terminates after the 1st successful response is received.
#[instrument(level = "debug", skip_all)]
async fn fetch_certificates_helper(
    name: Option<&AuthorityIdentifier>,
    network: PrimaryNetworkHandle,
    committee: &Committee,
    request: MissingCertificatesRequest,
) -> Option<FetchCertificatesResponse> {
    let _scope = monitored_scope("FetchingCertificatesFromPeers");
    trace!(target: "primary::cert_fetcher", "Start sending fetch certificates requests");
    // TODO: make this a config parameter.
    let request_interval = PARALLEL_FETCH_REQUEST_INTERVAL_SECS;
    let mut peers: Vec<PeerId> = committee
        .others_primaries_by_id(name)
        .into_iter()
        .map(|(auth_id, _, _)| auth_id.peer_id())
        .collect();
    peers.shuffle(&mut ThreadRng::default());
    let fetch_timeout = PARALLEL_FETCH_REQUEST_INTERVAL_SECS
        * peers.len().try_into().expect("usize into secs duration")
        + PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT;
    let fetch_callback = async move {
        debug!(target: "primary::cert_fetcher", "Starting to fetch certificates");
        let mut fut = FuturesUnordered::new();
        // Loop until one peer returns with certificates, or no peer does.
        loop {
            if let Some(peer) = peers.pop() {
                let request_clone = request.clone();
                let network_clone = network.clone();
                fut.push(monitored_future!(async move {
                    debug!(target: "primary::cert_fetcher", "Sending out fetch request in parallel to {peer}");
                    let result = network_clone.fetch_certificates(peer, request_clone).await;
                    if let Ok(certificates) = &result {
                        debug!(target: "primary::cert_fetcher", "Fetched {} certificates from peer {peer}", certificates.len());
                    }
                    result
                }));
            }
            let mut interval = Box::pin(sleep(request_interval));
            tokio::select! {
                res = fut.next() => match res {
                    Some(Ok(certificates)) => {
                        if certificates.is_empty() {
                            // Issue request to another primary immediately.
                            continue;
                        }
                        return Some(FetchCertificatesResponse { certificates });
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
        Ok(response) => response,
        Err(e) => {
            debug!(target: "primary::cert_fetcher", "Timed out fetching certificates: {e}");
            None
        }
    }
}

#[instrument(level = "debug", skip_all)]
async fn process_certificates_helper<DB: Database>(
    response: FetchCertificatesResponse,
    state_sync: &StateSynchronizer<DB>,
    _metrics: Arc<PrimaryMetrics>,
) -> CertManagerResult<()> {
    trace!(target: "primary::cert_fetcher", "Start sending fetched certificates to processing");
    if response.certificates.len() > MAX_CERTIFICATES_TO_FETCH {
        return Err(CertManagerError::TooManyFetchedCertificatesReturned {
            response: response.certificates.len(),
            request: MAX_CERTIFICATES_TO_FETCH,
        });
    }

    // We should not be getting mixed versions of certificates from a
    // validator, so any individual certificate with mismatched versions
    // should cancel processing for the entire batch of fetched certificates.
    let certificates = response
        .certificates
        .into_iter()
        .map(|cert| {
            let res = validate_received_certificate(cert).inspect_err(|err| {
                error!(target: "primary::cert_fetcher", "fetched certficate processing error: {err}");
            });
            Ok(res?)
        })
        .collect::<CertManagerResult<Vec<Certificate>>>()?;

    // In PrimaryReceiverHandler, certificates already in storage are ignored.
    // The check is unnecessary here, because there is no concurrent processing of older
    // certificates. For byzantine failures, the check will not be effective anyway.
    let _scope = monitored_scope("ProcessingFetchedCertificates");

    state_sync.process_fetched_certificates_in_parallel(certificates).await?;

    trace!(target: "primary::cert_fetcher", "Fetched certificates have been processed");

    Ok(())
}
