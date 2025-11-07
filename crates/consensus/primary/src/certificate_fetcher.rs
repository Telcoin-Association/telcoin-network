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
use tn_network_types::FetchCertificatesResponse;
use tn_primary_metrics::PrimaryMetrics;
use tn_storage::CertificateStore;
use tn_types::{
    validate_received_certificate, AuthorityIdentifier, BlsPublicKey, Certificate, Committee,
    Database, Header, Noticer, Notifier, Round, TaskManager, TaskSpawner, TnReceiver, TnSender,
};
use tokio::{
    task::JoinSet,
    time::{sleep, timeout, Instant},
};
use tracing::{debug, error, instrument, trace};

#[cfg(test)]
#[path = "tests/certificate_fetcher_tests.rs"]
mod certificate_fetcher_tests;

/// Seconds to wait for a response before issuing another parallel fetch request.
const PARALLEL_FETCH_REQUEST_INTERVAL_SECS: Duration = Duration::from_secs(5);
/// The timeout for an iteration of parallel fetch requests over all peers would be
/// num peers * PARALLEL_FETCH_REQUEST_INTERVAL_SECS + PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT
const PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
pub enum CertificateFetcherCommand {
    /// Fetch the certificate and its ancestors.
    Ancestors(Arc<Certificate>),
    /// Fetch once from a random primary.
    Kick,
}

/// Fetches missing certificates from peers. Tries peers in random order until one returns
/// a non-empty response or all peers have been tried.
pub(crate) struct CertificateFetcher<DB> {
    /// Identity of the current authority.
    authority_id: Option<AuthorityIdentifier>,
    /// The committee information.
    committee: Committee,
    /// Persistent storage for certificates. Read-only usage.
    certificate_store: DB,
    /// Network client to fetch certificates from other primaries.
    network: PrimaryNetworkHandle,
    /// Accepts Certificates into local storage.
    state_sync: StateSynchronizer<DB>,
    /// Used to get Receiver for signal of round changes.
    consensus_bus: ConsensusBus,
    /// Receiver for shutdown.
    rx_shutdown: Noticer,
    /// The metrics handler
    metrics: Arc<PrimaryMetrics>,
    /// Task spawner for creating concurrent tasks
    task_spawner: TaskSpawner,
    /// Map of validator to target rounds that local store must catch up to.
    targets: BTreeMap<AuthorityIdentifier, Round>,
    /// Handle to the current fetch task (at most one runs at a time).
    fetch_task: JoinSet<()>,
    /// The max allowable RPC message size shared with peers (in bytes).
    max_rpc_message_size: usize,
}

impl<DB: Database> CertificateFetcher<DB> {
    pub(crate) fn spawn(
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
        let metrics = consensus_bus.primary_metrics().node_metrics.clone();
        let max_rpc_message_size = config.network_config().libp2p_config().max_rpc_message_size;
        let task_spawner = task_manager.get_spawner();

        task_manager.spawn_critical_task(
            "certificate fetcher task",
            monitored_future!(
                async move {
                    Self {
                        authority_id,
                        committee,
                        certificate_store,
                        network,
                        state_sync,
                        consensus_bus,
                        rx_shutdown,
                        metrics,
                        task_spawner,
                        targets: BTreeMap::new(),
                        fetch_task: JoinSet::new(),
                        max_rpc_message_size,
                    }
                    .run()
                    .await
                },
                "CertificateFetcherTask"
            ),
        );
    }

    /// Receive messages on async channels until shutdown.
    async fn run(&mut self) {
        let mut rx_certificate_fetcher = self.consensus_bus.certificate_fetcher().subscribe();

        loop {
            tokio::select! {
                Some(command) = rx_certificate_fetcher.recv() => {
                    self.handle_command(command);
                },

                Some(result) = self.fetch_task.join_next(), if !self.fetch_task.is_empty() => {
                    self.handle_fetch_completion(result);
                },

                _ = &self.rx_shutdown => {
                    return;
                }
            }
        }
    }

    /// Process a certificate fetcher command and potentially start a new fetch task.
    fn handle_command(&mut self, command: CertificateFetcherCommand) {
        let certificate = match command {
            CertificateFetcherCommand::Ancestors(certificate) => certificate,
            CertificateFetcherCommand::Kick => {
                // Start fetch if no task is running and we have targets
                if self.fetch_task.is_empty() && !self.targets.is_empty() {
                    self.start_fetch_task();
                }
                return;
            }
        };

        let header = &certificate.header();

        // skip certificates from wrong epoch
        if header.epoch() != self.committee.epoch() {
            return;
        }

        // skip if later certificate already received from this authority
        if !self.should_fetch_certificate(header) {
            return;
        }

        // update targets and start fetching if needed
        self.targets.insert(header.author().clone(), header.round());

        // start fetching if available space
        if self.fetch_task.is_empty() {
            self.start_fetch_task();
        }
    }

    /// Check if we need to fetch ancestors for this certificate.
    fn should_fetch_certificate(&self, header: &Header) -> bool {
        // skip if a later round is already being fetched from this authority
        if let Some(target_round) = self.targets.get(header.author()) {
            if header.round() <= *target_round {
                return false;
            }
        }

        // skip if this round or later is already in storage
        match self.certificate_store.last_round_number(header.author()) {
            Ok(Some(last_round)) if header.round() <= last_round => false,
            Ok(_) => true,
            Err(e) => {
                error!(target: "primary::cert_fetcher",
                    "Failed to read latest round for {}: {}", header.author(), e);
                false
            }
        }
    }

    /// Handle completion of a fetch task and start another if needed.
    ///
    ///
    ///
    /// TODO: does this need to return error and force shutdown?
    ///
    ///
    ///
    ///
    ///
    ///
    ///
    ///
    ///
    ///
    ///
    /// @@@@!!!!!!!!!!!!! ! ! ! ! ! !!!!
    ///
    ///
    ///
    ///
    ///
    ///
    ///
    /// TOODOODODODODODOOOOOOOO
    fn handle_fetch_completion(&mut self, result: Result<(), tokio::task::JoinError>) {
        if let Err(e) = result {
            if !e.is_cancelled() {
                error!(target: "primary::cert_fetcher", "Fetch task failed: {e}");
            }
        }

        // start next fetch if more targets
        if self.fetch_task.is_empty() && !self.targets.is_empty() {
            self.start_fetch_task();
        }
    }

    /// Start a new task to fetch missing certificates.
    fn start_fetch_task(&mut self) {
        // Update targets - remove any that are already satisfied
        let gc_round = self.gc_round();
        self.update_targets(gc_round);

        // nothing to fetch
        if self.targets.is_empty() {
            debug!(target: "primary::cert_fetcher", "All targets satisfied, skipping fetch");
            return;
        }

        // prepare request with current state of local certificates
        let written_rounds = match self.get_written_rounds(gc_round) {
            Ok(rounds) => rounds,
            Err(e) => {
                error!(target: "primary::cert_fetcher", ?e, "Failed to read certificate store");
                return;
            }
        };

        // create the fetch request
        let request = match MissingCertificatesRequest::default()
            .set_bounds(gc_round, written_rounds)
            .map_err(|e| CertManagerError::RequestBounds(e.to_string()))
            .and_then(|r| Ok(r.set_max_response_size(self.max_rpc_message_size)))
        {
            Ok(req) => req,
            Err(e) => {
                error!(target: "primary::cert_fetcher", ?e, "Failed to create fetch request");
                return;
            }
        };

        // Spawn the fetch task
        let network = self.network.clone();
        let committee = self.committee.clone();
        let state_sync = self.state_sync.clone();
        let metrics = self.metrics.clone();
        let task_spawner = self.task_spawner.clone();
        let authority_id = self.authority_id.clone();

        self.fetch_task.spawn(monitored_future!(async move {
            let _scope = monitored_scope("CertificatesFetching");
            metrics.certificate_fetcher_inflight_fetch.inc();

            let result = fetch_and_process_certificates(
                authority_id,
                network,
                committee,
                request,
                state_sync,
                task_spawner,
            )
            .await;

            if let Err(e) = result {
                debug!(target: "primary::cert_fetcher", ?e, "Fetch task completed with error");
            }

            metrics.certificate_fetcher_inflight_fetch.dec();
        }));
    }

    /// Update targets by removing any that have already been satisfied.
    fn update_targets(&mut self, gc_round: Round) {
        self.targets.retain(|origin, target_round| {
            // Skip fetching anything at or below GC round
            if *target_round <= gc_round {
                return false;
            }

            // check if we already have this round or later in storage
            match self.certificate_store.last_round_number(origin) {
                Ok(Some(last_round)) => last_round < *target_round,
                Ok(None) => *target_round > gc_round,
                Err(e) => {
                    error!(target: "primary::cert_fetcher",
                        "Failed to check last round for {origin}: {e}");
                    false
                }
            }
        });
    }

    /// Get the current set of written rounds for each authority.
    fn get_written_rounds(
        &self,
        gc_round: Round,
    ) -> Result<BTreeMap<AuthorityIdentifier, BTreeSet<Round>>, CertManagerError> {
        let mut written_rounds = BTreeMap::new();

        // initialize for all authorities
        for authority in self.committee.authorities() {
            written_rounds.insert(authority.id(), BTreeSet::new());
        }

        // populate with actual written rounds
        // NOTE: origins_after_round() is inclusive.
        let origins = self
            .certificate_store
            .origins_after_round(gc_round + 1)
            .map_err(|e| CertManagerError::Storage(e))?;

        for (round, origins_at_round) in origins {
            for origin in origins_at_round {
                written_rounds.entry(origin).or_default().insert(round);
            }
        }

        Ok(written_rounds)
    }

    /// Read latest gc round from consensus bus watch channel.
    fn gc_round(&self) -> Round {
        *self.consensus_bus.gc_round_updates().borrow()
    }
}

/// Fetch missing certificates from peers and process them.
/// Tries peers in random order with parallel requests.
async fn fetch_and_process_certificates<DB: Database>(
    authority_id: Option<AuthorityIdentifier>,
    network: PrimaryNetworkHandle,
    committee: Committee,
    request: MissingCertificatesRequest,
    state_sync: StateSynchronizer<DB>,
    task_spawner: TaskSpawner,
) -> CertManagerResult<()> {
    // get randomized list of peers
    let mut peers: Vec<_> = committee
        .others_primaries_by_id(authority_id.as_ref())
        .into_iter()
        .map(|(_, key)| key)
        .collect();
    peers.shuffle(&mut ThreadRng::default());

    // calculate timeout based on number of peers
    let timeout_duration = PARALLEL_FETCH_REQUEST_INTERVAL_SECS * peers.len() as u32
        + PARALLEL_FETCH_REQUEST_ADDITIONAL_TIMEOUT;

    // try fetching from peers with staggered parallel requests
    let certificates =
        timeout(timeout_duration, fetch_from_peers(network, peers, request, task_spawner))
            .await
            .map_err(|_| {
            debug!(target: "primary::cert_fetcher", "Certificate fetch timed out");
            CertManagerError::Timeout
        })??;

    // process the certificates
    state_sync.process_fetched_certificates_in_parallel(certificates).await?;

    Ok(())
}

/// Try to fetch certificates from multiple peers with staggered requests.
/// Sends requests to peers one at a time with ~5 second intervals between each.
/// Returns as soon as one peer provides a non-empty response.
async fn fetch_from_peers(
    network: PrimaryNetworkHandle,
    peers: Vec<BlsPublicKey>,
    request: MissingCertificatesRequest,
    task_spawner: TaskSpawner,
) -> CertManagerResult<Vec<Certificate>> {
    let (tx_results, mut rx_results) = tokio::sync::mpsc::unbounded_channel();
    let cancel_signal = Arc::new(Notifier::new());

    const STAGGER_INTERVAL: Duration = Duration::from_secs(5);

    // track requests per peer
    let mut peer_index = 0;

    // spawn first peer fetch immediately
    if !peers.is_empty() {
        spawn_peer_fetch(
            &task_spawner,
            peers[peer_index],
            request.clone(),
            network.clone(),
            tx_results.clone(),
            cancel_signal.subscribe(),
        );
        peer_index += 1;
    } else {
        return Err(CertManagerError::NoCertificateFetched);
    }

    // timer for spawning next peer request
    let mut spawn_timer = Box::pin(sleep(STAGGER_INTERVAL));

    loop {
        tokio::select! {
            Some(result) = rx_results.recv() => {
                if let Ok(certificates) = result {
                    if !certificates.is_empty() {
                        debug!(target: "primary::cert_fetcher",
                            "Fetched {} certificates successfully", certificates.len());
                        // success - cancel all other fetch tasks
                        cancel_signal.notify();
                        return Ok(certificates);
                    }
                    // empty response - continue with other peers
                    debug!(target: "primary::cert_fetcher",
                        "Received empty certificate response, continuing with other peers");
                }

                // check if all peers have been tried and all tasks completed
                if peer_index >= peers.len() && tx_results.is_closed() {
                    debug!(target: "primary::cert_fetcher",
                        "All peers tried without success");
                    // wait a bit before returning to avoid immediate retry
                    sleep(STAGGER_INTERVAL).await;
                    return Err(CertManagerError::NoCertificateFetched);
                }
            }

            _ = &mut spawn_timer, if peer_index < peers.len() => {
                // time to spawn next peer fetch
                debug!(target: "primary::cert_fetcher",
                    "Spawning fetch request to peer {} of {}", peer_index + 1, peers.len());
                spawn_peer_fetch(
                    &task_spawner,
                    peers[peer_index],
                    request.clone(),
                    network.clone(),
                    tx_results.clone(),
                    cancel_signal.subscribe(),
                );
                peer_index += 1;

                // reset timer for next spawn
                spawn_timer = Box::pin(sleep(STAGGER_INTERVAL));
            }

            else => {
                // all peers spawned, no results yet - wait for tasks to complete
                if tx_results.is_closed() {
                    debug!(target: "primary::cert_fetcher",
                        "No peer could provide certificates");
                    sleep(STAGGER_INTERVAL).await;
                    return Err(CertManagerError::NoCertificateFetched);
                }
                // continue waiting for results
            }
        }
    }
}

/// Spawn a task to fetch certificates from a single peer.
fn spawn_peer_fetch(
    task_spawner: &TaskSpawner,
    peer: BlsPublicKey,
    request: MissingCertificatesRequest,
    network: PrimaryNetworkHandle,
    tx_results: tokio::sync::mpsc::UnboundedSender<CertManagerResult<Vec<Certificate>>>,
    cancel_signal: Noticer,
) {
    task_spawner.spawn_task(format!("fetch-cert-{peer}"), async move {
        tokio::select! {
            result = network.fetch_certificates(peer, request) => {
                let _ = tx_results.send(result.map_err(Into::into));
            }
            _ = cancel_signal => {
                // Cancelled - another peer succeeded
            }
        }
    });
}
