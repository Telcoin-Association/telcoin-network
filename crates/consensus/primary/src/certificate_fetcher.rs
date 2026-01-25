//! Fetch missing certificates from peers and verify them.

use crate::{
    consensus::gc_round,
    error::{CertManagerError, CertManagerResult},
    network::{MissingCertificatesRequest, PrimaryNetworkHandle},
    state_sync::StateSynchronizer,
    ConsensusBus,
};
use consensus_metrics::{monitored_future, monitored_scope};
use rand::{rngs::ThreadRng, seq::SliceRandom};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tn_config::ConsensusConfig;
use tn_primary_metrics::PrimaryMetrics;
use tn_storage::CertificateStore;
use tn_types::{
    validate_fetched_certificate, AuthorityIdentifier, BlsPublicKey, Certificate, Committee,
    Database, Header, Noticer, Notifier, Round, TaskManager, TaskSpawner, TnReceiver, TnSender,
};
use tokio::{
    sync::oneshot,
    time::{sleep, timeout},
};
use tracing::{debug, error, warn};

#[cfg(test)]
#[path = "tests/certificate_fetcher_tests.rs"]
mod certificate_fetcher_tests;

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
    fetch_task: FetchTask,
    /// The max allowable RPC message size shared with peers (in bytes).
    max_rpc_message_size: usize,
    /// Delay duration before issuing another parallel fetch request for missing certs.
    parallel_fetch_request_delay_interval: Duration,
    /// Configuration.
    config: ConsensusConfig<DB>,
}

impl<DB: Database> CertificateFetcher<DB> {
    /// Spawn the long-running certificate fetcher.
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
        let parallel_fetch_request_delay_interval =
            config.parameters().parallel_fetch_request_delay_interval;

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
                        fetch_task: FetchTask::new(),
                        max_rpc_message_size,
                        parallel_fetch_request_delay_interval,
                        config,
                    }
                    .run()
                    .await
                    .map_err(|e| {
                        error!(target: "primary::cert_fetcher", ?e, "cert fetcher shutting down");
                    })
                },
                "CertificateFetcherTask"
            ),
        );
    }

    /// Receive messages on async channels until shutdown.
    async fn run(&mut self) -> CertManagerResult<()> {
        let mut rx_certificate_fetcher = self.consensus_bus.certificate_fetcher().subscribe();

        loop {
            tokio::select! {
                Some(command) = rx_certificate_fetcher.recv() => {
                    debug!(target: "primary::cert_fetcher", ?command, "received next command");
                    self.handle_command(command)?;
                },

                result = &mut self.fetch_task => {
                    self.handle_fetch_completion(result)?;
                },

                _ = &self.rx_shutdown => {
                    break;
                }
            }
        }

        Ok(())
    }

    /// Process a certificate fetcher command and potentially start a new fetch task.
    fn handle_command(&mut self, command: CertificateFetcherCommand) -> CertManagerResult<()> {
        let certificate = match command {
            CertificateFetcherCommand::Ancestors(certificate) => certificate,
            CertificateFetcherCommand::Kick => {
                // start fetch if no task is running and there are pending targets
                debug!(target: "primary::cert_fetcher", fetch_task_empty=self.fetch_task.is_none(), empty_target=self.targets.is_empty(), "received kick command");
                if self.fetch_task.is_none() && !self.targets.is_empty() {
                    debug!(target: "primary::cert_fetcher", "spawning next fetch task");
                    self.start_fetch_task()?;
                }

                return Ok(());
            }
        };

        let header = &certificate.header();

        // skip certificates from wrong epoch
        if header.epoch() != self.committee.epoch() {
            warn!(
                target: "primary::cert_fetcher",
                requested_epoch=?header.epoch(),
                "ignoring request to fetch ancestor outside of current epoch"
            );

            return Ok(());
        }

        // skip if later certificate already received from this authority
        if !self.should_fetch_certificate(header)? {
            return Ok(());
        }

        // update targets and start fetching if needed
        self.targets.insert(header.author().clone(), header.round());

        // start fetching if available space
        if self.fetch_task.is_none() {
            self.start_fetch_task()?;
        }

        Ok(())
    }

    /// Check if we need to fetch ancestors for this certificate.
    fn should_fetch_certificate(&self, header: &Header) -> CertManagerResult<bool> {
        // skip if a later round is already being fetched from this authority
        if let Some(target_round) = self.targets.get(header.author()) {
            if header.round() <= *target_round {
                return Ok(false);
            }
        }

        // skip if this round or later is already in storage
        let header_is_newer = self
            .certificate_store
            .last_round_number(header.author())?
            .map(|last_round| header.round() > last_round)
            .unwrap_or(true); // true if missing

        Ok(header_is_newer)
    }

    /// Handle completion of a fetch task and start another if needed.
    fn handle_fetch_completion(&mut self, result: CertManagerResult<()>) -> CertManagerResult<()> {
        // log error
        if let Err(e) = result {
            error!(target: "primary::cert_fetcher", "Fetch task failed: {e}");
        }

        // NOTE: self.fetch_task.poll sets `Option` to `None` once result it received
        //
        // start next fetch if more targets
        if !self.targets.is_empty() {
            self.start_fetch_task()?;
        }

        Ok(())
    }

    /// Start a new task to fetch missing certificates.
    fn start_fetch_task(&mut self) -> CertManagerResult<()> {
        // update targets - remove any that are already satisfied
        let gc_round = self.gc_round();

        // prepare request based on latest certs in local storage
        let written_rounds = self.get_written_rounds(gc_round)?;

        // update targets - remove any that are already in storage
        self.update_targets(gc_round, &written_rounds);

        // nothing to fetch
        if self.targets.is_empty() {
            debug!(target: "primary::cert_fetcher", "Current targets empty - nothing to fetch");
            return Ok(());
        }

        // create the fetch request
        let request = match MissingCertificatesRequest::default()
            .set_bounds(gc_round, written_rounds)
            .map_err(|e| CertManagerError::RequestBounds(e.to_string()))
            .map(|r| r.set_max_response_size(self.max_rpc_message_size))
        {
            Ok(req) => req,
            Err(e) => {
                error!(target: "primary::cert_fetcher", ?e, "Failed to create missing cert request");
                return Ok(());
            }
        };

        // spawn the fetch task
        let network = self.network.clone();
        let committee = self.committee.clone();
        let state_sync = self.state_sync.clone();
        let metrics = self.metrics.clone();
        let task_spawner = self.task_spawner.clone();
        let authority_id = self.authority_id.clone();
        let fallback_delay = self.parallel_fetch_request_delay_interval;
        let (tx, rx) = oneshot::channel();

        // store receiver for polling in `Self::run`
        self.fetch_task.set_task(rx);
        debug!(target: "primary::cert_fetcher", ?gc_round, ?request, "spawning fetch task");

        // spawn task and hold receiver
        self.task_spawner.spawn_task(
            format!("fetch-certs-{}", request.exclusive_lower_bound),
            monitored_future!(async move {
                let _scope = monitored_scope("CertificatesFetching");
                metrics.certificate_fetcher_inflight_fetch.inc();

                let result = fetch_and_process_certificates(
                    authority_id,
                    network,
                    committee,
                    request,
                    state_sync,
                    task_spawner,
                    fallback_delay,
                )
                .await;

                let _ = tx.send(result);
                metrics.certificate_fetcher_inflight_fetch.dec();
            }),
        );

        Ok(())
    }

    /// Update targets by removing any that have already been satisfied.
    fn update_targets(
        &mut self,
        gc_round: Round,
        written_rounds: &BTreeMap<AuthorityIdentifier, BTreeSet<Round>>,
    ) {
        self.targets.retain(|origin, target_round| {
            let last_written_round = written_rounds
                .get(origin)
                .and_then(|rounds| rounds.last())
                .copied()
                .unwrap_or(gc_round);

            // Drop sync target when cert store already has an equal or higher round for the origin.
            // This applies GC to targets as well.
            //
            // NOTE: even if the store actually does not have target_round for the origin,
            // it is ok to stop fetching without this certificate.
            //
            // If this certificate becomes a parent of other certificates, another
            // fetch will be triggered eventually because of missing certificates.
            last_written_round < *target_round
        });
    }

    /// Get the current set of written rounds in storage for each authority.
    fn get_written_rounds(
        &self,
        gc_round: Round,
    ) -> CertManagerResult<BTreeMap<AuthorityIdentifier, BTreeSet<Round>>> {
        let mut written_rounds = BTreeMap::new();

        // initialize for all authorities
        for authority in self.committee.authorities() {
            written_rounds.insert(authority.id(), BTreeSet::new());
        }

        // populate with actual written rounds
        // NOTE: origins_after_round() is inclusive.
        let origins = self.certificate_store.origins_after_round(gc_round + 1)?;

        for (round, origins_at_round) in origins {
            for origin in origins_at_round {
                written_rounds.entry(origin).or_default().insert(round);
            }
        }

        Ok(written_rounds)
    }

    /// Read latest gc round from consensus bus watch channel.
    fn gc_round(&self) -> Round {
        gc_round(
            self.consensus_bus.committed_round(),
            self.config.parameters().gc_depth,
        )
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
    fallback_delay: Duration,
) -> CertManagerResult<()> {
    // get randomized list of peers
    let mut peers: Vec<_> = committee
        .others_primaries_by_id(authority_id.as_ref())
        .into_iter()
        .map(|(_, key)| key)
        .collect();
    peers.shuffle(&mut ThreadRng::default());

    // calculate timeout based on number of peers + twice the fallback delay
    // NOTE: fallback_delay is set to 5s by default and should be plenty of time
    let timeout_duration = fallback_delay * peers.len() as u32 + fallback_delay * 2;

    // try fetching from peers with staggered parallel requests
    let certificates = timeout(
        timeout_duration,
        fetch_from_peers(network, peers, request, task_spawner, fallback_delay),
    )
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
    fallback_delay: Duration,
) -> CertManagerResult<Vec<Certificate>> {
    let (tx_results, mut rx_results) = tokio::sync::mpsc::unbounded_channel();
    let cancel_signal = Arc::new(Notifier::new());

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
    let mut spawn_timer = Box::pin(sleep(fallback_delay));

    loop {
        tokio::select! {
            Some(result) = rx_results.recv() => {
                if let Ok(certificates) = result {
                    if !certificates.is_empty() {
                        debug!(target: "primary::cert_fetcher",
                            "Fetched {} certificates", certificates.len());
                        // all certs should have valid sigs, otherwise treat entire response as malicious
                        let certificates = certificates.into_iter().map(|cert| {
                            // set cert as `unverified` and reject genesis certificates
                            // cert signature is verified later
                            validate_fetched_certificate(cert).map_err(|e| {
                                error!(
                                    target: "primary::cert_fetcher",
                                    peer=?peers[peer_index],
                                    "cert fetch received invalid genesis cert from peer"
                                );
                                e.into()
                            })
                        }).collect::<CertManagerResult<Vec<_>>>();

                        // success - cancel all other fetch tasks
                        if certificates.is_ok() {
                            // only cancel on success - use timeout as fallback
                            cancel_signal.notify();
                            return certificates;
                        }
                    }

                    // empty response - continue with other peers
                    debug!(target: "primary::cert_fetcher",
                        "Received empty certificate response, continuing with other peers");
                }

                // check if all peers have been tried and all tasks completed
                if peer_index >= peers.len() && tx_results.is_closed() {
                    debug!(target: "primary::cert_fetcher",
                        "All peers tried without success");
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
                spawn_timer = Box::pin(sleep(fallback_delay));
            }

            else => {
                // all peers spawned, no results yet - wait for tasks to complete
                if tx_results.is_closed() {
                    debug!(target: "primary::cert_fetcher",
                        "No peer could provide certificates");
                    sleep(fallback_delay).await;
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
                // cancelled - another peer succeeded
            }
        }
    });
}

/// A future wrapper for managing at most one pending certificate fetch task.
///
/// This struct implements `Future` to enable polling in `tokio::select!` without
/// requiring complex Option handling. When no task is present, it returns `Poll::Pending`
/// indefinitely, allowing the select! to skip this branch. When a task completes,
/// it automatically clears the internal receiver and returns the result.
struct FetchTask {
    /// The receiver for a fetch task. Optional if a task has spawned to fetch certificates from
    /// peers.
    fetch_task: Option<oneshot::Receiver<CertManagerResult<()>>>,
}

impl FetchTask {
    /// Creates a new `FetchTask` with no pending operation.
    ///
    /// The future will return `Poll::Pending` until a task is set via `set_task`.
    fn new() -> Self {
        Self { fetch_task: None }
    }

    /// Sets a new fetch task, replacing any existing incomplete task.
    ///
    /// If a previous task was pending, it will be dropped and its sender will
    /// receive a cancellation error. This ensures only one fetch operation
    /// runs at a time.
    fn set_task(&mut self, receiver: oneshot::Receiver<CertManagerResult<()>>) {
        self.fetch_task = Some(receiver);
    }

    /// Checks whether a fetch task is currently pending.
    ///
    /// Returns `false` if a task is set and hasn't completed yet, `true` if there is no pending
    /// task. Used to prevent spawning duplicate fetch operations.
    fn is_none(&self) -> bool {
        self.fetch_task.is_none()
    }
}

impl Future for FetchTask {
    type Output = CertManagerResult<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.fetch_task {
            Some(rx) => {
                // poll the inner receiver
                match Pin::new(rx).poll(cx) {
                    Poll::Ready(Ok(result)) => {
                        // clear the receiver since it's consumed
                        self.fetch_task = None;
                        Poll::Ready(result)
                    }
                    Poll::Ready(Err(_)) => {
                        // sender was dropped
                        self.fetch_task = None;
                        Poll::Ready(Err(CertManagerError::ChannelClosed))
                    }
                    Poll::Pending => Poll::Pending,
                }
            }
            None => {
                // no task to poll - return pending
                Poll::Pending
            }
        }
    }
}
