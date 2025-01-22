//! Synchronize data between peers and workers

use crate::{
    aggregators::CertificatesAggregator, certificate_fetcher::CertificateFetcherCommand,
    ConsensusBus,
};
use consensus_metrics::{
    metered_channel::{channel_with_total_sender, MeteredMpscChannel},
    monitored_future, monitored_scope, spawn_logged_monitored_task,
};
use fastcrypto::hash::Hash as _;
use futures::{stream::FuturesOrdered, StreamExt};
use itertools::Itertools;
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};
use tn_config::ConsensusConfig;
use tn_network::{PrimaryToWorkerClient, RetryConfig};
use tn_network_types::WorkerSynchronizeMessage;
use tn_storage::traits::Database;
use tn_types::{
    ensure,
    error::{
        AcceptNotification, CertificateError, CertificateResult, DagError, DagResult, HeaderError,
        HeaderResult,
    },
    Certificate, CertificateDigest, Committee, Header, Round, SignatureVerificationState,
    TaskManager, TnReceiver, TnSender, CHANNEL_CAPACITY,
};
use tn_utils::sync::notify_once::NotifyOnce;
use tokio::{
    sync::{oneshot, MutexGuard},
    task::{spawn_blocking, JoinSet},
    time::{timeout, Instant},
};
use tracing::{debug, error, instrument, trace, warn};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

/// Only try to accept or suspend a certificate, if it is within this limit above the
/// locally highest processed round.
/// Expected max memory usage with 100 nodes: 100 nodes * 1000 rounds * 3.3KB per certificate =
/// 330MB.
const NEW_CERTIFICATE_ROUND_LIMIT: Round = 1000;

struct Inner<DB> {
    /// Node config.
    consensus_config: ConsensusConfig<DB>,
    /// Highest round that has been GC'ed.
    gc_round: AtomicU32,
    /// Highest round of certificate accepted into the certificate store.
    highest_processed_round: AtomicU32,
    /// Highest round of verfied certificate that has been received.
    highest_received_round: AtomicU32,
    /// Send certificates to be accepted into a separate task that runs
    /// `process_certificates_with_lock()` in a loop.
    /// See comment above `process_certificates_with_lock()` for why this is necessary.
    tx_certificate_acceptor:
        MeteredMpscChannel<(Vec<Certificate>, oneshot::Sender<CertificateResult<()>>, bool)>,
    consensus_bus: ConsensusBus,
    /// Genesis digests and contents.
    genesis: HashMap<CertificateDigest, Certificate>,
    /// A background task that synchronizes batches. A tuple of a header and the maximum accepted
    /// age is sent over.
    tx_batch_tasks: MeteredMpscChannel<(Header, Round)>,
    /// Aggregates certificates to use as parents for new headers.
    certificates_aggregators: Mutex<BTreeMap<Round, Box<CertificatesAggregator>>>,
    /// State for tracking suspended certificates and when they can be accepted.
    state: tokio::sync::Mutex<State>,
}

impl<DB: Database> Inner<DB> {
    /// Checks if the certificate is valid and can potentially be accepted into the DAG.
    fn sanitize_certificate(&self, certificate: Certificate) -> CertificateResult<Certificate> {
        // Ok to drop old certificate, because it will never be included into the consensus dag.
        let gc_round = self.gc_round.load(Ordering::Acquire);
        ensure!(
            gc_round < certificate.round(),
            CertificateError::from(HeaderError::TooOld(certificate.round(), gc_round))
        );
        // Verify the certificate (and the embedded header).
        certificate.verify(self.consensus_config.committee(), self.consensus_config.worker_cache())
    }

    async fn append_certificate_in_aggregator(&self, certificate: Certificate) -> DagResult<()> {
        // Check if we have enough certificates to enter a new dag round and propose a header.
        let Some(parents) = self
            .certificates_aggregators
            .lock()
            .entry(certificate.round())
            .or_insert_with(|| Box::new(CertificatesAggregator::new()))
            .append(certificate.clone(), self.consensus_config.committee())
        else {
            return Ok(());
        };
        // Send it to the `Proposer`.
        self.consensus_bus
            .parents()
            .try_send((parents, certificate.round()))
            // .await
            .map_err(|_| DagError::ShuttingDown)
    }

    async fn accept_suspended_certificate(
        &self,
        lock: &MutexGuard<'_, State>,
        suspended: SuspendedCertificate,
    ) -> DagResult<()> {
        self.accept_certificate_internal(lock, suspended.certificate.clone()).await?;
        // Notify waiters that the certificate is no longer suspended.
        // Must be after certificate acceptance.
        // It is ok if there is no longer any waiter.
        suspended.notify.notify().expect("Suspended certificate should be notified once.");
        Ok(())
    }

    // State lock must be held when calling this function.
    #[instrument(level = "debug", skip_all)]
    async fn accept_certificate_internal(
        &self,
        _lock: &MutexGuard<'_, State>,
        certificate: Certificate,
    ) -> DagResult<()> {
        let _scope = monitored_scope("Synchronizer::accept_certificate_internal");

        debug!("Accepting certificate {:?}", certificate);

        let digest = certificate.digest();

        // Validate that certificates are accepted in causal order.
        // This should be relatively cheap because of certificate store caching.
        if certificate.round() > self.gc_round.load(Ordering::Acquire) + 1 {
            let existence = self
                .consensus_config
                .node_storage()
                .certificate_store
                .multi_contains(certificate.header().parents().iter())?;
            for (digest, exists) in certificate.header().parents().iter().zip(existence.iter()) {
                if !*exists {
                    panic!("Parent {digest:?} not found for {certificate:?}!")
                }
            }
        }

        if !matches!(
            certificate.signature_verification_state(),
            SignatureVerificationState::VerifiedDirectly(_)
                | SignatureVerificationState::VerifiedIndirectly(_)
                | SignatureVerificationState::Genesis
        ) {
            panic!(
                "Attempting to write cert {:?} with invalid signature state {:?} to store",
                certificate.digest(),
                certificate.signature_verification_state()
            );
        }

        // Store the certificate and make it available as parent to other certificates.
        self.consensus_config
            .node_storage()
            .certificate_store
            .write(certificate.clone())
            .expect("Writing certificate to storage cannot fail!");

        // From this point, the certificate must be sent to consensus or Narwhal needs to shutdown,
        // to avoid inconsistencies in certificate store and consensus dag.

        // Update metrics for accepted certificates.
        let highest_processed_round = self
            .highest_processed_round
            .fetch_max(certificate.round(), Ordering::AcqRel)
            .max(certificate.round());
        let certificate_source = if self.consensus_config.authority().id().eq(&certificate.origin())
        {
            "own"
        } else {
            "other"
        };
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .highest_processed_round
            .with_label_values(&[certificate_source])
            .set(highest_processed_round as i64);
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .certificates_processed
            .with_label_values(&[certificate_source])
            .inc();

        // Append the certificate to the aggregator of the
        // corresponding round.
        if let Err(e) = self.append_certificate_in_aggregator(certificate.clone()).await {
            warn!("Failed to aggregate certificate {} for header: {}", digest, e);
            return Err(DagError::ShuttingDown);
        }

        // Send the accepted certificate to the consensus layer.
        if let Err(e) = self.consensus_bus.new_certificates().send(certificate).await {
            warn!("Failed to deliver certificate {} to the consensus: {}", digest, e);
            return Err(DagError::ShuttingDown);
        }

        Ok(())
    }

    /// Returns parent digests that do not exist in storage nor suspended state.
    async fn get_unknown_parent_digests(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<CertificateDigest>> {
        let _scope = monitored_scope("Synchronizer::get_unknown_parent_digests");

        if header.round() == 1 {
            for digest in header.parents() {
                if !self.genesis.contains_key(digest) {
                    return Err(HeaderError::InvalidGenesisParent(*digest));
                }
            }
            return Ok(Vec::new());
        }

        let existence = self
            .consensus_config
            .node_storage()
            .certificate_store
            .multi_contains(header.parents().iter())?;
        let mut unknown: Vec<_> = header
            .parents()
            .iter()
            .zip(existence.iter())
            .filter_map(|(digest, exists)| if *exists { None } else { Some(*digest) })
            .collect();
        let state = self.state.lock().await;
        unknown.retain(|digest| !state.suspended.contains_key(digest));
        Ok(unknown)
    }

    /// Tries to get all missing parents of the certificate. If there is any, sends the
    /// certificate to `CertificateFetcher` which will trigger range fetching of missing
    /// certificates.
    async fn get_missing_parents(
        &self,
        certificate: &Certificate,
    ) -> CertificateResult<Vec<CertificateDigest>> {
        let _scope = monitored_scope("Synchronizer::get_missing_parents");

        let mut result = Vec::new();
        if certificate.round() == 1 {
            for digest in certificate.header().parents() {
                if !self.genesis.contains_key(digest) {
                    return Err(CertificateError::from(HeaderError::InvalidGenesisParent(*digest)));
                }
            }
            return Ok(result);
        }

        let existence = self
            .consensus_config
            .node_storage()
            .certificate_store
            .multi_contains(certificate.header().parents().iter())?;
        for (digest, exists) in certificate.header().parents().iter().zip(existence.iter()) {
            if !*exists {
                result.push(*digest);
            }
        }
        if !result.is_empty() {
            self.consensus_bus
                .certificate_fetcher()
                .send(CertificateFetcherCommand::Ancestors(certificate.clone()))
                .await
                .map_err(|_| CertificateError::TNSend)?;
        }
        Ok(result)
    }

    #[cfg(test)]
    async fn get_suspended_stats(&self) -> (usize, usize) {
        let state = self.state.lock().await;
        (state.num_suspended(), state.num_missing())
    }

    /// Long running task to perform garbage collection on DAG.
    /// This will run a task managed by the task manager.
    async fn garbage_collection(&self) {
        const FETCH_TRIGGER_TIMEOUT: Duration = Duration::from_secs(30);
        let mut rx_consensus_round_updates =
            self.consensus_bus.consensus_round_updates().subscribe();
        loop {
            let Ok(result) =
                timeout(FETCH_TRIGGER_TIMEOUT, rx_consensus_round_updates.changed()).await
            else {
                // When consensus commit has not happened for 30s, it is possible that no
                // new certificate is received by this primary or
                // created in the network, so fetching should
                // definitely be started. For other reasons of
                // timing out, there is no harm to start fetching either.
                if let Err(e) = self
                    .consensus_bus
                    .certificate_fetcher()
                    .send(CertificateFetcherCommand::Kick)
                    .await
                {
                    error!(target: "primary::synchronizer::gc", ?e, "failed to send on tx_certificate_fetcher");
                    return;
                }
                self.consensus_bus.primary_metrics().node_metrics.synchronizer_gc_timeout.inc();
                warn!(target: "primary::synchronizer::gc", "No consensus commit happened for {:?}, triggering certificate fetching.", FETCH_TRIGGER_TIMEOUT);
                continue;
            };

            if let Err(e) = result {
                error!(target: "primary::synchronizer::gc", ?e, "failed to received rx_consensus_round_updates - shutting down...");
                return;
            }

            let _scope = monitored_scope("Synchronizer::gc_iteration");
            let gc_round = rx_consensus_round_updates.borrow().gc_round;
            // this is the only task updating gc_round
            self.gc_round.store(gc_round, Ordering::Release);
            self.certificates_aggregators.lock().retain(|k, _| k > &gc_round);
            // Accept certificates at and below gc round, if there is any.
            let mut state = self.state.lock().await;
            while let Some(((round, digest), suspended_cert)) = state.run_gc_once(gc_round) {
                assert!(round <= gc_round, "Never gc certificates above gc_round as this can lead to missing causal history in DAG");

                let suspended_children_certs = state.accept_children(round, digest);
                // Acceptance must be in causal order.
                for suspended in
                    suspended_cert.into_iter().chain(suspended_children_certs.into_iter())
                {
                    match self.accept_suspended_certificate(&state, suspended).await {
                        Ok(()) => {
                            trace!(target: "primary::synchronizer::gc", "accepted suspended cert")
                        }
                        Err(DagError::ShuttingDown) => {
                            error!(target: "primary::synchronizer::gc", "error accepting suspended cert - shutting down...");
                            return;
                        }
                        Err(e) => {
                            error!(target: "primary::synchronizer::gc", ?e, "error accepting suspended cert - PANIC");
                            panic!("Unexpected error accepting certificate during GC! {e}")
                        }
                    }
                }
            }
        }
    }

    /// Long running task to accept incoming certificates.
    /// This will run a task managed by the task manager.
    async fn accept_certificates(&self) {
        let mut rx_certificate_acceptor = self.tx_certificate_acceptor.subscribe();
        loop {
            let Some((certificates, result_sender, early_suspend)) =
                rx_certificate_acceptor.recv().await
            else {
                error!(target: "primary::synchronizer::accept_certificates", "error on rx_certificate_acceptor.recv() - synchronizer is shutting down...");
                return;
            };

            // ensure cert sigs are verified
            for certificate in &certificates {
                assert!(
                    matches!(
                        certificate.signature_verification_state(),
                        SignatureVerificationState::VerifiedDirectly(_)
                        | SignatureVerificationState::VerifiedIndirectly(_)
                    ),
                    "Never accept certificates that have not been verified either directly or indirectly."
                );
            }

            // Ignore error if receiver has been dropped.
            let _ = result_sender
                .send(self.process_certificates_with_lock(certificates, early_suspend).await);
        }
    }

    /// Long running task to synchronize blocks.
    /// This will run a task managed by the task manager.
    /// Note that due to internal implementation details this has to recieve an
    /// Inner wrapped in an Arc vs &self.
    async fn synchronize_blocks(inner: Arc<Inner<DB>>) {
        let mut rx_batch_tasks = inner.tx_batch_tasks.subscribe();
        let mut batch_tasks: JoinSet<HeaderResult<()>> = JoinSet::new();

        loop {
            tokio::select! {
                result = rx_batch_tasks.recv() => {
                    let (header, max_age) = match result {
                        Some(r) => r,
                        None => {
                            // exit loop if the channel has been closed
                            break;
                        }
                    };

                    let inner_clone = inner.clone();
                    batch_tasks.spawn(async move {
                        inner_clone.sync_batches_internal(&header, max_age, true).await
                    });
                },
                Some(result) = batch_tasks.join_next() => {
                    if let Err(e) = result  {
                        error!(target: "primary::synchronizer::synchronize_blocks", ?e, "error when synchronizing blocks")
                    }
                }
            }
        }
    }

    /// This function checks if a certificate has all parents and can be accepted into storage.
    /// If yes, writing the certificate to storage and sending it to consensus need to happen
    /// atomically. Otherwise, there will be divergence between certificate storage and consensus
    /// DAG. A certificate that is sent to consensus must have all of its parents already in
    /// the consensus DAG.
    ///
    /// Because of the atomicity requirement, this function cannot be made cancellation safe.
    /// So it is run in a loop inside a separate task, connected to `Synchronizer` via a channel.
    #[instrument(level = "debug", skip_all)]
    async fn process_certificates_with_lock(
        &self,
        certificates: Vec<Certificate>,
        early_suspend: bool,
    ) -> CertificateResult<()> {
        let _scope = monitored_scope("Synchronizer::process_certificates_with_lock");

        // We re-check here in case we already have in pipeline the same certificate for processing
        // more that once.
        let digests = certificates.iter().map(|c| c.digest()).collect_vec();
        let exists = self
            .consensus_config
            .node_storage()
            .certificate_store
            .multi_contains(digests.iter())?;
        let certificates = certificates
            .into_iter()
            .zip(exists.into_iter())
            .filter_map(|(c, exist)| {
                if exist {
                    debug!("Skip processing certificate {:?}", c);
                    self.consensus_bus
                        .primary_metrics()
                        .node_metrics
                        .duplicate_certificates_processed
                        .inc();
                    return None;
                }
                Some(c)
            })
            .collect_vec();
        if certificates.is_empty() {
            return Ok(());
        }

        // The state lock must be held for the rest of the function, to ensure updating state,
        // writing certificates into storage and sending certificates to consensus are atomic.
        // The atomicity makes sure the internal state is consistent with DAG in certificate store,
        // and certificates are sent to consensus in causal order.
        // It is possible to reduce the critical section below, but it seems unnecessary for now.
        let mut state = self.state.lock().await;

        // Returns Ok(()) if all input certificates are accepted, or a suspended error for the last
        // suspended certificate. It is ok to not return all suspended errors for suspended
        // certificates, because the accept notification is only used when trying to accept a
        // single certificate.
        //
        // TODO: simplify the logic here by separating try_accept_certificate() and notify_accept().
        let mut result = Ok(());

        for certificate in certificates {
            debug!(target: "primary::synchronizer", ?certificate, "processing certificate with lock");
            let digest = certificate.digest();

            // Ensure parents are checked if !early_suspend.
            // See comments above `try_accept_fetched_certificate()` for details.
            if early_suspend {
                // Re-check if the certificate has been suspended, which can happen before the lock
                // is acquired.
                if let Some(notify) = state.check_suspended(&digest) {
                    trace!(target: "primary::synchronizer", "Certificate {digest:?} is still suspended. Skip processing.");
                    self.consensus_bus
                        .primary_metrics()
                        .node_metrics
                        .certificates_suspended
                        .with_label_values(&["dedup_locked"])
                        .inc();
                    result = Err(CertificateError::Suspended);
                    continue;
                }
            }

            // Ensure either we have all the ancestors of this certificate, or the parents have been
            // garbage collected. If we don't, the synchronizer will start fetching
            // missing certificates.
            if certificate.round() > self.gc_round.load(Ordering::Acquire) + 1 {
                let missing_parents = self.get_missing_parents(&certificate).await?;
                if !missing_parents.is_empty() {
                    debug!("Processing certificate {:?} suspended: missing ancestors", certificate);
                    self.consensus_bus
                        .primary_metrics()
                        .node_metrics
                        .certificates_suspended
                        .with_label_values(&["missing_parents"])
                        .inc();
                    // There is no upper round limit to suspended certificates. Currently there is
                    // no memory usage issue and this will speed up catching up.
                    // But we can revisit later.
                    let notify = state.insert(certificate, missing_parents, !early_suspend);
                    self.consensus_bus
                        .primary_metrics()
                        .node_metrics
                        .certificates_currently_suspended
                        .set(state.num_suspended() as i64);
                    result = Err(CertificateError::Suspended);
                    continue;
                }
            }

            let suspended_certs = state.accept_children(certificate.round(), certificate.digest());
            // Accept in causal order.
            self.accept_certificate_internal(&state, certificate)
                .await
                .map_err(|e| CertificateError::DeleteThisOne(e.to_string()))?;
            for suspended in suspended_certs {
                self.accept_suspended_certificate(&state, suspended)
                    .await
                    .map_err(|e| CertificateError::DeleteThisOne(e.to_string()))?;
            }
        }

        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .certificates_currently_suspended
            .set(state.num_suspended() as i64);

        result
    }

    /// This method is called:
    /// - when peer requests to vote
    /// - long running task to sync blocks
    async fn sync_batches_internal(
        &self,
        header: &Header,
        max_age: Round,
        is_certified: bool,
    ) -> HeaderResult<()> {
        let authority_id = self.consensus_config.authority().id();

        // TODO: this is already checked during vote,
        // but what about long running task to sync blocks?
        if header.author() == authority_id {
            debug!(target: "primary::synchronizer", "skipping sync_batches for header - no need to sync payload from own workers");
            return Ok(());
        }

        // Clone the round updates channel so we can get update notifications specific to
        // this RPC handler.
        let mut rx_consensus_round_updates =
            self.consensus_bus.consensus_round_updates().subscribe();
        let mut consensus_round = rx_consensus_round_updates.borrow().committed_round;
        ensure!(
            header.round() >= consensus_round.saturating_sub(max_age),
            HeaderError::TooOld(header.round(), consensus_round.saturating_sub(max_age))
        );

        let mut missing = HashMap::new();
        for (digest, (worker_id, _)) in header.payload().iter() {
            // Check whether we have the batch. If one of our worker has the batch, the primary
            // stores the pair (digest, worker_id) in its own storage. It is important
            // to verify that we received the batch from the correct worker id to
            // prevent the following attack:
            //      1. A Bad node sends a batch X to 2f good nodes through their worker #0.
            //      2. The bad node proposes a malformed block containing the batch X and claiming
            //         it comes from worker #1.
            //      3. The 2f good nodes do not need to sync and thus don't notice that the header
            //         is malformed. The bad node together with the 2f good nodes thus certify a
            //         block containing the batch X.
            //      4. The last good node will never be able to sync as it will keep sending its
            //         sync requests to workers #1 (rather than workers #0). Also, clients will
            //         never be able to retrieve batch X as they will be querying worker #1.
            if !self.consensus_config.node_storage().payload_store.contains(*digest, *worker_id)? {
                missing.entry(*worker_id).or_insert_with(Vec::new).push(*digest);
            }
        }

        // Build Synchronize requests to workers.
        let mut synchronize_handles = Vec::new();
        for (worker_id, digests) in missing {
            let worker_name = self
                .consensus_config
                .worker_cache()
                .worker(
                    self.consensus_config
                        .committee()
                        .authority(&authority_id)
                        .unwrap()
                        .protocol_key(),
                    &worker_id,
                )
                .expect("Author of valid header is not in the worker cache")
                .name;
            let client = self.consensus_config.local_network().clone();
            let retry_config = RetryConfig::default(); // 30s timeout
            let handle = retry_config.retry(move || {
                let digests = digests.clone();
                let message = WorkerSynchronizeMessage {
                    digests: digests.clone(),
                    target: header.author(),
                    is_certified,
                };
                let client = client.clone();
                let worker_name = worker_name.clone();
                async move {
                    let result = client.synchronize(worker_name, message).await.map_err(|e| {
                        backoff::Error::transient(DagError::NetworkError(format!("{e:?}")))
                    });
                    if result.is_ok() {
                        for digest in &digests {
                            self.consensus_config
                                .node_storage()
                                .payload_store
                                .write(digest, &worker_id)
                                .map_err(|e| backoff::Error::permanent(DagError::StoreError(e)))?
                        }
                    }
                    result
                }
            });
            synchronize_handles.push(handle);
        }

        // Wait until results are back, or this request gets too old to continue.
        let mut wait_synchronize = futures::future::try_join_all(synchronize_handles);
        loop {
            tokio::select! {
                results = &mut wait_synchronize => {
                    break results
                        .map(|_| ())
                        .map_err(|e| HeaderError::SyncBatches(format!("error synchronizing batches: {e:?}")))
                },
                // This aborts based on consensus round and not narwhal round. When this function
                // is used as part of handling vote requests, this may cause us to wait a bit
                // longer than needed to give up on synchronizing batches for headers that are
                // too old to receive a vote. This shouldn't be a big deal (the requester can
                // always abort their request at any point too), however if the extra resources
                // used to attempt to synchronize batches for longer than strictly needed become
                // problematic, this function could be augmented to also support cancellation based
                // on primary round.
                Ok(()) = rx_consensus_round_updates.changed() => {
                    consensus_round = rx_consensus_round_updates.borrow().committed_round;
                    ensure!(
                        header.round() >= consensus_round.saturating_sub(max_age),
                        HeaderError::TooOld(
                            header.round(),
                            consensus_round.saturating_sub(max_age),
                        )
                    );
                },
            }
        }
    }
}

/// `Synchronizer` helps this primary and other peers stay in sync with each other,
/// w.r.t. certificates and the DAG. Specifically, it is responsible for
/// - Validating and accepting certificates received from peers.
/// - Triggering fetching for certificates and batches.
/// - Broadcasting created certificates.
///
/// `Synchronizer` contains most of the certificate processing logic in Narwhal.
#[derive(Clone)]
pub struct Synchronizer<DB> {
    /// Internal data that are thread safe.
    inner: Arc<Inner<DB>>,
}

impl<DB: Database> Synchronizer<DB> {
    pub fn new(consensus_config: ConsensusConfig<DB>, consensus_bus: &ConsensusBus) -> Self {
        let metrics = consensus_bus.primary_metrics();
        let primary_channel_metrics = &metrics.primary_channel_metrics;
        let committee: &Committee = consensus_config.committee();
        let genesis = Self::make_genesis(committee);
        let node_store = consensus_config.node_storage();
        let highest_processed_round = node_store.certificate_store.highest_round_number();
        let gc_round = consensus_bus.consensus_round_updates().borrow().gc_round;
        let tx_certificate_acceptor = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &primary_channel_metrics.tx_certificate_acceptor,
            &primary_channel_metrics.tx_certificate_acceptor_total,
        );

        let tx_batch_tasks = channel_with_total_sender(
            CHANNEL_CAPACITY,
            &primary_channel_metrics.tx_batch_tasks,
            &primary_channel_metrics.tx_batch_tasks_total,
        );

        let inner = Arc::new(Inner {
            consensus_config,
            gc_round: AtomicU32::new(gc_round),
            highest_processed_round: AtomicU32::new(highest_processed_round),
            highest_received_round: AtomicU32::new(0),
            tx_certificate_acceptor,
            consensus_bus: consensus_bus.clone(),
            genesis,
            tx_batch_tasks,
            certificates_aggregators: Mutex::new(BTreeMap::new()),
            state: tokio::sync::Mutex::new(State::default()),
        });

        Self { inner }
    }

    pub fn spawn(&self, task_manager: &TaskManager) {
        // Start a task to recover parent certificates for proposer.
        // This is a short running task not managed by the task_manager.
        let inner_proposer = self.inner.clone();
        spawn_logged_monitored_task!(
            async move {
                let last_round_certificates = inner_proposer
                    .consensus_config
                    .node_storage()
                    .certificate_store
                    .last_two_rounds_certs()
                    .expect("Failed recovering certificates in primary core");
                for certificate in last_round_certificates {
                    if let Err(e) =
                        inner_proposer.append_certificate_in_aggregator(certificate).await
                    {
                        debug!(
                            target: "primary::synchronizer",
                            ?e,
                            "failed to recover certificate for aggregator - shutting down"
                        );
                        return;
                    }
                }
            },
            "Synchronizer::RecoverCertificates"
        );

        // Start a task to update gc_round, gc in-memory data, and trigger certificate catchup
        // if no gc / consensus commit happened for 30s.
        let inner = self.inner.clone();
        let rx_shutdown = self.inner.consensus_config.shutdown().subscribe();
        task_manager.spawn_task(
            "synchronizer GC",
            monitored_future!(
                async move {
                    tokio::select! {
                        _ = inner.garbage_collection() => {}  // This task should run for life of program.
                        _ = &rx_shutdown => {}  // App is shutting down.

                    }
                },
                "Synchronizer::GarbageCollection"
            ),
        );

        // Start a task to accept certificates. See comment above `process_certificates_with_lock()`
        // for why this task is needed.
        let inner = self.inner.clone();
        let rx_shutdown = self.inner.consensus_config.shutdown().subscribe();
        task_manager.spawn_task(
            "synchronizer accept certificates",
            monitored_future!(
                async move {
                    tokio::select! {
                        _ = inner.accept_certificates() => {}  // This task should run for life of program.
                        _ = &rx_shutdown => {}  // App is shutting down.
                    }
                },
                "Synchronizer::AcceptCertificates"
            ),
        );

        // Start a task to async download batches if needed
        let inner = self.inner.clone();
        let rx_shutdown = self.inner.consensus_config.shutdown().subscribe();
        task_manager.spawn_task(
            "synchronize blocks",
            monitored_future!(
                async move {
                    tokio::select! {
                        _ = Inner::synchronize_blocks(inner) => {}  // This task should run for life of program.
                        _ = &rx_shutdown => {}  // App is shutting down.
                    }
                },
                "Synchronizer::SyncrhonizeBlocks"
            ),
        );
    }

    // NOTE: this is called when primary receives:
    // - vote request
    // - gossip certificate
    /// Validates the certificate and accepts it into the DAG, if the certificate can be verified
    /// and has all parents in the certificate store. Otherwise an error is returned.
    /// If the certificate has missing parents and cannot be accepted immediately, the error would
    /// contain a value that can be awaited on, for signaling when the certificate is accepted.
    pub async fn try_accept_certificate(&self, certificate: Certificate) -> CertificateResult<()> {
        let _scope = monitored_scope("Synchronizer::try_accept_certificate");
        self.process_certificate_internal(certificate, true).await
    }

    /// Tries to accept a batch of certificates from certificate fetcher.
    ///
    /// The implementation takes advantage of input certificates being in a batch and
    /// likely topologically sorted, to improve processing efficiency.
    ///
    /// NOTE: when making changes to this function, check if the same change needs to be made to
    /// process_certificate_internal() which is non-batched.
    pub async fn try_accept_fetched_certificates(
        &self,
        certificates: Vec<Certificate>,
    ) -> DagResult<()> {
        if certificates.is_empty() {
            return Ok(());
        }

        let _scope = monitored_scope("Synchronizer::try_accept_fetched_certificates");

        let certificates = self.sanitize_fetched_certificates(certificates).await?;

        let highest_round = certificates.iter().map(|c| c.round()).max().unwrap();
        let certificate_source = "other";
        let highest_received_round = self
            .inner
            .highest_received_round
            .fetch_max(highest_round, Ordering::AcqRel)
            .max(highest_round);
        self.inner
            .consensus_bus
            .primary_metrics()
            .node_metrics
            .highest_received_round
            .with_label_values(&[certificate_source])
            .set(highest_received_round as i64);

        // Let the proposer draw early conclusions from a certificate at this round and epoch,
        // without its parents or payload (which we may not have yet).
        //
        // Since our certificate is well-signed, it shows a majority of honest signers stand at
        // round r, so to make a successful proposal, our proposer must use parents at least
        // at round r-1.
        //
        // This allows the proposer not to fire proposals at rounds strictly below the certificate
        // we witnessed.
        let minimal_round_for_parents = highest_received_round.saturating_sub(1);
        self.inner
            .consensus_bus
            .parents()
            .send((vec![], minimal_round_for_parents))
            .await
            .map_err(|_| DagError::ShuttingDown)?;

        // Try to accept the verified certificates.
        let _accept_scope =
            monitored_scope("Synchronizer::try_accept_fetched_certificates::accept");

        let max_age = self.inner.consensus_config.parameters().gc_depth.saturating_sub(1);
        let highest_processed_round = self.inner.highest_processed_round.load(Ordering::Acquire);
        for certificate in &certificates {
            // Instruct workers to download any missing batches referenced in this certificate.
            // Since this header got certified, we are sure that all the data it refers to (ie. its
            // batches and its parents) are available. We can thus continue the
            // processing of the certificate without blocking on batch synchronization.
            let header = certificate.header().clone();
            self.inner
                .tx_batch_tasks
                .send((header.clone(), max_age))
                .await
                .map_err(|_| DagError::ShuttingDown)?;

            if highest_processed_round + NEW_CERTIFICATE_ROUND_LIMIT < certificate.round() {
                self.inner
                    .consensus_bus
                    .certificate_fetcher()
                    .send(CertificateFetcherCommand::Ancestors(certificate.clone()))
                    .await
                    .map_err(|_| DagError::ShuttingDown)?;
                return Err(DagError::TooNew(
                    certificate.digest().into(),
                    certificate.round(),
                    highest_processed_round,
                ));
            }
        }

        let (sender, receiver) = oneshot::channel();
        self.inner
            .tx_certificate_acceptor
            .send((certificates, sender, false))
            .await
            .expect("Synchronizer should shut down before certificate acceptor task.");
        receiver.await.expect("Synchronizer should shut down before certificate acceptor task.")?;

        Ok(())
    }

    /// Accepts a certificate produced by this primary. This is not expected to fail unless
    /// the primary is shutting down.
    pub async fn accept_own_certificate(&self, certificate: Certificate) -> DagResult<()> {
        let authority = self.inner.consensus_config.authority().id();
        // Process the new certificate.
        match self.process_certificate_internal(certificate.clone(), false).await {
            Ok(_) => {
                trace!(target: "primary::synchronizer", ?authority, ?certificate, "successfully processed certificate")
            }
            // result @ Err(DagError::Certificate(_)) => {
            //     error!(target: "primary::synchronizer", ?authority, ?certificate, ?result,
            // "failed to process certificate internally - shutting down...");
            //     return Err(DagError::ShuttingDown);
            // }
            // TODO: double check this
            Err(e) => {
                error!(target: "primary::synchronizer", ?authority, ?certificate, "failed to process certificate internally - PANIC");
                return Err(DagError::ShuttingDown);
            }
        };

        // Update metrics.
        let round = certificate.round();
        let header_to_certificate_duration =
            Duration::from_millis(certificate.created_at() - *certificate.header().created_at())
                .as_secs_f64();
        self.inner
            .consensus_bus
            .primary_metrics()
            .node_metrics
            .certificate_created_round
            .set(round as i64);
        self.inner.consensus_bus.primary_metrics().node_metrics.certificates_created.inc();
        self.inner
            .consensus_bus
            .primary_metrics()
            .node_metrics
            .header_to_certificate_latency
            .observe(header_to_certificate_duration);

        // NOTE: This log entry is used to compute performance.
        debug!(
            target: "primary::synchronizer",
            "Header {:?} at round {} with {} batches, took {} seconds to be materialized to a certificate {:?}",
            certificate.header().digest(),
            certificate.header().round(),
            certificate.header().payload().len(),
            header_to_certificate_duration,
            certificate.digest()
        );

        Ok(())
    }

    fn make_genesis(committee: &Committee) -> HashMap<CertificateDigest, Certificate> {
        Certificate::genesis(committee).into_iter().map(|x| (x.digest(), x)).collect()
    }

    /// Checks if the certificate is valid and can potentially be accepted into the DAG.
    fn sanitize_certificate(&self, certificate: Certificate) -> CertificateResult<Certificate> {
        self.inner.sanitize_certificate(certificate)
    }

    async fn sanitize_fetched_certificates(
        &self,
        mut certificates: Vec<Certificate>,
    ) -> DagResult<Vec<Certificate>> {
        // Number of certificates to verify in a batch. Verifications in each batch run serially.
        // Batch size is chosen so that verifying a batch takes non-trival
        // time (verifying a batch of 50 certificates should take > 25ms).
        const VERIFY_CERTIFICATES_V2_BATCH_SIZE: usize = 50;
        // Number of rounds to force verfication of certificates by signature, to bound the maximum
        // number of certificates with bad signatures in storage.
        const CERTIFICATE_VERIFICATION_ROUND_INTERVAL: u32 = 50;

        let mut all_digests = HashSet::<CertificateDigest>::new();
        let mut all_parents = HashSet::<CertificateDigest>::new();
        for cert in &certificates {
            all_digests.insert(cert.digest());
            all_parents.extend(cert.header().parents().iter());
        }

        // Identify leaf certs and preemptively set the parent certificates
        // as verified indirectly. This is safe because any leaf certs that
        // fail verification will cancel processing for all fetched certs.
        let mut direct_verification_certs = Vec::new();
        for (idx, c) in certificates.iter_mut().enumerate() {
            if !all_parents.contains(&c.digest())
                || c.header().round() % CERTIFICATE_VERIFICATION_ROUND_INTERVAL == 0
            {
                direct_verification_certs.push((idx, c.clone()));
                continue;
            }
            // TODO: add dedicated Certificate API for VerifiedIndirectly.
            c.set_signature_verification_state(SignatureVerificationState::VerifiedIndirectly(
                c.aggregated_signature().ok_or(DagError::InvalidSignature)?.clone(),
            ));
        }

        // Start verify tasks only for certificates requiring direct verifications.
        let verify_tasks = direct_verification_certs
            .chunks(VERIFY_CERTIFICATES_V2_BATCH_SIZE)
            .map(|chunk| {
                let certs = chunk.to_vec();
                let inner = self.inner.clone();
                spawn_blocking(move || {
                    let now = Instant::now();
                    let mut sanitized_certs = Vec::new();
                    for (idx, c) in certs {
                        sanitized_certs.push((idx, inner.sanitize_certificate(c)?));
                    }
                    inner
                        .consensus_bus
                        .primary_metrics()
                        .node_metrics
                        .certificate_fetcher_total_verification_us
                        .inc_by(now.elapsed().as_micros() as u64);
                    Ok::<Vec<(usize, Certificate)>, DagError>(sanitized_certs)
                })
            })
            .collect_vec();

        // We ensure sanitization of certificates completes for all leaves
        // fetched certificates before accepting any certficates.
        for task in verify_tasks.into_iter() {
            // Any certificates that fail to be verified should cancel the entire
            // batch of fetched certficates.
            let idx_and_certs = task.await.map_err(|e| {
                error!(target: "primary::synchronizer", ?e, "verify task cancelled");
                DagError::Canceled
            })??;
            for (idx, cert) in idx_and_certs {
                certificates[idx] = cert;
            }
        }

        let certificates_count = certificates.len() as u64;
        let direct_verification_count = direct_verification_certs.len() as u64;
        self.inner
            .consensus_bus
            .primary_metrics()
            .node_metrics
            .fetched_certificates_verified_directly
            .inc_by(direct_verification_count);
        self.inner
            .consensus_bus
            .primary_metrics()
            .node_metrics
            .fetched_certificates_verified_indirectly
            .inc_by(certificates_count.saturating_sub(direct_verification_count));

        Ok(certificates)
    }

    /// Process a certificate that was received.
    ///
    /// The `external` argument indicates if the certificate was received from a external peer or
    /// created internally. Certificates received from peers should be fully verified. Certificates
    /// produced by this node are trusted.
    ///
    /// TODO:
    /// old NOTE: when making changes to this function, check if the same change needs to be made to
    /// try_accept_fetched_certificates() which is the batched version.
    async fn process_certificate_internal(
        &self,
        mut certificate: Certificate,
        external: bool,
    ) -> CertificateResult<()> {
        let _scope = monitored_scope("Synchronizer::process_certificate_internal");
        let digest = certificate.digest();
        if self.inner.consensus_config.node_storage().certificate_store.contains(&digest)? {
            trace!(target: "primary::synchronizer", "Certificate {digest:?} has already been processed. Skip processing.");
            self.inner
                .consensus_bus
                .primary_metrics()
                .node_metrics
                .duplicate_certificates_processed
                .inc();
            return Ok(());
        }

        // scrutinize certificates received from peers
        if external {
            if let Some(notify) = self.inner.state.lock().await.check_suspended(&digest) {
                trace!(target: "primary::synchronizer", ?digest, "certificate is still suspended - returning suspended error...");
                self.inner
                    .consensus_bus
                    .primary_metrics()
                    .node_metrics
                    .certificates_suspended
                    .with_label_values(&["dedup"])
                    .inc();
                return Err(CertificateError::Suspended);
            }

            certificate = self.sanitize_certificate(certificate)?;
        }

        debug!(target: "primary::synchronizer", round=certificate.round(), ?certificate, "processing certificate");

        let certificate_source =
            if self.inner.consensus_config.authority().id().eq(&certificate.origin()) {
                "own"
            } else {
                "other"
            };
        let highest_received_round = self
            .inner
            .highest_received_round
            .fetch_max(certificate.round(), Ordering::AcqRel)
            .max(certificate.round());
        self.inner
            .consensus_bus
            .primary_metrics()
            .node_metrics
            .highest_received_round
            .with_label_values(&[certificate_source])
            .set(highest_received_round as i64);

        // Let the proposer draw early conclusions from a certificate at this round and epoch,
        // without its parents or payload (which we may not have yet).
        //
        // Since our certificate is well-signed, it shows a majority of honest signers stand at
        // round r, so to make a successful proposal, our proposer must use parents at least
        // at round r-1.
        //
        // This allows the proposer not to fire proposals at rounds strictly below the certificate
        // we witnessed.
        let minimal_round_for_parents = certificate.round().saturating_sub(1);
        self.inner
            .consensus_bus
            .parents()
            .send((vec![], minimal_round_for_parents))
            .await
            .map_err(|_| CertificateError::TNSend)?;

        // Instruct workers to download any missing batches referenced in this certificate.
        // Since this header got certified, we are sure that all the data it refers to (ie. its
        // batches and its parents) are available. We can thus continue the processing of
        // the certificate without blocking on block synchronization.
        let header = certificate.header().clone();
        let max_age = self.inner.consensus_config.parameters().gc_depth.saturating_sub(1);
        self.inner
            .tx_batch_tasks
            .send((header.clone(), max_age))
            .await
            .map_err(|_| CertificateError::TNSend)?;

        let highest_processed_round = self.inner.highest_processed_round.load(Ordering::Acquire);
        if highest_processed_round + NEW_CERTIFICATE_ROUND_LIMIT < certificate.round() {
            self.inner
                .consensus_bus
                .certificate_fetcher()
                .send(CertificateFetcherCommand::Ancestors(certificate.clone()))
                .await
                .map_err(|_| CertificateError::TNSend)?;

            error!(target: "primary::synchronizer", "processed certificate that is too new");

            return Err(CertificateError::TooNew(certificate.round(), highest_processed_round));
        }

        let (sender, res) = oneshot::channel();
        self.inner
            .tx_certificate_acceptor
            .send((vec![certificate], sender, external))
            .await
            .map_err(|_| CertificateError::TNSend)?;

        res.await.map_err(|e| CertificateError::ResChannelClosed(e.to_string()))??;

        Ok(())
    }

    /// Synchronizes batches in the given header with other nodes (through our workers).
    /// Blocks until either synchronization is complete, or the current consensus rounds advances
    /// past the max allowed age. (`max_age == 0` means the header's round must match current
    /// round.)
    pub async fn sync_header_batches(&self, header: &Header, max_age: Round) -> HeaderResult<()> {
        self.inner.sync_batches_internal(header, max_age, false).await
    }

    // TODO: Add batching support to synchronizer and use this call from executor.
    // pub async fn sync_certificate_batches(
    //     &self,
    //     header: &Header,
    //     network: anemo::Network,
    //     max_age: Round,
    // ) -> DagResult<()> {
    //     Synchronizer::sync_batches_internal(self.inner.clone(), header, max_age, true)
    //         .await
    // }

    /// Returns the parent certificates of the given header, waits for availability if needed.
    pub async fn notify_read_parent_certificates(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<Certificate>> {
        let mut parents = Vec::new();
        if header.round() == 1 {
            for digest in header.parents() {
                match self.inner.genesis.get(digest) {
                    Some(certificate) => parents.push(certificate.clone()),
                    None => return Err(HeaderError::InvalidGenesisParent(*digest)),
                };
            }
        } else {
            let mut cert_notifications: FuturesOrdered<_> = header
                .parents()
                .iter()
                .map(|digest| {
                    self.inner
                        .consensus_config
                        .node_storage()
                        .certificate_store
                        .notify_read(*digest)
                })
                .collect();
            while let Some(result) = cert_notifications.next().await {
                parents.push(result?);
            }
        }

        Ok(parents)
    }

    /// Returns parent digests that do no exist either in storage or among suspended.
    pub async fn get_unknown_parent_digests(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<CertificateDigest>> {
        self.inner.get_unknown_parent_digests(header).await
    }

    /// Tries to get all missing parents of the certificate. If there is any, sends the
    /// certificate to `CertificateFetcher` which will trigger range fetching of missing
    /// certificates.
    #[cfg(test)]
    pub(crate) async fn get_missing_parents(
        &self,
        certificate: &Certificate,
    ) -> DagResult<Vec<CertificateDigest>> {
        self.inner.get_missing_parents(certificate).await.map_err(Into::into)
    }

    /// Returns the number of suspended certificates and missing certificates.
    #[cfg(test)]
    pub(crate) async fn get_suspended_stats(&self) -> (usize, usize) {
        self.inner.get_suspended_stats().await
    }
}

/// Holds information for a suspended certificate. The certificate can be accepted into the DAG
/// once `missing_parents` become empty.
struct SuspendedCertificate {
    certificate: Certificate,
    missing_parents: HashSet<CertificateDigest>,
    notify: AcceptNotification,
}

impl Drop for SuspendedCertificate {
    fn drop(&mut self) {
        // Make sure waiters are notified on shutdown.
        let _ = self.notify.notify();
    }
}

/// Keeps track of suspended certificates and their missing parents.
/// The digest keys in `suspended` and `missing` can overlap, but a digest can exist in one map
/// but not the other.
///
/// They can be combined into a single map, but it seems more complex to differentiate between
/// suspended certificates that is not a missing parent of another, from a missing parent without
/// the actual certificate.
///
/// Traversal of certificates that can be accepted should start from the missing map, i.e.
/// 1. If a certificate exists in `missing`, remove its entry.
/// 2. Find children of the certificate, update their missing parents.
/// 3. If a child certificate no longer has missing parent, traverse from it with step 1.
///
/// Synchronizer should access this struct via its methods, to avoid making inconsistent changes.
#[derive(Default)]
struct State {
    // Maps digests of suspended certificates to details including the certificate itself.
    suspended: HashMap<CertificateDigest, SuspendedCertificate>,
    // Maps digests of certificates that are not yet in the DAG, to digests of certificates that
    // include them as parents. Keys are prefixed by round number to allow GC.
    missing: BTreeMap<(Round, CertificateDigest), HashSet<CertificateDigest>>,
}

impl State {
    /// Checks if a digest is suspended. If it is, gets a notification for when it is accepted.
    fn check_suspended(&self, digest: &CertificateDigest) -> Option<AcceptNotification> {
        self.suspended.get(digest).map(|suspended_cert| suspended_cert.notify.clone())
    }

    /// Inserts a certificate with its missing parents into the suspended state.
    /// When `allow_reinsert` is false and the same certificate digest is inserted again,
    /// this function will panic. Otherwise, this function checks the missing parents of
    /// the certificate and verifies the same set is stored, before allowing a reinsertion.
    fn insert(
        &mut self,
        certificate: Certificate,
        missing_parents: Vec<CertificateDigest>,
        allow_reinsert: bool,
    ) -> AcceptNotification {
        let digest = certificate.digest();
        let missing_round = certificate.round() - 1;
        let missing_parents_map: HashSet<_> = missing_parents.iter().cloned().collect();
        if allow_reinsert {
            if let Some(suspended_cert) = self.suspended.get(&digest) {
                assert_eq!(
                    suspended_cert.missing_parents, missing_parents_map,
                    "Inconsistent missing parents! {:?} vs {:?}",
                    suspended_cert.missing_parents, missing_parents_map
                );
                return suspended_cert.notify.clone();
            }
        }
        let notify = Arc::new(NotifyOnce::new());
        assert!(self
            .suspended
            .insert(
                digest,
                SuspendedCertificate {
                    certificate,
                    missing_parents: missing_parents_map,
                    notify: notify.clone(),
                }
            )
            .is_none());
        for d in missing_parents {
            assert!(self.missing.entry((missing_round, d)).or_default().insert(digest));
        }
        notify
    }

    /// Examines children of a certificate that has been accepted, and returns the children that
    /// can be accepted as well.
    fn accept_children(
        &mut self,
        round: Round,
        digest: CertificateDigest,
    ) -> Vec<SuspendedCertificate> {
        // Validate that the parent certificate is no longer suspended.
        if let Some(suspended_cert) = self.suspended.remove(&digest) {
            error!(target: "primary::synchronizer", ?digest, "cert is still suspended, but should not have missing parents");
            panic!(
                "Certificate {:?} should have no missing parent, but is still suspended (missing parents {:?})",
                suspended_cert.certificate,
                suspended_cert.missing_parents
            )
        }
        let mut to_traverse = VecDeque::new();
        let mut to_accept = Vec::new();
        to_traverse.push_back((round, digest));
        while let Some((round, digest)) = to_traverse.pop_front() {
            let Some(child_digests) = self.missing.remove(&(round, digest)) else {
                continue;
            };
            for child in &child_digests {
                let suspended_child = self.suspended.get_mut(child).expect("Inconsistency found!");
                suspended_child.missing_parents.remove(&digest);
                if suspended_child.missing_parents.is_empty() {
                    let suspended_child = self.suspended.remove(child).unwrap();
                    to_traverse.push_back((
                        suspended_child.certificate.round(),
                        suspended_child.certificate.digest(),
                    ));
                    to_accept.push(suspended_child);
                }
            }
        }
        to_accept
    }

    /// Runs GC on the suspended certificates.
    /// Returns one certificate that can be GC'ed and accepted, or None.
    ///
    /// If (round, digest) is returned, it is the caller's responsibility to check if any
    /// of its children can also be accepted. If SuspendedCertificate is returned as well,
    /// it should be accepted before any of its children.
    fn run_gc_once(
        &mut self,
        gc_round: Round,
    ) -> Option<((Round, CertificateDigest), Option<SuspendedCertificate>)> {
        // Accept suspended certificates at and below gc round because their parents will not
        // be accepted into the DAG store anymore, in sanitize_certificate().
        let ((round, digest), _children) = self.missing.first_key_value()?;
        // Note that gc_round is the highest round where certificates are gc'ed, and which will
        // never be in a consensus commit. It's safe to gc up to gc_round, so anything suspended on
        // gc_round + 1 can safely be accepted as their parents (of gc_round) have already
        // been removed from the DAG. It would be dangerous to accept anything above
        // gc_round here as this could create inconsistencies in the DAG because we have to
        // make sure anything we accept above gc_round is able to get properly processed ,
        // stored and sent to the DAG.
        if *round > gc_round {
            return None;
        }

        let mut suspended = self.suspended.remove(digest);
        if let Some(suspended) = suspended.as_mut() {
            // Clear the missing_parents field to be consistent with other accepted
            // certificates.
            suspended.missing_parents.clear();
        }

        // The missing children info is needed for and will be cleared in accept_children() later.
        Some(((*round, *digest), suspended))
    }

    fn num_suspended(&self) -> usize {
        self.suspended.len()
    }

    #[cfg(test)]
    fn num_missing(&self) -> usize {
        self.missing.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::synchronizer::State;
    use fastcrypto::{hash::Hash, traits::KeyPair};
    use itertools::Itertools;
    use std::{collections::BTreeSet, num::NonZeroUsize};
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils::{make_optimal_signed_certificates, CommitteeFixture};
    use tn_types::{Certificate, Committee, Round};

    // Tests that gc_once is reporting back missing certificates up to gc_round and no further.
    #[tokio::test]
    async fn test_run_gc_once() {
        // GIVEN
        const NUM_AUTHORITIES: usize = 4;

        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .randomize_ports(true)
            .committee_size(NonZeroUsize::new(NUM_AUTHORITIES).unwrap())
            .build();

        let committee: Committee = fixture.committee();
        let genesis =
            Certificate::genesis(&committee).iter().map(|x| x.digest()).collect::<BTreeSet<_>>();
        let keys: Vec<_> = fixture.authorities().map(|a| (a.id(), a.keypair().copy())).collect();
        let (certificates, _next_parents) =
            make_optimal_signed_certificates(1..=3, &genesis, &committee, keys.as_slice());
        let certificates = certificates.into_iter().collect_vec();

        let mut state = State::default();

        // Insert all certificates of round 2, except the first certificate.
        // We report as missing the certificate of validator 1 from round 1.
        let round_1_validator_1 = certificates[0].digest();
        for cert in &certificates[NUM_AUTHORITIES + 1..NUM_AUTHORITIES * 2] {
            state.insert(cert.clone(), vec![round_1_validator_1], true);
        }

        // Insert all certificates of round 3. We report as missing the certificate of validator 1
        // from round 2.
        let round_2_validator_1 = certificates[NUM_AUTHORITIES].digest();
        for cert in &certificates[NUM_AUTHORITIES * 2..] {
            state.insert(cert.clone(), vec![round_2_validator_1], true);
        }

        // AND
        // Round 1 certificate of validator 1
        // Round 2 certificate of validator 1
        assert_eq!(state.num_missing(), 2);

        // 3 certificates of round 2,
        // 4 certificates of round 3
        assert_eq!(state.num_suspended(), 7);

        // WHEN running the gc for gc_round = 1, we expect to gc up to round = 1.
        const GC_ROUND: Round = 1;

        let ((round, certificate_digest), suspended_certificate) =
            state.run_gc_once(GC_ROUND).unwrap();

        assert_eq!(certificate_digest, certificates[0].digest()); // Ensure that only the missing certificate digest of round 1 gets garbage collected.
        assert_eq!(round, 1);
        assert!(suspended_certificate.is_none()); // We don't have its certificate

        // Accept its children
        let suspended_certificates = state.accept_children(round, certificate_digest);

        // 3 certificates of round 2 have been unsuspended
        assert_eq!(suspended_certificates.len(), 3);
        assert!(suspended_certificates.iter().all(|c| c.certificate.round() == 2));

        // WHEN trying to trigger again for gc_round 1, it should return None
        assert!(state.run_gc_once(GC_ROUND).is_none());

        // THEN
        assert_eq!(state.num_missing(), 1);
        assert_eq!(state.num_suspended(), 4);
    }

    #[tokio::test]
    async fn delete_my_atomic_test() {
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Task that updates the counter
        let counter_updater = std::sync::Arc::clone(&counter);
        tokio::spawn(async move {
            for _ in 0..10 {
                // Increment the counter
                counter_updater.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        });

        // Tasks that read the counter
        let readers: Vec<_> = (0..5)
            .map(|_| {
                let counter_reader = std::sync::Arc::clone(&counter);
                tokio::spawn(async move {
                    loop {
                        let value = counter_reader.load(std::sync::atomic::Ordering::SeqCst);
                        println!("Current value: {}", value);
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    }
                })
            })
            .collect();

        // Wait for the updater task to finish (in a real application, you might handle this
        // differently)
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }
}
