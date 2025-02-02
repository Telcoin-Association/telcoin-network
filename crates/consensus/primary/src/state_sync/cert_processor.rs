//! Main actor processing certificates.
//!
//! This module orchestrates all other certificate components, such as garbage collection, pending certs, and accepting new certificates.

use crate::{
    aggregators::CertificatesAggregatorManager,
    certificate_fetcher::CertificateFetcherCommand,
    error::{PrimaryNetworkError, PrimaryNetworkResult},
    network::MissingCertificatesRequest,
    state_sync::PendingCertCommand,
    ConsensusBus,
};
use consensus_metrics::monitored_scope;
use fastcrypto::hash::Hash as _;
use futures::{stream::FuturesOrdered, FutureExt};
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tn_config::ConsensusConfig;
use tn_network::RetryConfig;
use tn_network_types::WorkerSynchronizeMessage;
use tn_storage::{traits::Database, CertificateStore};
use tn_types::{
    error::{AcceptNotification, CertificateError, CertificateResult, HeaderError, HeaderResult},
    AuthorityIdentifier, Certificate, CertificateDigest, Header, Noticer, Round, TnReceiver,
    TnSender as _,
};
use tn_utils::sync::notify_once::NotifyOnce;
use tokio::{sync::oneshot, time::Instant};
use tokio_stream::StreamExt as _;
use tracing::{debug, error, trace, warn};

use super::AtomicRound;

/// Process new, unverified certificates.
#[derive(Debug, Clone)]
pub struct CertificateProcessor<DB> {
    /// Consensus channels.
    consensus_bus: ConsensusBus,
    /// The configuration for consensus.
    config: ConsensusConfig<DB>,
    /// Collection of parents to advance the round.
    parents: CertificatesAggregatorManager,
    /// Genesis digests and contents.
    genesis: HashMap<CertificateDigest, Certificate>,
    /// Highest garbage collection round.
    gc_round: AtomicRound,
    /// Highest round of certificate accepted into the certificate store.
    highest_processed_round: AtomicRound,
    /// Highest round of verfied certificate that has been received.
    highest_received_round: AtomicRound,
}

impl<DB> CertificateProcessor<DB>
where
    DB: Database,
{
    /// Create a new instance of Self.
    pub fn new(
        config: ConsensusConfig<DB>,
        consensus_bus: ConsensusBus,
        parents: CertificatesAggregatorManager,
        gc_round: AtomicRound,
        highest_processed_round: AtomicRound,
        highest_received_round: AtomicRound,
    ) -> Self {
        let genesis = Certificate::genesis(config.committee())
            .into_iter()
            .map(|cert| (cert.digest(), cert))
            .collect();

        Self {
            consensus_bus,
            config,
            parents,
            genesis,
            gc_round,
            highest_processed_round,
            highest_received_round,
        }
    }

    /// Returns the parent certificates of the given header, waits for availability if needed.
    pub async fn notify_read_parent_certificates(
        &self,
        header: &Header,
    ) -> HeaderResult<Vec<Certificate>> {
        let mut parents = Vec::new();
        if header.round() == 1 {
            for digest in header.parents() {
                match self.genesis.get(digest) {
                    Some(certificate) => parents.push(certificate.clone()),
                    None => return Err(HeaderError::InvalidGenesisParent(*digest)),
                };
            }
        } else {
            let mut cert_notifications: FuturesOrdered<_> = header
                .parents()
                .iter()
                .map(|digest| self.config.node_storage().certificate_store.notify_read(*digest))
                .collect();
            while let Some(result) = cert_notifications.next().await {
                parents.push(result?);
            }
        }

        Ok(parents)
    }

    /// Synchronize batches.
    pub async fn sync_batches(&self, header: &Header, is_certified: bool) -> HeaderResult<()> {
        let authority_id = self.config.authority().id();

        // TODO: this is already checked during vote,
        // but what about long running task to sync blocks?
        if header.author() == authority_id {
            debug!(target: "primary::synchronizer", "skipping sync_batches for header - no need to sync payload from own workers");
            return Ok(());
        }

        // Clone the round updates channel so we can get update notifications specific to
        // this RPC handler.
        let mut rx_committed_round_updates =
            self.consensus_bus.committed_round_updates().subscribe();
        let mut committed_round = *rx_committed_round_updates.borrow();
        if header.round() < committed_round {
            return Err(HeaderError::TooOld(header.digest(), header.round(), committed_round));
        }

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
            if !self.config.node_storage().payload_store.contains(*digest, *worker_id)? {
                missing.entry(*worker_id).or_insert_with(Vec::new).push(*digest);
            }
        }

        // Build Synchronize requests to workers.
        let mut synchronize_handles = Vec::new();
        for (worker_id, digests) in missing {
            let worker_name = self
                .config
                .worker_cache()
                .worker(
                    self.config.committee().authority(&authority_id).unwrap().protocol_key(),
                    &worker_id,
                )
                .expect("Author of valid header is not in the worker cache")
                .name;
            let client = self.config.local_network().clone();
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
                            self.config
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
                Ok(()) = rx_committed_round_updates.changed() => {
                    committed_round = *rx_committed_round_updates.borrow_and_update();

                    if header.round < committed_round {
                        return Err(HeaderError::TooOld(header.digest(), header.round(), committed_round));
                    }
                },
            }
        }
    }

    /// Filter parent digests that do not exist in storage or pending state.
    pub async fn get_unkown_parent_digests(
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

        let existence =
            self.config.node_storage().certificate_store.multi_contains(header.parents().iter())?;
        let unknown: Vec<_> = header
            .parents()
            .iter()
            .zip(existence.iter())
            .filter_map(|(digest, exists)| if *exists { None } else { Some(*digest) })
            .collect();
        let (reply, filtered) = oneshot::channel();
        self.consensus_bus
            .pending_cert_commands()
            .send(PendingCertCommand::FilterUnkownDigests { unknown: Box::new(unknown), reply })
            .await?;
        let unknown = filtered.await.map_err(|_| HeaderError::PendingCertificateOneshot)?;
        Ok(*unknown)
    }

    //
    //
    // === new cert intake
    //
    //

    /// Process certificate from this primary.
    async fn process_own_certificate(&self, certificate: Certificate) -> CertificateResult<()> {
        todo!()
    }

    /// Process a new, unverified certificate.
    ///
    /// Try to validate the certificate and add it to the DAG if there are no missing parents. If the certificate contains unkown parents, then return an error and try to fetch them.
    fn process_external_certificate(&mut self, certificate: Certificate) -> CertificateResult<()> {
        let _scope = monitored_scope("state-sync::try_accept_certificate");
        todo!()
    }

    /// Process the certificate.
    ///
    /// If the certificate is external, the certificate is validated and signatures are verified.
    /// The certificate's signature verification status is updated.
    async fn process_certificate(
        &mut self,
        mut certificate: Certificate,
        external: bool,
    ) -> CertificateResult<()> {
        let _scope = monitored_scope("state-sync::process_certificate");

        // see if certificate already processed
        let digest = certificate.digest();
        if self.config.node_storage().certificate_store.contains(&digest)? {
            trace!(target: "primary::state-sync", "Certificate {digest:?} has already been processed. Skip processing.");
            self.consensus_bus
                .primary_metrics()
                .node_metrics
                .duplicate_certificates_processed
                .inc();
            return Ok(());
        }

        // scrutinize certificates received from peers
        if external {
            // check pending status
            let pending = self.check_pending_status(digest).await?;

            if pending {
                trace!(target: "primary::state-sync", ?digest, "certificate is still suspended - returning suspended error...");

                self.consensus_bus
                    .primary_metrics()
                    .node_metrics
                    .certificates_suspended
                    .with_label_values(&["dedup"])
                    .inc();

                return Err(CertificateError::Pending(digest));
            }

            certificate = self.validate_certificate(certificate)?;
        }

        debug!(target: "primary::state-sync", round=certificate.round(), ?certificate, "processing certificate");

        let certificate_source =
            if self.config.authority().id().eq(&certificate.origin()) { "own" } else { "other" };
        let highest_received_round =
            self.highest_received_round.fetch_max(certificate.round()).max(certificate.round());
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .highest_received_round
            .with_label_values(&[certificate_source])
            .set(highest_received_round as i64);

        // A well-signed certificate from round r provides important information about network
        // progress, even before its contents are fully validated. The certificate's signatures
        // prove that a majority of honest validators have processed all certificates through
        // round r-1. This must be true because these validators could not have participated
        // in creating the round r certificate without first processing its parent rounds.
        //
        // This knowledge enables an important proposer optimization. Given evidence that the
        // network has progressed to round r, generating proposals with parents from rounds
        // earlier than r-1 becomes pointless. Such proposals would be outdated and unable
        // to achieve consensus. Skipping these older rounds prevents the creation of obsolete
        // proposals that the network would ignore.
        //
        // The optimization allows the proposer to stay synchronized with network progress
        // even while parent certificates and payload data are still being downloaded and
        // validated. It extracts actionable timing information from the certificate's
        // signatures alone, independent of the certificate's complete contents.
        let minimal_round_for_parents = certificate.round().saturating_sub(1);
        self.consensus_bus.parents().send((vec![], minimal_round_for_parents)).await?;

        // Instruct workers to download any missing batches referenced in this certificate.
        // Since this header got certified, we are sure that all the data it refers to (ie. its
        // batches and its parents) are available. We can thus continue the processing of
        // the certificate without blocking on block synchronization.
        let header = certificate.header().clone();
        let max_age = self.config.parameters().gc_depth.saturating_sub(1);
        self.consensus_bus.sync_missing_batches().send((header, max_age)).await?;

        // return error if certificate round is too far ahead
        let highest_processed_round = self.highest_processed_round.load();
        if highest_processed_round
            + self
                .config
                .network_config()
                .sync_config()
                .max_diff_between_external_cert_round_and_highest_local_round
            < certificate.round()
        {
            self.consensus_bus
                .certificate_fetcher()
                .send(CertificateFetcherCommand::Ancestors(certificate.clone()))
                .await?;

            error!(target: "primary::state-sync", "processed certificate that is too new");

            return Err(CertificateError::TooNew(
                certificate.digest(),
                certificate.round(),
                highest_processed_round,
            ));
        }

        // let (sender, res) = oneshot::channel();
        // self.tx_certificate_acceptor
        //     .send((vec![certificate], sender, external))
        //     .await
        //     ?;

        // res.await.map_err(|e| CertificateError::ResChannelClosed(e.to_string()))??;
        self.try_accept_verified_certificates(vec![certificate], external).await?;
        Ok(())
    }

    /// Validate the certificate.
    ///
    /// This method validates the certificate and verifies signatures.
    fn validate_certificate(&self, certificate: Certificate) -> CertificateResult<Certificate> {
        // certificates outside gc can never be included in the DAG
        let gc_round = self.gc_round.load();

        if certificate.round() < gc_round {
            return Err(CertificateError::TooOld(
                certificate.digest(),
                certificate.round(),
                gc_round,
            ));
        }

        // validate certificate and verify signatures
        certificate.verify(self.config.committee(), self.config.worker_cache())
    }

    /// Try to accept a certificate.
    ///
    /// TODO: synchronizer::process_certificates_with_lock
    async fn try_accept_verified_certificates(
        &mut self,
        certificates: Vec<Certificate>,
        external: bool,
    ) -> CertificateResult<()> {
        let _scope = monitored_scope("primary::state-sync::try_accept_verified_certificates");
        // ensure verification state is verified
        for cert in &certificates {
            if !cert.signature_verification_state().is_verified() {
                return Err(CertificateError::UnverifiedSignature(cert.digest()));
            }
        }

        // >>>>>>
        // TODO: is this check redundant??
        // <<<<<<<<<<<<<
        // see if certificate already processed
        let digests: Vec<_> = certificates.iter().map(|cert| cert.digest()).collect();
        let exists = self.config.node_storage().certificate_store.multi_contains(digests.iter())?;
        let certificates_to_accept: Vec<_> = certificates
            .into_iter()
            .zip(exists.into_iter())
            .zip(digests.into_iter())
            .filter_map(|((cert, exist), digest)| {
                if exist {
                    debug!(target: "primary::state-sync", ?cert, ?digest, "skip processing certificate - already exists");
                    self.consensus_bus
                        .primary_metrics()
                        .node_metrics
                        .duplicate_certificates_processed
                        .inc();
                    return None;
                }
                Some((digest, cert))
            })
            .collect();

        // return early if all certificates already processed
        if certificates.is_empty() {
            return Ok(());
        }

        for (digest, certificate) in certificates_to_accept {
            // check if this certificate is currently pending already and skip further validation
            if external {
                if self.is_pending(&digest) {
                    trace!(target: "primary::synchronizer", "Certificate {digest:?} is still suspended. Skip processing.");
                    self.consensus_bus
                        .primary_metrics()
                        .node_metrics
                        .certificates_suspended
                        .with_label_values(&["dedup_locked"])
                        .inc();
                    return Err(CertificateError::Pending(digest));
                }
            }

            // check parents are either accounted for or garbage collected
            if certificate.round() > self.gc_round.load() + 1 {
                let missing_parents = self.get_missing_parents(&certificate).await?;
                if !missing_parents.is_empty() {
                    // forward to pending certificate manager
                    self.consensus_bus
                        .pending_cert_commands()
                        .send(PendingCertCommand::MissingParents { certificate, missing_parents })
                        .await?;

                    return Err(CertificateError::Pending(digest));
                }
            }

            // no missing parents - update pending state and try to process an ready certs
            // let unlocked_pending_certs =
            //     self.process_accepted_certificate(certificate.round(), digest)?;
            self.consensus_bus
                .pending_cert_commands()
                .send(PendingCertCommand::ProcessNewVerifiedCertificate { certificate })
                .await?;

            // accept in causal order
            self.try_accept_certificate(certificate, digest).await?;
            for ready in unlocked_pending_certs {
                let ready_digest = ready.digest();
                self.try_accept_certificate(ready, ready_digest).await?;
            }
        }

        // update metrics
        self.consensus_bus
            .primary_metrics()
            .node_metrics
            .certificates_currently_suspended
            .set(self.pending.len() as i64);

        Ok(())
    }

    /// Forward certificate and digest to the `CertificateAcceptor` task.
    async fn try_accept_certificate(
        &self,
        certificate: Certificate,
        digest: CertificateDigest,
    ) -> CertificateResult<()> {
        let (reply, result) = oneshot::channel();
        self.consensus_bus.verified_certificates().send((certificate, digest, reply)).await?;
        result.await.map_err(|_| CertificateError::CertificateAcceptorOneshot)?
    }

    /// Send an update to the `PendingCertificateManager` to update pending state.
    ///
    /// This method returns a collection of unlocked certificates to accept.

    /// Check that certificate's parents are in storage. Returns the digests of any parents that are missing.
    async fn get_missing_parents(
        &self,
        certificate: &Certificate,
    ) -> CertificateResult<HashSet<CertificateDigest>> {
        let _scope = monitored_scope("primary::state-sync::get_missing_parents");

        // handle genesis cert
        if certificate.round() == 1 {
            for digest in certificate.header().parents() {
                if !self.genesis.contains_key(digest) {
                    return Err(CertificateError::from(HeaderError::InvalidGenesisParent(*digest)));
                }
            }
            return Ok(HashSet::with_capacity(0));
        }

        let existence = self
            .config
            .node_storage()
            .certificate_store
            .multi_contains(certificate.header().parents().iter())?;

        let missing_parents: HashSet<_> = certificate
            .header()
            .parents()
            .iter()
            .zip(existence.iter())
            .filter(|(_, exists)| !*exists)
            .map(|(digest, _)| *digest)
            .collect();

        if !missing_parents.is_empty() {
            self.consensus_bus
                .certificate_fetcher()
                .send(CertificateFetcherCommand::Ancestors(certificate.clone()))
                .await?;
        }

        Ok(missing_parents)
    }

    /// Helper method to query the pending status of a digest.
    async fn check_pending_status(&self, digest: CertificateDigest) -> CertificateResult<bool> {
        // self.consensus_bus.
        todo!()
    }
}
