//! The state of consensus

use crate::{
    consensus::{bullshark::Bullshark, utils::gc_round, ConsensusError, ConsensusMetrics},
    ConsensusBus, NodeMode,
};
use consensus_metrics::monitored_future;
use std::{
    cmp::{max, Ordering},
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Debug,
    sync::Arc,
};
use tn_config::ConsensusConfig;
use tn_storage::{CertificateStore, ConsensusStore};
use tn_types::{
    AuthorityIdentifier, Certificate, CertificateDigest, CommittedSubDag, Committee, Database,
    Hash as _, Noticer, Round, TaskManager, Timestamp, TnReceiver, TnSender,
};
use tracing::{debug, info, instrument};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
mod consensus_tests;

/// The representation of the DAG in memory.
pub type Dag = BTreeMap<Round, HashMap<AuthorityIdentifier, (CertificateDigest, Certificate)>>;

/// The state that needs to be persisted for crash-recovery.
pub struct ConsensusState {
    /// The information about the last committed round and corresponding GC round.
    pub last_round: ConsensusRound,
    /// The chosen gc_depth
    pub gc_depth: Round,
    /// Keeps the last committed round for each authority. This map is used to clean up the dag and
    /// ensure we don't commit twice the same certificate.
    pub last_committed: HashMap<AuthorityIdentifier, Round>,
    /// The last committed sub dag. If value is None, it means that we haven't committed any sub
    /// dag yet.
    pub last_committed_sub_dag: Option<CommittedSubDag>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything
    /// older must be regularly cleaned up through the function `update`.
    pub dag: Dag,
    /// Metrics handler
    pub metrics: Arc<ConsensusMetrics>,
}

impl ConsensusState {
    /// Create a new empty ConsensusState.  Used for tests.
    pub fn new(metrics: Arc<ConsensusMetrics>, gc_depth: Round) -> Self {
        Self {
            last_round: ConsensusRound::default(),
            gc_depth,
            last_committed: Default::default(),
            dag: Default::default(),
            last_committed_sub_dag: None,
            metrics,
        }
    }

    fn new_from_store<DB: Database>(
        metrics: Arc<ConsensusMetrics>,
        last_committed_round: Round,
        gc_depth: Round,
        recovered_last_committed: HashMap<AuthorityIdentifier, Round>,
        latest_sub_dag: Option<CommittedSubDag>,
        cert_store: DB,
    ) -> Self {
        let last_round = ConsensusRound::new_with_gc_depth(last_committed_round, gc_depth);

        let dag = Self::construct_dag_from_cert_store(
            &cert_store,
            &recovered_last_committed,
            last_round.gc_round,
        )
        .expect("error when recovering DAG from store");
        metrics.recovered_consensus_state.inc();

        let last_committed_sub_dag = if let Some(latest_sub_dag) = latest_sub_dag.as_ref() {
            let mut certificates = Vec::new();
            for cert in &latest_sub_dag.certificates {
                certificates.push(cert.clone());
            }
            Some(latest_sub_dag.clone())
        } else {
            None
        };

        Self {
            gc_depth,
            last_round,
            last_committed: recovered_last_committed,
            last_committed_sub_dag,
            dag,
            metrics,
        }
    }

    #[instrument(level = "info", skip_all)]
    pub fn construct_dag_from_cert_store<DB: CertificateStore>(
        cert_store: &DB,
        last_committed: &HashMap<AuthorityIdentifier, Round>,
        gc_round: Round,
    ) -> Result<Dag, ConsensusError> {
        let mut dag: Dag = BTreeMap::new();

        info!("Recreating dag from last GC round: {}", gc_round);

        // get all certificates at rounds > gc_round
        let certificates = cert_store.after_round(gc_round + 1).expect("database available");

        let mut num_certs = 0;
        for cert in &certificates {
            if Self::try_insert_in_dag(&mut dag, last_committed, gc_round, cert, false)? {
                info!("Inserted certificate: {:?}", cert);
                num_certs += 1;
            }
        }
        info!("Dag is restored and contains {} certs for {} rounds", num_certs, dag.len());

        Ok(dag)
    }

    /// Returns true if certificate is inserted in the dag.
    pub fn try_insert(&mut self, certificate: &Certificate) -> Result<bool, ConsensusError> {
        Self::try_insert_in_dag(
            &mut self.dag,
            &self.last_committed,
            self.last_round.gc_round,
            certificate,
            true,
        )
    }

    /// Returns true if certificate is inserted in the dag.
    fn try_insert_in_dag(
        dag: &mut Dag,
        last_committed: &HashMap<AuthorityIdentifier, Round>,
        gc_round: Round,
        certificate: &Certificate,
        check_parents: bool,
    ) -> Result<bool, ConsensusError> {
        if certificate.round() <= gc_round {
            debug!(target: "telcoin::consensus_state",
                "Ignoring certificate {:?} as it is at or before gc round {}",
                certificate, gc_round
            );
            return Ok(false);
        }
        if check_parents {
            Self::check_parents(certificate, dag, gc_round)?;
        }

        // Always insert the certificate even if it is below last committed round of its origin,
        // to allow verifying parent existence.
        if let Some((_, existing_certificate)) = dag
            .entry(certificate.round())
            .or_default()
            .insert(certificate.origin().clone(), (certificate.digest(), certificate.clone()))
        {
            // we want to error only if we try to insert a different certificate in the dag
            if existing_certificate.digest() != certificate.digest() {
                return Err(ConsensusError::CertificateEquivocation(
                    Box::new(certificate.clone()),
                    Box::new(existing_certificate),
                ));
            }
        }

        Ok(certificate.round()
            > last_committed.get(certificate.origin()).cloned().unwrap_or_default())
    }

    /// Update and clean up internal state after committing a certificate.
    pub fn update(&mut self, certificate: &Certificate) {
        self.last_committed
            .entry(certificate.origin().clone())
            .and_modify(|r| *r = max(*r, certificate.round()))
            .or_insert_with(|| certificate.round());
        self.last_round = self.last_round.update(certificate.round(), self.gc_depth);

        self.metrics
            .last_committed_round
            .with_label_values(&[])
            .set(self.last_round.committed_round as i64);
        let elapsed = certificate.created_at().elapsed().as_secs_f64();
        self.metrics
            .certificate_commit_latency
            .observe(certificate.created_at().elapsed().as_secs_f64());

        // NOTE: This log entry is used to compute performance.
        tracing::debug!(target: "telcoin::consensus_state",
            "Certificate {:?} took {} seconds to be committed at round {}",
            certificate.digest(),
            elapsed,
            certificate.round(),
        );

        // Purge all certificates past the gc depth.
        self.dag.retain(|r, _| *r > self.last_round.gc_round);
    }

    // Checks that the provided certificate's parents exist return an error if they do not.
    fn check_parents(
        certificate: &Certificate,
        dag: &Dag,
        gc_round: Round,
    ) -> Result<(), ConsensusError> {
        let round = certificate.round();
        // Skip checking parents if they are GC'ed.
        // Also not checking genesis parents for simplicity.
        if round <= gc_round + 1 {
            return Ok(());
        }
        if let Some(round_table) = dag.get(&(round - 1)) {
            let store_parents: BTreeSet<&CertificateDigest> =
                round_table.iter().map(|(_, (digest, _))| digest).collect();
            for parent_digest in certificate.header().parents() {
                if !store_parents.contains(parent_digest) {
                    return Err(ConsensusError::MissingParent(
                        *parent_digest,
                        Box::new(certificate.clone()),
                    ));
                }
            }
        } else {
            tracing::error!(target: "telcoin::consensus_state", "Parent round not found in DAG for {certificate:?}!");
            return Err(ConsensusError::MissingParentRound(Box::new(certificate.clone())));
        }
        Ok(())
    }
}

/// Holds information about a committed round in consensus.
///
/// When a certificate gets committed then
/// the corresponding certificate's round is considered a "committed" round. It bears both the
/// committed round and the corresponding garbage collection round.
#[derive(Debug, Default, Copy, Clone)]
pub struct ConsensusRound {
    pub committed_round: Round,
    pub gc_round: Round,
}

impl ConsensusRound {
    pub fn new(committed_round: Round, gc_round: Round) -> Self {
        Self { committed_round, gc_round }
    }

    pub fn new_with_gc_depth(committed_round: Round, gc_depth: Round) -> Self {
        let gc_round = gc_round(committed_round, gc_depth);

        Self { committed_round, gc_round }
    }

    /// Calculates the latest CommittedRound by providing a new committed round and the gc_depth.
    /// The method will compare against the existing committed round and return
    /// the updated instance.
    fn update(&self, new_committed_round: Round, gc_depth: Round) -> Self {
        let last_committed_round = max(self.committed_round, new_committed_round);
        let last_gc_round = gc_round(last_committed_round, gc_depth);

        ConsensusRound { committed_round: last_committed_round, gc_round: last_gc_round }
    }
}

pub struct Consensus<DB> {
    /// The committee information.
    committee: Committee,
    /// The chanell "bus" for consensus (container for consesus channel and watches).
    consensus_bus: ConsensusBus,

    /// Receiver for shutdown.
    rx_shutdown: Noticer,

    /// The consensus protocol to run.
    protocol: Bullshark<DB>,

    /// Metrics handler
    metrics: Arc<ConsensusMetrics>,

    /// Inner state
    state: ConsensusState,

    /// Are we an active CVV?
    /// An active CVV is participating in consensus (not catching up or following as an NVV).
    active: bool,
}

impl<DB: Database> Consensus<DB> {
    pub fn spawn(
        consensus_config: ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        protocol: Bullshark<DB>,
        task_manager: &TaskManager,
    ) {
        let metrics = consensus_bus.consensus_metrics();
        let rx_shutdown = consensus_config.shutdown().subscribe();
        // The consensus state (everything else is immutable).
        let current_epoch = consensus_config.epoch();
        let recovered_last_committed =
            consensus_config.node_storage().read_last_committed(current_epoch);

        debug!(target: "epoch-manager", ?recovered_last_committed, "recovered last committed for epoch {}", current_epoch);
        let last_committed_round = recovered_last_committed
            .iter()
            .max_by(|a, b| a.1.cmp(b.1))
            .map(|(_k, v)| *v)
            .unwrap_or_else(|| 0);

        // ignore previous epochs
        let latest_sub_dag = consensus_config
            .node_storage()
            .get_latest_sub_dag()
            .filter(|subdag| subdag.leader_epoch() >= current_epoch);

        debug!(target: "epoch-manager", ?latest_sub_dag, "recovered latest subdag:");
        if let Some(sub_dag) = &latest_sub_dag {
            assert_eq!(
                sub_dag.leader_round(),
                last_committed_round,
                "Last subdag leader round {} is not equal to the last committed round {}!",
                sub_dag.leader_round(),
                last_committed_round,
            );
        }

        // restore local dag
        let state = ConsensusState::new_from_store(
            metrics.clone(),
            last_committed_round,
            consensus_config.parameters().gc_depth,
            recovered_last_committed,
            latest_sub_dag,
            consensus_config.node_storage().clone(),
        );

        consensus_bus
            .update_consensus_rounds(state.last_round)
            .expect("Failed to send last_committed_round on initialization!");

        let s = Self {
            committee: consensus_config.committee().clone(),
            consensus_bus: consensus_bus.clone(),
            rx_shutdown,
            protocol,
            metrics,
            state,
            active: false,
        };

        // Only run the consensus task if we are an active CVV.
        // Active means we are participating in consensus.
        if consensus_bus.node_mode().borrow().is_active_cvv() {
            task_manager
                .spawn_critical_task("consensus", monitored_future!(s.run(), "Consensus", INFO));
        }
    }

    async fn run(mut self) -> Result<(), ConsensusError> {
        // Clone the bus or the borrow checker will yell at us...
        let bus_clone = self.consensus_bus.clone();
        let mut rx_new_certificates = bus_clone.new_certificates().subscribe();
        self.active = bus_clone.node_mode().borrow().is_active_cvv();

        // Listen to incoming certificates.
        loop {
            tokio::select! {
                _ = &self.rx_shutdown => {
                    return Ok(())
                }

                Some(certificate) = rx_new_certificates.recv() => {
                    self.new_certificate(certificate).await?;
                },
            }
        }
    }

    /// Process a new certificate.
    async fn new_certificate(&mut self, certificate: Certificate) -> Result<(), ConsensusError> {
        match certificate.epoch().cmp(&self.committee.epoch()) {
            Ordering::Equal => {
                // we can proceed.
            }
            _ => {
                tracing::debug!(target: "telcoin::consensus_state", "Already moved to the next epoch");
                return Ok(());
            }
        }
        // Process the certificate using the selected consensus protocol.
        let (_, committed_sub_dags) =
            self.protocol.process_certificate(&mut self.state, certificate)?;
        if self.active {
            // We extract a list of headers from this specific validator that
            // have been agreed upon, and signal this back to the narwhal sub-system
            // to be used to re-send batches that have not made it to a commit.
            let mut committed_certificates = Vec::new();

            // Output the sequence in the right order.
            for committed_sub_dag in committed_sub_dags {
                // We need to make sure execution has caught up so we can verify we have not forked.
                // This will force the follow function to not outrun execution...  this is probably
                // fine. Also once we can follow gossiped consensus output this will not really be
                // an issue (except during initial catch up).
                let base_execution_block = committed_sub_dag.leader.header.latest_execution_block;
                if self.consensus_bus.wait_for_execution(base_execution_block).await.is_err() {
                    // This seems to be a bogus sub dag, we are out of sync...
                    tracing::error!(target: "telcoin::consensus_state", "Got a bogus sub dag from bullshark, we are out of sync!");
                    self.consensus_bus.node_mode().send_modify(|v| *v = NodeMode::CvvInactive);
                    break;
                }

                tracing::debug!(target: "telcoin::consensus_state", "Commit in Sequence {:?}", committed_sub_dag.leader.nonce());

                for certificate in &committed_sub_dag.certificates {
                    committed_certificates.push(certificate.clone());
                }

                // NOTE: The size of the sub-dag can be arbitrarily large (depending on the network
                // condition and Byzantine leaders).
                self.consensus_bus
                    .sequence()
                    .send(committed_sub_dag)
                    .await
                    .map_err(|_| ConsensusError::ShuttingDown)?;
            }

            if !committed_certificates.is_empty() {
                // Highest committed certificate round is the leader round / commit round
                // expected by primary.
                let leader_commit_round = committed_certificates
                    .iter()
                    .map(|c| c.round())
                    .max()
                    .expect("committed_certificates isn't empty");

                self.consensus_bus
                    .committed_certificates()
                    .send((leader_commit_round, committed_certificates))
                    .await
                    .map_err(|_| ConsensusError::ShuttingDown)?;

                assert_eq!(self.state.last_round.committed_round, leader_commit_round);

                self.consensus_bus
                    .update_consensus_rounds(self.state.last_round)
                    .map_err(|_| ConsensusError::ShuttingDown)?;
            }

            self.metrics
                .consensus_dag_rounds
                .with_label_values(&[])
                .set(self.state.dag.len() as i64);
        }
        Ok(())
    }
}
