//! The state of consensus

use crate::{
    consensus::{bullshark::Bullshark, utils::gc_round, ConsensusError},
    ConsensusBus, NodeMode,
};
use std::{
    cmp::{max, Ordering},
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Debug,
};
use tn_config::ConsensusConfig;
use tn_storage::{
    certificate_pack::{CertificatePack, PackError},
    CertificateStore,
};
use tn_types::{
    forks::subsecond_timestamp_active, AuthorityIdentifier, Certificate, CommittedSubDag,
    Committee, ConsensusChainReader, Database, Epoch, EpochSeedChainError, EpochSeedChainValue,
    Hash as _, HeaderDigest, Noticer, Round, TaskManager, TimestampMs, TimestampSec, TnReceiver,
    TnSender,
};
use tracing::{debug, error, info, instrument, warn};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
mod consensus_tests;

/// The representation of the DAG in memory.
pub type Dag = BTreeMap<Round, HashMap<AuthorityIdentifier, (HeaderDigest, Certificate)>>;

/// The state that needs to be persisted for crash-recovery.
#[derive(Debug)]
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
    /// The epoch seed chain value the next commit folds into.
    ///
    /// Private on purpose: it may only be seeded from a fail-closed recovery (see
    /// [`Consensus::spawn`]) and thereafter advanced through [`Self::set_seed_chain`] with the
    /// value of a sub-dag that was actually committed. Any other provenance would re-root the
    /// chain and fork execution permanently.
    seed_chain: EpochSeedChainValue,
    /// The lower bound for the epoch's first commit timestamp (see
    /// [`CommittedSubDag::new_with_commit_floor`]).
    ///
    /// Private and never mutated: it is resolved once from committed history when consensus
    /// starts the epoch (see [`Consensus::spawn`]). It is kept apart from
    /// [`Self::last_committed_sub_dag`] because that field also seeds reputation scores, so it
    /// cannot stand in for a commit from the previous epoch.
    epoch_commit_floor: Option<TimestampMs>,
}

impl ConsensusState {
    /// Create a new empty ConsensusState.  Used for tests.
    ///
    /// The seed chain starts at [`EpochSeedChainValue::genesis_placeholder`], which is the pinned
    /// fixture anchor. Production state is always built by [`Self::new_from_store`], which resolves
    /// the anchor from the recovered commit.
    pub fn new(gc_depth: Round) -> Self {
        Self {
            last_round: ConsensusRound::default(),
            gc_depth,
            last_committed: Default::default(),
            dag: Default::default(),
            last_committed_sub_dag: None,
            seed_chain: EpochSeedChainValue::genesis_placeholder(),
            epoch_commit_floor: None,
        }
    }

    /// The epoch seed chain value the next commit folds into.
    pub fn seed_chain(&self) -> EpochSeedChainValue {
        self.seed_chain
    }

    /// Advance the epoch seed chain to `seed_chain`.
    ///
    /// The caller must pass the value of the sub-dag it has just committed
    /// ([`CommittedSubDag::seed_chain_value`]), in commit order, so the chain matches what every
    /// other honest node folds.
    pub fn set_seed_chain(&mut self, seed_chain: EpochSeedChainValue) {
        self.seed_chain = seed_chain;
    }

    /// The lower bound for the epoch's first commit timestamp, in milliseconds.
    ///
    /// `None` when no floor applies (see [`resolve_epoch_commit_floor`]). Once the epoch has
    /// committed, [`CommittedSubDag::new_with_commit_floor`] clamps against the previous sub-dag
    /// and ignores this value.
    pub fn epoch_commit_floor(&self) -> Option<TimestampMs> {
        self.epoch_commit_floor
    }

    fn new_from_store<DB: Database>(
        last_committed_round: Round,
        gc_depth: Round,
        recovered_last_committed: HashMap<AuthorityIdentifier, Round>,
        latest_sub_dag: Option<CommittedSubDag>,
        seed_chain: EpochSeedChainValue,
        epoch_commit_floor: Option<TimestampMs>,
        cert_store: DB,
    ) -> Self {
        let last_round = ConsensusRound::new_with_gc_depth(last_committed_round, gc_depth);

        let dag = Self::construct_dag_from_cert_store(
            &cert_store,
            &recovered_last_committed,
            last_round.gc_round,
        )
        .expect("error when recovering DAG from store");

        let last_committed_sub_dag = latest_sub_dag.clone();

        Self {
            gc_depth,
            last_round,
            last_committed: recovered_last_committed,
            last_committed_sub_dag,
            dag,
            seed_chain,
            epoch_commit_floor,
        }
    }

    #[instrument(level = "info", skip_all)]
    fn construct_dag_from_cert_store<DB: CertificateStore>(
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
    #[instrument(level = "debug", skip_all, fields(round = certificate.round(), origin = ?certificate.origin()))]
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

        // Metric: certificate_commit_latency_ms - time from certificate creation to commit
        info!(
            target: "consensus::metrics",
            round = certificate.round(),
            origin = ?certificate.origin(),
            digest = ?certificate.digest(),
            committed_round = self.last_round.committed_round,
            gc_round = self.last_round.gc_round,
            "certificate committed"
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
            let store_parents: BTreeSet<&HeaderDigest> =
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

/// Telcoin Network consensus.
#[derive(Debug)]
pub struct Consensus<DB> {
    /// The committee information.
    committee: Committee,
    /// The channel "bus" for consensus (container for consensus channel and watches).
    consensus_bus: ConsensusBus,
    /// Consensus config for the app, used to shutdown an epoch for mode switching.
    consensus_config: ConsensusConfig<DB>,

    /// Receiver for shutdown.
    rx_shutdown: Noticer,

    /// The consensus protocol to run.
    protocol: Bullshark,

    /// Inner state
    state: ConsensusState,

    /// Are we an active CVV?
    /// An active CVV is participating in consensus (not catching up or following as an NVV).
    active: bool,

    /// Optional pack that is available we want to save all our certificates into
    /// before sending to bullshark.
    certificate_pack: Option<CertificatePack>,
}

/// Resolve the epoch seed chain anchor from the two cursors recovered at startup, failing closed
/// on every inconsistent combination.
///
/// `latest_sub_dag` and `last_committed_round` are read from the same per-epoch pack, so they must
/// agree: the chain value has to be anchored to exactly the commit the cursor points at. Every
/// disagreement is an error rather than a fallback, because the only available fallback - starting
/// from the epoch root - re-roots the chain mid-epoch, and the chain value reaches the executed
/// block's `parent_beacon_block_root`, so that fork is permanent and never re-converges.
///
/// Split out of [`Consensus::spawn`] so the four `(latest_sub_dag, last_committed_round > 0)`
/// combinations are directly testable without standing up a node.
pub(crate) fn resolve_seed_chain_anchor(
    latest_sub_dag: Option<&CommittedSubDag>,
    last_committed_round: Round,
    epoch: Epoch,
) -> Result<EpochSeedChainValue, EpochSeedChainError> {
    match (latest_sub_dag, last_committed_round > 0) {
        // Recovered mid-epoch: continue the chain from the commit the cursor points at.
        (Some(sub_dag), true) if sub_dag.leader_round() == last_committed_round => {
            Ok(EpochSeedChainValue::from_committed(sub_dag.randomness()))
        }
        // One of the two cursors is stale; folding either would diverge from the network.
        (Some(sub_dag), true) => Err(EpochSeedChainError::LeaderRoundMismatch {
            sub_dag_leader_round: sub_dag.leader_round(),
            last_committed_round,
        }),
        // The cursor says this epoch has committed, but the commit itself is unreadable.
        (None, true) => {
            Err(EpochSeedChainError::MissingSubDagForCommittedRound { last_committed_round })
        }
        // A commit exists with no cursor pointing at it: the pack is inconsistent.
        (Some(sub_dag), false) => Err(EpochSeedChainError::SubDagWithoutCommittedRound {
            sub_dag_leader_round: sub_dag.leader_round(),
        }),
        // Genuine first commit of the epoch: start from the epoch root.
        (None, false) => Ok(EpochSeedChainValue::epoch_root(epoch)),
    }
}

/// Resolve the lower bound for the first commit timestamp of the epoch being started.
///
/// With sub-second timestamps active, [`CommittedSubDag::new_with_commit_floor`] raises the
/// epoch's first commit to at least 1 ms past this floor, so EVM time does not run backwards
/// across the epoch seam. The inputs are the fork schedule and committed history, never local
/// clocks or node progress, so every honest node resolves the same floor for the same epoch:
///
/// - `gate_active` false: `None`. Pre-fork commit timestamps are whole seconds and never take a
///   floor.
/// - `latest_sub_dag` present: `None`. The epoch has already committed, so this is a restart within
///   the epoch and the next commit clamps against the recovered sub-dag instead.
/// - `epoch` 0: `None`. The first epoch follows genesis rather than a closed epoch.
/// - Otherwise: `prior_epoch_close` in milliseconds (whole seconds, sub-second part 0). A config
///   without a close yields `None` and logs a warning; the node's epoch startup supplies one for
///   every epoch after 0, and only test configs omit it.
///
/// `gate_active` is [`subsecond_timestamp_active`] for `epoch`. The caller evaluates it so every
/// branch stays testable under any build's fork schedule.
fn resolve_epoch_commit_floor(
    gate_active: bool,
    latest_sub_dag: Option<&CommittedSubDag>,
    epoch: Epoch,
    prior_epoch_close: Option<TimestampSec>,
) -> Option<TimestampMs> {
    if !gate_active || latest_sub_dag.is_some() || epoch == 0 {
        return None;
    }
    let Some(close_secs) = prior_epoch_close else {
        // production epoch startup always supplies the close, so reaching this is a tripwire for
        // a config built without one
        warn!(
            target: "tn::consensus",
            epoch,
            "sub-second gate active but no prior epoch close was supplied; first commit of the epoch is not floored against the previous epoch"
        );
        return None;
    };
    Some(TimestampMs::from_parts(close_secs, 0))
}

impl<DB: Database> Consensus<DB> {
    pub async fn spawn(
        consensus_config: ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        protocol: Bullshark,
        task_manager: &TaskManager,
        consensus_chain: &impl ConsensusChainReader,
        certificate_pack: Option<CertificatePack>,
    ) -> Result<(), ConsensusError> {
        let rx_shutdown = consensus_config.shutdown().subscribe();
        // The consensus state (everything else is immutable).
        let current_epoch = consensus_config.epoch();
        let recovered_last_committed = consensus_chain.read_last_committed(current_epoch).await?;

        debug!(target: "epoch-manager", ?recovered_last_committed, "recovered last committed for epoch {}", current_epoch);
        let last_committed_round = recovered_last_committed
            .iter()
            .max_by(|a, b| a.1.cmp(b.1))
            .map(|(_k, v)| *v)
            .unwrap_or_else(|| 0);

        // ignore previous epochs
        //
        // A pack read failure is propagated rather than collapsed to `None`. `None` means "this
        // epoch has committed nothing yet", which seeds the epoch seed chain at its root; taking
        // that branch after a failed read would re-root the chain mid-epoch, and because the
        // chain value reaches the executed block's `parent_beacon_block_root` that fork is
        // permanent (see [`EpochSeedChainValue`]).
        let latest_sub_dag = consensus_chain
            .latest_consensus_header_from_pack(current_epoch)
            .await?
            .map(|h| h.sub_dag);

        debug!(target: "epoch-manager", ?latest_sub_dag, "recovered latest subdag:");

        // Recovery's self-consistency tripwire lives inside `resolve_seed_chain_anchor`, which
        // subsumes the standalone `sub_dag.leader_round() == last_committed_round` check main
        // carries here: it rejects that same disagreement as
        // `EpochSeedChainError::LeaderRoundMismatch`, and additionally rejects the two cursor
        // combinations the standalone check cannot see (a committed round with no readable
        // sub-dag, and a sub-dag with no cursor pointing at it). Both operands derive from the
        // same append-only `ConsensusPack`, so none of these is reachable on an honest
        // single-node crash; all three are typed, recoverable errors rather than an
        // `assert_eq!` backtrace, which is what `ConsensusInvariant` asks for at this site.
        let seed_chain = resolve_seed_chain_anchor(
            latest_sub_dag.as_ref(),
            last_committed_round,
            current_epoch,
        )?;

        let epoch_commit_floor = resolve_epoch_commit_floor(
            subsecond_timestamp_active(current_epoch),
            latest_sub_dag.as_ref(),
            current_epoch,
            consensus_config.prior_epoch_close(),
        );

        // restore local dag
        let state = ConsensusState::new_from_store(
            last_committed_round,
            consensus_config.parameters().gc_depth,
            recovered_last_committed,
            latest_sub_dag,
            seed_chain,
            epoch_commit_floor,
            consensus_config.node_storage().clone(),
        );

        consensus_bus
            .app()
            .committed_round_updates()
            .send_replace(state.last_round.committed_round);

        let s = Self {
            committee: consensus_config.committee().clone(),
            consensus_bus: consensus_bus.clone(),
            consensus_config: consensus_config.clone(),
            rx_shutdown,
            protocol,
            state,
            active: false,
            certificate_pack,
        };

        // Only run the consensus task if we are an active CVV.
        // Active means we are participating in consensus.
        if consensus_bus.is_active_cvv() {
            // Subscribe before spawning so the channel is active before any messages are sent.
            let rx_new_certificates = consensus_bus.subscribe_new_certificates();
            task_manager.spawn_critical_task("consensus task", async move {
                Ok(s.run(rx_new_certificates).await?)
            });
        }
        Ok(())
    }

    async fn run(
        mut self,
        mut rx_new_certificates: impl TnReceiver<Certificate>,
    ) -> Result<(), ConsensusError> {
        self.active = self.consensus_bus.is_active_cvv();

        // Listen to incoming certificates.
        loop {
            tokio::select! {
                _ = &self.rx_shutdown => {
                    if let Some(pack) = self.certificate_pack {
                        if let Err(e) = pack.shutdown().await {
                            error!(target: "epoch-manager", ?e, "error shutting down certificate pack");
                        }
                    }
                    return Ok(())
                }

                Some(certificate) = rx_new_certificates.recv() => {
                    self.new_certificate(certificate).await.inspect_err(|e| {
                        error!(target: "epoch-manager", ?e, "new certificate failed");
                    })?;
                },
            }
        }
    }

    /// Process a new certificate.
    #[instrument(level = "debug", skip_all, fields(round = certificate.round(), origin = ?certificate.origin()))]
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
        if let Some(certificate_pack) = &self.certificate_pack {
            if let Err(e) = certificate_pack.try_save(certificate.clone()) {
                tracing::error!(target: "telcoin::consensus_state", ?e, "Failed to save certificate to cert pack file");
                // The certificate pack is in a failed state so stop using it.
                // This is not a critical path so not stopping but if the DB gets in a failed
                // state the node is probably not long for world...
                // If the sender is overflowed then can try again later.
                if let PackError::SendFailed = e {
                    self.certificate_pack = None;
                }
            }
        }
        // Process the certificate using the selected consensus protocol.
        let (outcome, committed_sub_dags) =
            self.protocol.process_certificate(&mut self.state, certificate)?;
        if self.active {
            let mut own_rounds_committed = Vec::new();
            let mut leader_commit_round = 0;
            let mut has_headers = false;
            let authority_id = self.consensus_config.authority_id();

            // Output the sequence in the right order.
            let csd_len = committed_sub_dags.len();
            for (i, committed_sub_dag) in committed_sub_dags.into_iter().enumerate() {
                // We need to make sure execution has caught up so we can verify we have not forked.
                // This will force the follow function to not outrun execution...  this is probably
                // fine. Also once we can follow gossiped consensus output this will not really be
                // an issue (except during initial catch up).
                let base_execution_block = committed_sub_dag.leader().latest_execution_block();
                if self.consensus_bus.app().wait_for_execution(base_execution_block).await.is_err()
                {
                    // This seems to be a bogus sub dag, we are out of sync...
                    tracing::error!(target: "telcoin::consensus_state", "Got a bogus sub dag from bullshark, we are out of sync and probably can not recover!");
                    // Going inactive will probably not help us at this point....
                    self.consensus_bus
                        .app()
                        .node_mode()
                        .send_modify(|v| *v = NodeMode::CvvInactive);
                    self.consensus_config.shutdown().notify();
                    tracing::error!(target: "telcoin::consensus_state", ?base_execution_block, ?outcome, "commit {i} of {csd_len} subdags");
                    return Ok(());
                }

                tracing::debug!(target: "telcoin::consensus_state", "Commit in Sequence {:?}", committed_sub_dag.leader().nonce());

                for header in committed_sub_dag.headers() {
                    has_headers = true;
                    leader_commit_round = leader_commit_round.max(header.round());
                    // Now we are going to signal which of our own batches have been committed.
                    if Some(header.author()) == authority_id.as_ref() {
                        own_rounds_committed.push(header.round())
                    }
                }

                // metric: subdag committed + commit latency in fractional seconds. pre-fork
                // leaders carry whole seconds only, so their latency reads up to 1s high
                let metrics = self.consensus_bus.app().metrics();
                metrics.subdags_committed_total.increment(1);
                let commit_latency = committed_sub_dag.leader().created_at_ms().elapsed();
                metrics.commit_latency_seconds.record(commit_latency.as_secs_f64());

                // NOTE: The size of the sub-dag can be arbitrarily large (depending on the network
                // condition and Byzantine leaders).
                self.consensus_bus
                    .sequence()
                    .send(committed_sub_dag)
                    .await
                    .map_err(|_| ConsensusError::ShuttingDown)?;
            }

            if has_headers {
                self.consensus_bus
                    .committed_own_headers()
                    .send((leader_commit_round, own_rounds_committed))
                    .await
                    .map_err(|_| ConsensusError::ShuttingDown)?;

                // Commit-accounting invariant: the last committed round must equal the max
                // header round we just sequenced. If it ever disagrees, mirror the bogus-sub-dag
                // handling above (go inactive + shut down cleanly) instead of panicking on the
                // single consensus task; the notified shutdown ends the run loop on the next
                // select, so the trailing `Ok(())` is reached without further processing.
                if self.state.last_round.committed_round == leader_commit_round {
                    self.consensus_bus
                        .app()
                        .committed_round_updates()
                        .send_replace(self.state.last_round.committed_round);
                } else {
                    tracing::error!(
                        target: "telcoin::consensus_state",
                        committed_round = self.state.last_round.committed_round,
                        leader_commit_round,
                        "commit-accounting invariant violated: last committed round does not match leader commit round; going inactive and shutting down",
                    );
                    self.consensus_bus
                        .app()
                        .node_mode()
                        .send_modify(|v| *v = NodeMode::CvvInactive);
                    self.consensus_config.shutdown().notify();
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod epoch_commit_floor_tests {
    use super::resolve_epoch_commit_floor;
    use tn_types::{CommittedSubDag, Epoch, TimestampMs, TimestampSec};

    /// The previous epoch's closing EVM block timestamp used by every case.
    const CLOSE_SECS: TimestampSec = 1_700_000_000;

    /// Any epoch after 0.
    const EPOCH: Epoch = 3;

    #[test]
    fn epoch_commit_floor_is_none_when_gate_inactive() {
        assert_eq!(resolve_epoch_commit_floor(false, None, EPOCH, Some(CLOSE_SECS)), None);
    }

    #[test]
    fn epoch_commit_floor_is_none_after_in_epoch_commit() {
        let latest_sub_dag = CommittedSubDag::default();
        assert_eq!(
            resolve_epoch_commit_floor(true, Some(&latest_sub_dag), EPOCH, Some(CLOSE_SECS)),
            None
        );
    }

    #[test]
    fn epoch_commit_floor_is_none_for_epoch_zero() {
        assert_eq!(resolve_epoch_commit_floor(true, None, 0, Some(CLOSE_SECS)), None);
    }

    #[test]
    fn epoch_commit_floor_is_prior_epoch_close_in_whole_seconds() {
        let floor = resolve_epoch_commit_floor(true, None, EPOCH, Some(CLOSE_SECS));
        assert_eq!(floor, Some(TimestampMs::from_parts(CLOSE_SECS, 0)));
        assert_eq!(floor.map(TimestampMs::as_millis), Some(CLOSE_SECS * 1000));
    }

    #[test]
    fn epoch_commit_floor_is_none_without_prior_epoch_close() {
        assert_eq!(resolve_epoch_commit_floor(true, None, EPOCH, None), None);
    }
}
