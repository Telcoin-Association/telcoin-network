//! The state of consensus

use crate::{
    consensus::{
        bullshark::Bullshark, liveness::RoundQuorumObserver, utils::gc_round, ConsensusError,
        Outcome,
    },
    metrics::{record_mode_transition, ModeTransitionReason},
    ConsensusBus, NodeMode,
};
use std::{
    cmp::{max, Ordering},
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt::Debug,
    sync::Arc,
    time::{Duration, Instant},
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
    TnSender, MAX_COMMIT_RUN_AHEAD_ROUNDS,
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
        debug!(
            target: "primary::consensus_state",
            num_certs,
            rounds = dag.len(),
            gc_round,
            min_round = dag.first_key_value().map_or(0, |(round, _)| *round),
            max_round = dag.last_key_value().map_or(0, |(round, _)| *round),
            "dag restored"
        );

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

        // Metric: certificate_commit_latency_ms - time from certificate creation to commit. the
        // age reads the wall clock against the header's own creation time, so it is a log field
        // only and never feeds a consensus decision
        let created_at = certificate.header().created_at_ms();
        info!(
            target: "consensus::metrics",
            round = certificate.round(),
            origin = ?certificate.origin(),
            digest = ?certificate.digest(),
            committed_round = self.last_round.committed_round,
            gc_round = self.last_round.gc_round,
            created_at_ms = created_at.as_millis(),
            age_ms = u64::try_from(created_at.elapsed().as_millis()).unwrap_or(u64::MAX),
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
    ///
    /// Shared with the epoch's owner (`run_epoch` in the node), which seals it once this task has
    /// ended and stops the node if the seal reports a failed save: an error this task returned
    /// after the epoch's shutdown was signalled would only be logged.
    certificate_pack: Option<Arc<CertificatePack>>,

    /// The consensus pack's committed round when this task was spawned: the floor of the executed
    /// round the run-ahead gate measures commits against.
    ///
    /// Initial and new-epoch starts replay the pack and wait for its execution before consensus
    /// starts, so there the floor matches execution. After a promotion from catch-up nothing is
    /// replayed and the pack written by state sync can lead execution; the floor keeps that lead
    /// from holding the first commits.
    exec_floor: Round,

    /// How many rounds a committed leader may run ahead of the executed round before its commit
    /// is held: [`MAX_COMMIT_RUN_AHEAD_ROUNDS`], kept as a field so a test can hold a commit
    /// without building a thousand rounds.
    max_run_ahead: Round,
}

/// The outcome of archiving a certificate in this epoch's certificate pack, given what
/// [`CertificatePack::try_save`] returned.
///
/// Only a full channel ([`PackError::SendFull`]) is transient backpressure: that certificate goes
/// unarchived and consensus carries on. Any other error means the pack has failed for good, and it
/// is this node's own storage failing, never a peer's input: certificates are verified before they
/// reach consensus, and nothing a peer sends can latch the pack (only its own open, write or sync
/// failing, or its thread being gone, does), short of a peer's batch flood filling the shared disk
/// (the known exception in the storage README §8). So it ends the consensus task, which stops the
/// node.
fn certificate_archived(saved: Result<(), PackError>) -> Result<(), ConsensusError> {
    match saved {
        Err(PackError::SendFull) => {
            warn!(target: "telcoin::consensus_state", "cert pack is backed up; this certificate was not archived");
            Ok(())
        }
        saved => saved.map_err(ConsensusError::CertificatePack),
    }
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
/// Commit milliseconds therefore strictly increase within an epoch but not across the seam. The
/// closing EVM block keeps only whole seconds, so this floor drops the sub-second part of the
/// previous epoch's last commit: the epoch's first commit lands after the closing second but can
/// sit up to 998 ms below that last commit. The whole seconds, and so the EVM timestamp, never
/// decrease.
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

/// Whether a sub-dag committed at `leader_round` may go on to execution while this node has
/// executed through leader round `exec_round`: true unless the leader runs more than
/// `max_run_ahead` rounds ahead. The sum saturates, so no round overflows the check.
pub(crate) const fn commit_run_ahead_ok(
    leader_round: Round,
    exec_round: Round,
    max_run_ahead: Round,
) -> bool {
    leader_round <= exec_round.saturating_add(max_run_ahead)
}

/// The lowest round of a restored DAG that has a hole: a round missing between the round after
/// the GC round, `last_committed_round - gc_depth + 1`, and `last_committed_round`.
///
/// The restore reads every stored certificate above `gc_round` (see
/// `construct_dag_from_cert_store`), and ordering can need any of them, not only those above the
/// committed round: a certificate in `(gc_round, last_committed_round]` that no leader has
/// committed yet is still ordered by the next leader whose history reaches it, and `order_dag`
/// skips a parent missing from the DAG instead of failing. Ordering over a hole can sequence a
/// sub-dag no other node commits. Two histories leave one:
/// - execution lagged: the certificate store prunes from its newest round, and the consensus pack
///   can trail that round by more than the prune depth less `gc_depth`, so the DAG starts above the
///   round after the GC round;
/// - the node was inactive, when it stores no certificates, and restarts within about `gc_depth`
///   rounds of its promotion: the rounds it spent inactive are missing at the bottom or in the
///   middle of the window. Such a restart is demoted once although nothing is wrong with the node;
///   it costs one more catch-up, the price of never ordering over a hole.
///
/// `None` for a fresh epoch (nothing committed), an empty DAG (a promotion from catch-up, which
/// stores no certificates) and a DAG holding every round of the window.
fn restored_dag_gap(last_committed_round: Round, gc_round: Round, dag: &Dag) -> Option<Round> {
    let dag_min_round = *dag.first_key_value()?.0;
    let expected_min_round = gc_round.saturating_add(1);
    let window = expected_min_round..=last_committed_round;
    // `range` panics on an inverted range, so an empty window (a zero gc_depth) checks the bottom
    let window_complete =
        window.is_empty() || dag.range(window.clone()).map(|(round, _)| *round).eq(window);
    (last_committed_round > 0 && (dag_min_round > expected_min_round || !window_complete))
        .then_some(dag_min_round)
}

impl<DB: Database> Consensus<DB> {
    pub async fn spawn(
        consensus_config: ConsensusConfig<DB>,
        consensus_bus: &ConsensusBus,
        protocol: Bullshark,
        task_manager: &TaskManager,
        consensus_chain: &impl ConsensusChainReader,
        certificate_pack: Option<Arc<CertificatePack>>,
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

        // an active cvv must not order from a restored dag with a hole between the round after
        // its gc round and its committed round (see `restored_dag_gap`); it goes inactive and
        // catches up from its peers instead
        if consensus_bus.is_active_cvv() {
            let gc_round = state.last_round.gc_round;
            if let Some(dag_min_round) =
                restored_dag_gap(last_committed_round, gc_round, &state.dag)
            {
                info!(
                    target: "primary::consensus",
                    last_committed_round,
                    dag_min_round,
                    gc_round,
                    expected_min = gc_round.saturating_add(1),
                    "restored dag has a gap above the committed round; going inactive"
                );
                let previous = consensus_bus.app().node_mode().send_replace(NodeMode::CvvInactive);
                record_mode_transition(
                    previous,
                    NodeMode::CvvInactive,
                    ModeTransitionReason::RestartGap,
                );
                consensus_config.shutdown().notify();
                return Ok(());
            }
        }

        let s = Self {
            committee: consensus_config.committee().clone(),
            consensus_bus: consensus_bus.clone(),
            consensus_config: consensus_config.clone(),
            rx_shutdown,
            protocol,
            state,
            active: false,
            certificate_pack,
            exec_floor: last_committed_round,
            max_run_ahead: MAX_COMMIT_RUN_AHEAD_ROUNDS,
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

    /// Run active consensus and service observation deadlines without new traffic.
    async fn run(
        mut self,
        mut rx_new_certificates: impl TnReceiver<Certificate>,
    ) -> Result<(), ConsensusError> {
        self.active = self.consensus_bus.is_active_cvv();
        let mut observer = RoundQuorumObserver::from_recovered_dag(
            self.committee.clone(),
            self.consensus_bus.app().metrics().clone(),
            self.state.last_round.committed_round,
            &self.state.dag,
        );
        let mut observation_tick = tokio::time::interval(Duration::from_secs(1));
        observation_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Listen to incoming certificates.
        loop {
            // biased so shutdown is checked first: once a commit returned on shutdown (a held
            // run-ahead gate), no certificate queued meanwhile may commit past the held sub-dag.
            // the tick goes before the certificates, so a certificate burst cannot starve it
            tokio::select! {
                biased;

                // The epoch's owner seals the certificate pack once this task has ended.
                _ = &self.rx_shutdown => return Ok(()),

                _ = observation_tick.tick() => {
                    observer.evaluate_expired(tokio::time::Instant::now());
                },
                Some(certificate) = rx_new_certificates.recv() => {
                    self.new_certificate(certificate, &mut observer).await.inspect_err(|e| {
                        error!(target: "epoch-manager", ?e, "new certificate failed");
                    })?;
                },
            }
        }
    }

    /// Process a new certificate.
    #[instrument(level = "debug", skip_all, fields(round = certificate.round(), origin = ?certificate.origin()))]
    async fn new_certificate(
        &mut self,
        certificate: Certificate,
        observer: &mut RoundQuorumObserver,
    ) -> Result<(), ConsensusError> {
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
            certificate_archived(certificate_pack.try_save(certificate.clone()))?;
        }
        // Process the certificate using the selected consensus protocol.
        let round = certificate.round();
        let origin = certificate.origin().clone();
        let (outcome, committed_sub_dags) =
            self.protocol.process_certificate(&mut self.state, certificate)?;
        if outcome != Outcome::CertificateBelowCommitRound {
            observer.observe(self.committee.epoch(), round, &origin, tokio::time::Instant::now());
        }
        if self.active {
            let mut own_rounds_committed = Vec::new();
            let mut leader_commit_round = 0;
            let mut has_headers = false;
            let authority_id = self.consensus_config.authority_id();

            // Output the sequence in the right order.
            for committed_sub_dag in committed_sub_dags {
                // commits do not wait for local execution: the subscriber checks each leader's
                // execution block against this node's chain before it assembles and saves the
                // output. this gate only bounds how far commits run ahead of execution
                if !self.hold_commit_run_ahead(committed_sub_dag.leader_round()).await? {
                    // the epoch is shutting down: the rest of the batch, the own-header report and
                    // the commit-accounting check are skipped (a partial batch would fail that
                    // check); the next epoch start re-primes the committed round from the pack
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

                // copied out before the send moves the sub-dag
                let subdag_index = committed_sub_dag.leader().nonce();
                let leader_round = committed_sub_dag.leader_round();
                let commit_timestamp_ms = committed_sub_dag.commit_timestamp_ms().as_millis();
                let base_execution_block =
                    committed_sub_dag.leader().latest_execution_block().number;

                // NOTE: The size of the sub-dag can be arbitrarily large (depending on the network
                // condition and Byzantine leaders).
                self.consensus_bus
                    .sequence()
                    .send(committed_sub_dag)
                    .await
                    .map_err(|_| ConsensusError::ShuttingDown)?;

                // the sub-dag is final once the run-ahead gate let it through; this line marks its
                // hand-off to the subscriber, which owns the execution wait that follows
                info!(
                    target: "consensus::metrics",
                    subdag_index,
                    leader_round,
                    commit_timestamp_ms,
                    base_execution_block,
                    "subdag sequenced"
                );
                observer.record_commit();
            }

            if has_headers {
                self.consensus_bus
                    .committed_own_headers()
                    .send((leader_commit_round, own_rounds_committed))
                    .await
                    .map_err(|_| ConsensusError::ShuttingDown)?;

                // Commit-accounting invariant: the last committed round must equal the max
                // header round we just sequenced. If it ever disagrees, go inactive and shut down
                // cleanly (as the subscriber's fork check does) instead of panicking on the
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
                    let previous =
                        self.consensus_bus.app().node_mode().send_replace(NodeMode::CvvInactive);
                    record_mode_transition(
                        previous,
                        NodeMode::CvvInactive,
                        ModeTransitionReason::CommitAccounting,
                    );
                    self.consensus_config.shutdown().notify();
                }
            }
        }
        Ok(())
    }

    /// Hold the commit of a sub-dag led at `leader_round` while it runs more than
    /// [`Self::max_run_ahead`] rounds ahead of the leader round this node has executed.
    ///
    /// The executed round is the last leader round the engine processed, raised to
    /// [`Self::exec_floor`]. A hold logs one warning and counts once in
    /// `commit_run_ahead_blocked_total`, then ends, with an info line giving its length, when
    /// execution advances far enough.
    ///
    /// Returns `Ok(false)` if the epoch shuts down while the commit is held, so the caller can
    /// return before it reports a partial batch.
    async fn hold_commit_run_ahead(&self, leader_round: Round) -> Result<bool, ConsensusError> {
        let app = self.consensus_bus.app();
        let exec_round = || app.last_consensus_round().max(self.exec_floor);
        if commit_run_ahead_ok(leader_round, exec_round(), self.max_run_ahead) {
            return Ok(true);
        }
        // subscribe, then read again: an execution update between the first read and the
        // subscription is seen by the second read, any later one wakes the wait
        let mut rx_recent_blocks = app.recent_blocks().subscribe();
        if commit_run_ahead_ok(leader_round, exec_round(), self.max_run_ahead) {
            return Ok(true);
        }
        let held_at = Instant::now();
        warn!(
            target: "primary::consensus",
            leader_round,
            exec_round = exec_round(),
            "commit held: execution more than MAX_COMMIT_RUN_AHEAD_ROUNDS behind"
        );
        app.metrics().commit_run_ahead_blocked_total.increment(1);
        loop {
            tokio::select! {
                biased;

                _ = &self.rx_shutdown => return Ok(false),
                changed = rx_recent_blocks.changed() => {
                    // the bus owns the sender, so this fails only once the bus is gone
                    changed.map_err(|_| ConsensusError::ShuttingDown)?;
                    let executed = exec_round();
                    if commit_run_ahead_ok(leader_round, executed, self.max_run_ahead) {
                        info!(
                            target: "primary::consensus",
                            leader_round,
                            exec_round = executed,
                            held_ms = u64::try_from(held_at.elapsed().as_millis())
                                .unwrap_or(u64::MAX),
                            "commit hold released"
                        );
                        return Ok(true);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod commit_run_ahead_tests {
    use super::{commit_run_ahead_ok, restored_dag_gap, Dag};
    use std::collections::HashMap;
    use tn_types::{Round, MAX_COMMIT_RUN_AHEAD_ROUNDS};

    /// The executed round every boundary case measures from.
    const EXEC: Round = 300;

    #[test]
    fn commit_run_ahead_ok_at_the_executed_round() {
        assert!(commit_run_ahead_ok(EXEC, EXEC, MAX_COMMIT_RUN_AHEAD_ROUNDS));
    }

    #[test]
    fn commit_run_ahead_ok_one_round_ahead() {
        assert!(commit_run_ahead_ok(EXEC + 1, EXEC, MAX_COMMIT_RUN_AHEAD_ROUNDS));
    }

    #[test]
    fn commit_run_ahead_ok_at_the_bound() {
        assert!(commit_run_ahead_ok(
            EXEC + MAX_COMMIT_RUN_AHEAD_ROUNDS,
            EXEC,
            MAX_COMMIT_RUN_AHEAD_ROUNDS
        ));
        // a fresh start (nothing executed) commits up to the bound itself
        assert!(commit_run_ahead_ok(MAX_COMMIT_RUN_AHEAD_ROUNDS, 0, MAX_COMMIT_RUN_AHEAD_ROUNDS));
    }

    #[test]
    fn commit_run_ahead_holds_past_the_bound() {
        assert!(!commit_run_ahead_ok(
            EXEC + MAX_COMMIT_RUN_AHEAD_ROUNDS + 1,
            EXEC,
            MAX_COMMIT_RUN_AHEAD_ROUNDS
        ));
        assert!(!commit_run_ahead_ok(
            MAX_COMMIT_RUN_AHEAD_ROUNDS + 1,
            0,
            MAX_COMMIT_RUN_AHEAD_ROUNDS
        ));
    }

    #[test]
    fn commit_run_ahead_ok_saturates_at_the_round_ceiling() {
        assert!(commit_run_ahead_ok(Round::MAX, Round::MAX, MAX_COMMIT_RUN_AHEAD_ROUNDS));
        assert!(commit_run_ahead_ok(Round::MAX, Round::MAX - 1, MAX_COMMIT_RUN_AHEAD_ROUNDS));
        // an executed round ahead of the leader (an earlier epoch's last round) never holds
        assert!(commit_run_ahead_ok(2, Round::MAX, MAX_COMMIT_RUN_AHEAD_ROUNDS));
    }

    /// A restored DAG is a gap only when it is not empty, the epoch has committed something and
    /// it misses a round between the round after the GC round, `C - gc_depth + 1`, and `C`.
    #[test]
    fn restored_dag_gap_boundaries() {
        const GC_DEPTH: Round = 50;
        const C: Round = 200;
        const GC_ROUND: Round = C - GC_DEPTH;
        // rounds `min_round` through `C + 2`, without `missing`
        let dag_from_without = |min_round: Round, missing: Option<Round>| -> Dag {
            (min_round..=C + 2)
                .filter(|round| Some(*round) != missing)
                .map(|round| (round, HashMap::new()))
                .collect()
        };
        let dag_from = |min_round: Round| dag_from_without(min_round, None);
        assert_eq!(restored_dag_gap(C, GC_ROUND, &Dag::new()), None, "an empty dag (a promotion)");
        assert_eq!(restored_dag_gap(0, 0, &dag_from(150)), None, "nothing committed yet");
        assert_eq!(
            restored_dag_gap(C, GC_ROUND, &dag_from(C - GC_DEPTH + 1)),
            None,
            "the round after the gc round is present"
        );
        assert_eq!(restored_dag_gap(C, GC_ROUND, &dag_from(C - 120)), None, "rounds below it too");
        assert_eq!(
            restored_dag_gap(C, GC_ROUND, &dag_from(C - GC_DEPTH + 2)),
            Some(C - GC_DEPTH + 2),
            "the round after the gc round is missing"
        );
        // the old boundary admitted a dag reaching down to the round after the committed round,
        // with a hole in the rounds an uncommitted certificate can still be ordered from
        assert_eq!(restored_dag_gap(C, GC_ROUND, &dag_from(C + 1)), Some(C + 1));
        // a hole in the middle of the window, from rounds spent inactive before a promotion
        let middle_hole = dag_from_without(C - GC_DEPTH + 1, Some(C - 10));
        assert_eq!(restored_dag_gap(C, GC_ROUND, &middle_hole), Some(C - GC_DEPTH + 1));
        let committed_round_missing = dag_from_without(C - GC_DEPTH + 1, Some(C));
        assert_eq!(restored_dag_gap(C, GC_ROUND, &committed_round_missing), Some(C - GC_DEPTH + 1));
        // inside the epoch's first gc_depth rounds the gc round saturates at 0: the dag must reach
        // round 1
        assert_eq!(restored_dag_gap(20, 0, &dag_from(1)), None, "a healthy early restart");
        assert_eq!(restored_dag_gap(20, 0, &dag_from(2)), Some(2));
        // a zero gc_depth leaves an empty window: only the bottom is checked
        assert_eq!(restored_dag_gap(C, C, &dag_from(C + 1)), None);
        assert_eq!(restored_dag_gap(C, C, &dag_from(C + 2)), Some(C + 2));
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
