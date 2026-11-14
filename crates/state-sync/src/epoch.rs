//! Tasks and helpers for collecting epoch records trustlessly.

#[cfg(test)]
use tn_test_utils as _;

use eyre::WrapErr as _;
use futures::{StreamExt as _, TryStreamExt as _};
use std::{
    collections::BTreeSet,
    future::{ready, Future},
    time::Duration,
};

use tn_config::Parameters;
use tn_network_libp2p::error::NetworkError;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp};
use tn_storage::{
    consensus::ConsensusChain,
    epoch_records::{EpochDbError, EpochRecordValidation},
};
use tn_types::{
    AuthorityIdentifier, BlsPublicKey, ConsensusHeaderDigest, ConsensusNumHash, Epoch,
    EpochCertificate, EpochRecord, Noticer, TaskError, TaskSpawner,
};
use tracing::{debug, error, info};

use crate::metrics::{StateSyncMetrics, STATE_SYNC_METRICS};

/// How long to wait before retrying a failed epoch record collection.
const EPOCH_COLLECT_RETRY_SECS: u64 = 5;

/// Maximum time spent collecting epoch records before startup continues.
const STARTUP_SYNC_TIMEOUT: Duration = Duration::from_secs(30);

/// Give each startup pass one background retry interval to make database progress.
const STARTUP_SYNC_PASS_TIMEOUT: Duration = Duration::from_secs(EPOCH_COLLECT_RETRY_SECS);

/// Cadence for checking whether a startup dial has established a connection.
const STARTUP_PEER_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Pause before repeating a startup pass that no peer answered.
///
/// Peers between epochs drop inbound primary requests for a fraction of a second, so a short
/// pause lets the next pass reach them once they serve again.
const STARTUP_SYNC_RETRY_BACKOFF: Duration = Duration::from_millis(250);

/// How long unanswered startup passes are repeated before their silence counts as the answer.
///
/// A peer between epochs drops inbound primary requests for well under a second, so this covers a
/// restart into a transitioning network several times over. It stays short because at genesis,
/// and whenever the whole network restarts at once, every peer drops requests until its own first
/// epoch starts, which happens only after its own startup sync: without this cap every node would
/// sit out the full [`STARTUP_SYNC_TIMEOUT`] before serving anyone. A node that gives up on a
/// longer gap and starts an epoch the network already closed is still demoted once the record
/// collector stores a later epoch's certified record (see `run_epoch` in `tn-node`).
const STARTUP_SYNC_UNANSWERED_BUDGET: Duration = Duration::from_secs(2);

/// Most epochs a record collection holds open as walked-past gaps at a time (see
/// [`Collection::gaps`]). It bounds the record collector's gap retry to this many record requests
/// per retry interval; past it, collection stops at the next uncertified epoch as it does with the
/// walk off.
const MAX_UNCERTIFIED_GAPS: usize = 16;

/// Longest one gap retry may hold the record collector loop (see [`retry_gaps`]): one retry
/// interval.
///
/// The retry runs at most once per retry interval, so with this bound the pass after it (and with
/// it the certified-record republish and the certified-prefix gauge) starts at most one interval
/// late, never later than the retry timer would have started it. A single record request can take
/// far longer: `request_epoch_cert` asks up to three peers in turn, each bounded only by the
/// request-response timeout of 10 s. So the retry stops at this deadline even mid-request, and the
/// next retry starts at the gap after the one it stopped at.
const GAP_RETRY_DEADLINE: Duration = Duration::from_secs(EPOCH_COLLECT_RETRY_SECS);

/// Where a collection pass stopped (see [`collect_epoch_records`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CollectedPass {
    /// The last collected epoch: the cursor the next pass starts from.
    epoch: Epoch,
    /// False when the pass stopped because no peer answered the request for the next epoch (no
    /// peer, a dropped request, an unresolved responder). Such a pass says nothing about the
    /// network's newest record. Every other stop is a peer's answer: it does not hold the epoch,
    /// or it sent a record this node refused or cannot anchor yet.
    definitive: bool,
}

/// Cursor and injected operations carried between startup collection passes.
struct StartupSyncState<Collect, Progress> {
    /// Last collected epoch, or the predecessor of the certified prefix after a capped pass.
    last_epoch: Epoch,
    /// The previous pass's epoch when it was definitive, compared against the next result when
    /// detecting quiescence.
    previous_pass: Option<Epoch>,
    /// When the current run of unanswered passes began; `None` once a pass is answered.
    unanswered_since: Option<tokio::time::Instant>,
    /// Fetch and validate records from the current cursor.
    collect: Collect,
    /// Read the first epoch without a historical certificate.
    progress: Progress,
}

/// The record collection's switch, read from [`Parameters`]. It defaults off, which is the
/// collection from before it existed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct EpochRecordCollectSwitches {
    /// [`Parameters::epoch_record_collect_past_uncertified`]: walk past an epoch this node holds as
    /// its own uncertified record (see [`Collection::may_walk_past`]).
    past_uncertified: bool,
}

impl From<&Parameters> for EpochRecordCollectSwitches {
    fn from(parameters: &Parameters) -> Self {
        Self { past_uncertified: parameters.epoch_record_collect_past_uncertified }
    }
}

/// What the passes of one record collection share: the record collector keeps one for its whole
/// life, the startup sync makes a new one for every pass.
struct Collection<'m> {
    /// The switches the collection runs with.
    switches: EpochRecordCollectSwitches,
    /// Epochs this node holds as its own uncertified record that a pass walked past: a certified
    /// record after the epoch validated against this node's own record and is stored. The record
    /// collector asks for each once per retry interval ([`retry_gaps`]) and drops it once any path
    /// (the collector, this node's vote round, a re-vote) stores its certificate. At most
    /// [`MAX_UNCERTIFIED_GAPS`]; empty with the walk off.
    gaps: BTreeSet<Epoch>,
    /// The gap the next [`retry_gaps`] asks for first (or the first gap after it): the gap after
    /// the one the previous retry stopped at, so the retry's deadline cannot keep one gap from
    /// ever being asked by sorting it last.
    retry_from: Epoch,
    /// Where the walk and conflict counters go.
    metrics: &'m StateSyncMetrics,
}

impl<'m> Collection<'m> {
    /// A collection with no gap open yet.
    fn new(switches: EpochRecordCollectSwitches, metrics: &'m StateSyncMetrics) -> Self {
        Self { switches, gaps: BTreeSet::new(), retry_from: 0, metrics }
    }

    /// Whether a pass told by a peer that it holds no certified record of `epoch` may go on to the
    /// next epoch, `walked` being the epochs this pass already walked past without a record after
    /// them landing yet.
    ///
    /// Only with the walk switched on, only while fewer than [`MAX_UNCERTIFIED_GAPS`] gaps would
    /// be open, and only when this node holds `epoch` as its own stored, uncertified record that
    /// is not the epoch-0 dummy anchor. The next epoch's record is then anchored on that record:
    /// its digest and its `next_committee` come from this node's own execution of the epoch, so a
    /// record past it is checked exactly as it would be had the epoch been certified. An
    /// uncertified record is stored by this node's own epoch close, or is a peer's record that
    /// validated with a super-quorum certificate whose own write then failed. A node without
    /// its own record of the epoch (a fresh node, an observer far behind) never walks: the
    /// epoch's `next_committee` is not constrained by anything such a node can check, so a
    /// fabricated record of the epoch could name the committee that certifies a fabricated next
    /// one.
    async fn may_walk_past(
        &self,
        epoch: Epoch,
        walked: &[Epoch],
        consensus_chain: &ConsensusChain,
    ) -> bool {
        if !self.switches.past_uncertified {
            return false;
        }
        let opening = walked.iter().filter(|walked| !self.gaps.contains(walked)).count();
        if !self.gaps.contains(&epoch) && self.gaps.len() + opening >= MAX_UNCERTIFIED_GAPS {
            return false;
        }
        let epochs = consensus_chain.epochs();
        match epochs.get_epoch_by_number(epoch).await {
            Some((_, None)) => epoch > 0 || !epochs.contains_dummy_epoch0().await,
            Some((_, Some(_))) | None => false,
        }
    }

    /// Open a gap for every epoch in `walked`, now that a certified record after them is stored,
    /// counting and logging each epoch not open already.
    fn land(&mut self, walked: &mut Vec<Epoch>) {
        for epoch in walked.drain(..) {
            if self.gaps.insert(epoch) {
                self.metrics.epoch_record_gap_walks_total.increment(1);
                info!(
                    target: "epoch-manager",
                    epoch,
                    gaps = self.gaps.len(),
                    "epoch record collector walking past an uncertified epoch",
                );
            }
        }
    }

    /// Handle a failed save of `peer`'s validated record of `epoch`.
    ///
    /// A [`EpochDbError::ConflictingRecord`] here is a certified record that validated against
    /// this node's record of the epoch before, with a super-quorum certificate of the committee
    /// that record hands over to, and that differs from the record this node stored for the epoch.
    /// That shows only that this many members of the committee signed another record than this
    /// node's: either this node's record is wrong or the signers are faulty, which the evidence
    /// cannot tell apart. No single peer can cause it: the certificate takes more than `f` members
    /// of the committee this node's own previous record names (see
    /// [`tn_types::EpochRecord::committee_compatible`]).
    ///
    /// A conflict is counted in `tn_state_sync.epoch_record_conflicts_total` and logged at error
    /// with what an operator needs to look into it (see [`ConflictEvidence`]): both digests, the
    /// peer that served the record (a relay, not necessarily a signer), the certificate's signers
    /// as authority ids, how many signed out of how many members of the committee the record
    /// before the epoch names, and the offered record's epoch, parent and final execution state.
    /// The line starts with `epoch record conflict:`. The conflict then goes to
    /// [`stop_on_local_failure`] like every other failed save, which stops the node on it: the
    /// error ends the record collector (and so the node) or fails startup, and it repeats on every
    /// restart, since the startup sync meets the same conflict. For every other failed save our own
    /// store failing stops the node and a refusal is logged.
    async fn refused(
        &self,
        e: EpochDbError,
        epoch: Epoch,
        peer: BlsPublicKey,
        (record, cert): (&EpochRecord, &EpochCertificate),
        consensus_chain: &ConsensusChain,
    ) -> eyre::Result<()> {
        if let EpochDbError::ConflictingRecord { stored, offered, .. } = &e {
            self.metrics.epoch_record_conflicts_total.increment(1);
            let evidence = ConflictEvidence::read(epoch, record, cert, consensus_chain).await;
            let signers = evidence.signer_ids.len();
            let committee = evidence.committee;
            error!(
                target: "epoch-manager",
                epoch,
                %stored,
                %offered,
                %peer,
                signers,
                committee,
                signer_ids = %AuthorityIds(&evidence.signer_ids),
                offered_epoch = record.epoch,
                offered_parent = %record.parent_hash,
                offered_final_number = record.final_state.number,
                offered_final_hash = %record.final_state.hash,
                "epoch record conflict: this node's stored record of epoch {epoch} differs from a \
                 record certified by {signers} of the {committee} members of the committee the \
                 record before it names; this node stops; do not delete its chain data before \
                 the cause is known",
            );
        }
        stop_on_local_failure(e, epoch)
    }
}

/// What a conflicting certified record shows about who certified it, read from this node's store
/// for the conflict's log line (see [`Collection::refused`]).
struct ConflictEvidence {
    /// The offered certificate's signers that belong to the committee the record before the epoch
    /// names, as authority ids in the offered record's committee order.
    signer_ids: Vec<AuthorityIdentifier>,
    /// Size of that committee: the previous record's `next_committee`, the committee validation
    /// anchors the epoch on (for epoch 0, the committee this node holds for epoch 0). Zero when
    /// the read fails.
    committee: usize,
}

impl ConflictEvidence {
    /// Read the anchor committee of `epoch` as `EpochRecordDb::validate_downloaded_record` does
    /// and count `cert`'s signers of `record` in it.
    async fn read(
        epoch: Epoch,
        record: &EpochRecord,
        cert: &EpochCertificate,
        consensus_chain: &ConsensusChain,
    ) -> Self {
        let epochs = consensus_chain.epochs();
        let anchor: BTreeSet<BlsPublicKey> = if epoch == 0 {
            epochs.get_committee_keys(0).await
        } else {
            epochs
                .record_by_epoch(epoch - 1)
                .await
                .map(|previous| previous.next_committee.into_iter().collect())
        }
        .unwrap_or_default();
        // the bitmap iterates in ascending order, so the ids follow the committee order
        let signer_ids = cert
            .signed_authorities
            .iter()
            .filter_map(|index| record.committee.get(index as usize))
            .filter(|key| anchor.contains(*key))
            .map(|key| AuthorityIdentifier::from(*key))
            .collect();
        Self { signer_ids, committee: anchor.len() }
    }
}

/// Log rendering of a list of authority ids: `[id1,id2]`, or `[]` when empty.
///
/// Comma-separated with no spaces and no quotes, so a `key=value` log parser that reads the value
/// up to the next whitespace captures the whole list.
struct AuthorityIds<'a>(&'a [AuthorityIdentifier]);

impl std::fmt::Display for AuthorityIds<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[")?;
        for (i, id) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{id}")?;
        }
        f.write_str("]")
    }
}

/// How one request for an epoch's certified record ended (see [`fetch_epoch_record`]).
#[derive(Debug)]
enum Fetched {
    /// The record and its certificate are stored; carries the record's final consensus header.
    Stored(ConsensusNumHash),
    /// A peer's record was refused: it failed validation, cannot be anchored yet, or the store
    /// declined it. `parent_mismatch` is true when the record names another parent than this
    /// node's record of the epoch before.
    Refused {
        /// The record failed validation on its parent hash.
        parent_mismatch: bool,
    },
    /// No peer returned a record: the error of [`PrimaryNetworkHandle::request_epoch_cert`].
    Missing(NetworkError),
}

/// Collect epoch records before the first epoch starts.
///
/// The caller must seed the trusted genesis committee before calling this function and start the
/// background record collector afterward. Records use the same locally anchored validation as
/// background collection. Peer readiness is polled until a connection is established. Network
/// failures never fail startup: two consecutive definitive passes ending at the same epoch, a
/// capped pass without certified-prefix progress, shutdown, or a thirty-second deadline stop
/// collection. A pass is definitive when it ends on a peer's answer, normally "I do not have that
/// epoch" at the network's tip. A pass that no peer answered (a dropped request, an unresolved
/// peer, no peer at all) is repeated after a short pause and does not count toward quiescence
/// until no peer has answered for [`STARTUP_SYNC_UNANSWERED_BUDGET`]: two such failures must not
/// read as "nothing newer exists", but a network starting all at once cannot answer anyone until
/// its nodes finish this sync themselves. The deadline includes the peer wait.
/// Each pass has a five-second cap, so connected peers that cannot yet serve certificates do not
/// consume the full deadline. Our own epoch store failing to save or persist a record does fail
/// startup, and so does a certified record that conflicts with this node's own (see
/// [`collect_epoch_records`]).
pub async fn sync_epoch_records_to_tip(
    consensus_chain: &ConsensusChain,
    primary_handle: &PrimaryNetworkHandle,
    consensus_bus: &ConsensusBusApp,
    node_shutdown: Noticer,
    parameters: &Parameters,
) -> eyre::Result<()> {
    sync_epoch_records_to_tip_with(
        consensus_chain,
        primary_handle,
        consensus_bus,
        node_shutdown,
        parameters.into(),
        &STATE_SYNC_METRICS,
    )
    .await
}

/// [`sync_epoch_records_to_tip`], with `switches` and reporting to `metrics`.
///
/// The certified prefix is read from the store before the first pass, so a restarted node
/// reports the certificates it already holds before any peer answers, and again around every
/// pass (see [`certified_prefix_end`]). Every pass walks with a [`Collection`] of its own: gaps
/// it walks past are not asked for again here, the record collector does that once it runs.
async fn sync_epoch_records_to_tip_with(
    consensus_chain: &ConsensusChain,
    primary_handle: &PrimaryNetworkHandle,
    consensus_bus: &ConsensusBusApp,
    node_shutdown: Noticer,
    switches: EpochRecordCollectSwitches,
    metrics: &StateSyncMetrics,
) -> eyre::Result<()> {
    certified_prefix_end(consensus_chain, metrics).await;
    drive_epoch_record_sync(
        || async { primary_handle.inner_handle().established_peer_count().await.unwrap_or(0) },
        |epoch| async move {
            let mut collection = Collection::new(switches, metrics);
            collect_epoch_records(
                epoch,
                consensus_chain,
                primary_handle,
                consensus_bus,
                &mut collection,
            )
            .await
        },
        // This cached scan also detects certificate backfill below the latest stored record,
        // including replacement of the dummy epoch 0 with a certified record.
        || certified_prefix_end(consensus_chain, metrics),
        node_shutdown,
    )
    .await
}

/// Drive startup collection with independently bounded passes and a process-wide stop condition.
///
/// `progress` returns the first epoch without a certificate, so a capped pass continues only when
/// it extended the certified prefix. A pass that no peer answered waits
/// [`STARTUP_SYNC_RETRY_BACKOFF`] and resets the quiescence comparison, until
/// [`STARTUP_SYNC_UNANSWERED_BUDGET`] of silence makes such passes count. A pass that fails (our
/// own store failing) ends the sync with its error. Separate scheduling from transport to exercise
/// repair, stalled peers, unanswered requests, and shutdown deterministically.
async fn drive_epoch_record_sync<Peers, Collect, Progress>(
    peer_count: impl FnMut() -> Peers,
    collect: impl FnMut(Epoch) -> Collect,
    progress: impl FnMut() -> Progress,
    shutdown: impl Future<Output = ()>,
) -> eyre::Result<()>
where
    Peers: Future<Output = usize>,
    Collect: Future<Output = eyre::Result<CollectedPass>>,
    Progress: Future<Output = Epoch>,
{
    let sync = async {
        // Await each borrowed peer query inside the polling future. Passing it through
        // `then(identity)` prevents `Send` inference for node startup on Rust 1.94.
        futures::stream::unfold(peer_count, |mut peer_count| async move {
            Some((peer_count().await, peer_count))
        })
        .take_while(|peers| ready(*peers == 0))
        .for_each(|_| tokio::time::sleep(STARTUP_PEER_POLL_INTERVAL))
        .await;

        futures::stream::try_unfold(
            StartupSyncState {
                last_epoch: 0,
                previous_pass: None,
                unanswered_since: None,
                collect,
                progress,
            },
            |mut state| async move {
                let before = (state.progress)().await;
                let collected = tokio::select! {
                    biased;
                    _ = tokio::time::sleep(STARTUP_SYNC_PASS_TIMEOUT) => None,
                    pass = (state.collect)(state.last_epoch) => Some(pass?),
                };
                // Only a pass that ended on a peer's answer says where the network's records end,
                // until no peer has answered for the whole unanswered budget: then the silence is
                // the answer, as it is at genesis and in a whole-network restart.
                // A backward step repairs an anchor; only equality establishes quiescence.
                let unanswered = collected.is_some_and(|pass| !pass.definitive);
                let now = tokio::time::Instant::now();
                let since = state.unanswered_since;
                let budget_spent = unanswered
                    && since.is_some_and(|since| {
                        now.duration_since(since) >= STARTUP_SYNC_UNANSWERED_BUDGET
                    });
                state.unanswered_since = unanswered.then(|| since.unwrap_or(now));
                if budget_spent && state.previous_pass.is_none() {
                    info!(
                        target: "epoch-manager",
                        budget = ?STARTUP_SYNC_UNANSWERED_BUDGET,
                        "no peer answered the startup epoch record requests for the whole budget; \
                         taking the local records as the newest (the peers may be starting too)",
                    );
                }
                let definitive =
                    collected.filter(|pass| pass.definitive || budget_spent).map(|pass| pass.epoch);
                let quiescent = definitive.is_some_and(|epoch| state.previous_pass == Some(epoch));
                let after = (state.progress)().await;
                let stalled = collected.is_none() && after == before;
                state.last_epoch =
                    collected.map_or_else(|| after.saturating_sub(1), |pass| pass.epoch);
                state.previous_pass = definitive;
                if unanswered && !budget_spent {
                    // no peer answered: ask again shortly, inside the outer deadline
                    tokio::time::sleep(STARTUP_SYNC_RETRY_BACKOFF).await;
                } else {
                    // Keep the outer deadline and shutdown responsive for immediately ready passes.
                    tokio::task::yield_now().await;
                }
                Ok((!quiescent && !stalled).then_some(((), state)))
            },
        )
        .try_for_each(|()| ready(Ok(())))
        .await
    };
    tokio::select! {
        biased;
        _ = shutdown => Ok(()),
        _ = tokio::time::sleep(STARTUP_SYNC_TIMEOUT) => {
            info!(target: "epoch-manager", "startup epoch record sync reached its deadline");
            Ok(())
        },
        result = sync => result,
    }
}

/// The node's count of epoch records that contradict its own execution (`EpochMetrics` in
/// `tn-node`, `tn_epoch_final_state_mismatches_total` in Prometheus). Every count stops the node.
const FINAL_STATE_MISMATCHES: &str = "tn_epoch.final_state_mismatches_total";

/// Split a failed save of a downloaded epoch record ([`EpochDbError::stop_on_local_failure`]):
/// our own store failing is returned, so the collection fails and the node stops; a refused
/// record, which a peer can send, is only logged, and the caller asks again later.
///
/// A certified record (it passed `validate_downloaded_record`) refused because this node holds a
/// different record for the epoch (`ConflictingRecord`) is returned too: the stored record is this
/// node's own uncertified one, built from its execution (the collector skips epochs whose record
/// is already certified), so this node's execution diverged from the one a quorum certified. It is
/// counted in `tn_epoch_final_state_mismatches_total` and the node stops, as a committee validator
/// does at the epoch vote: an observer or a node outside the committee would otherwise run on, and
/// serve, a chain no quorum certified. A restart stops again, as the collector asks again.
fn stop_on_local_failure(e: EpochDbError, epoch: Epoch) -> eyre::Result<()> {
    if matches!(e, EpochDbError::ConflictingRecord { .. }) {
        ::metrics::counter!(FINAL_STATE_MISMATCHES).increment(1);
        error!(
            target: "epoch-manager",
            epoch,
            e = %e,
            "a certified epoch record from peers conflicts with this node's own record for the \
             epoch: this node's execution diverged from the certified chain, stopping the node",
        );
        return Err(eyre::Report::new(e).wrap_err(format!(
            "epoch {epoch}: a quorum certified an epoch record other than the one this node built \
             from its own execution, so this node's execution diverged - stopping the node rather \
             than run on a chain no quorum certified (do NOT delete chain-data: keep the datadir to \
             find the first block that differs)"
        )));
    }
    e.stop_on_local_failure(format!("failed to save epoch record/cert for epoch {epoch}"))
}

/// Asks peers for records from last_epoch to requested_epoch.
/// Returns the Epoch that was last retrieved, and whether the pass ended on a peer's answer (see
/// [`CollectedPass`]).
///
/// Fails when our own epoch store fails to save or persist, or when a certified record conflicts
/// with this node's own (see [`stop_on_local_failure`]); a peer's invalid or otherwise refused
/// record is logged and asked for again on a later pass. Each
/// certified record stored is published on [`ConsensusBusApp::certified_record_epoch`], so an
/// active validator still running an epoch the network already closed leaves it to catch up.
///
/// With [`Parameters::epoch_record_collect_past_uncertified`] on, a pass told by a peer that it
/// holds no certified record of an epoch this node holds as its own uncertified record goes on to
/// the next epoch (see [`Collection::may_walk_past`]). Once a certified record after it is stored,
/// the epoch is a gap of `collection`, which the record collector asks for again
/// ([`retry_gaps`]). If the walk ends before that, on a record naming another parent (this node's
/// own record differs from the one the network built on), a peer without the next record or no
/// answer, the pass stops where it would have stopped with the walk off. A pass that walked always
/// ends with the persist and the final-consensus publish below, a refused record included.
async fn collect_epoch_records(
    last_epoch: Epoch,
    consensus_chain: &ConsensusChain,
    primary_handle: &PrimaryNetworkHandle,
    consensus_bus: &ConsensusBusApp,
    collection: &mut Collection<'_>,
) -> eyre::Result<CollectedPass> {
    let mut result_epoch = last_epoch;
    // the last failed request decides; a record that ends the pass early is a peer's answer
    let mut definitive = true;
    // Track the highest final_consensus seen across all downloaded epoch records.
    // We emit a single state-sync notification at the end rather than one per epoch,
    // to avoid flooding peers with concurrent request_consensus calls during catch-up.
    let mut best_final_consensus: Option<(Epoch, u64, ConsensusHeaderDigest)> = None;
    // epochs this pass walked past that no stored certified record after them confirms yet
    let mut walked: Vec<Epoch> = Vec::new();
    // whether this pass walked at all: such a pass ends at the persist below, whatever stops it
    let mut walked_any = false;
    for epoch in last_epoch.. {
        // If we already have epoch record AND it's certificate then continue.
        if let Some((rec, Some(_))) = consensus_chain.epochs().get_epoch_by_number(epoch).await {
            // Advance result_epoch so we correctly track the last confirmed epoch,
            // allowing last_epoch to advance past already-complete epochs on future calls.
            result_epoch = epoch;
            collection.land(&mut walked);

            let number = rec.final_consensus.number;
            let hash = rec.final_consensus.hash;
            // Don't know the output bytes so use 0.  If we could get them (already have the output)
            // we won't need them...
            // A certified record may also replace a gossip pin the record proves impossible
            // (issue #1771).
            if consensus_bus.publish_certified_consensus_num_hash(epoch, number, hash) {
                debug!(
                    target: "epoch-manager",
                    "epoch record sync downloaded up to epoch {result_epoch}, final consensus at block {number} ({hash}) - notifying state sync",
                );
            }

            continue;
        }
        // Try to recover by downloading the epoch record and cert from a peer.
        match fetch_epoch_record(epoch, consensus_chain, primary_handle, consensus_bus, collection)
            .await?
        {
            Fetched::Stored(final_consensus) => {
                result_epoch = epoch;
                collection.land(&mut walked);
                // Track the highest final_consensus across downloaded epochs.
                if final_consensus.hash != ConsensusHeaderDigest::default()
                    && final_consensus.number > best_final_consensus.map(|(_, n, _)| n).unwrap_or(0)
                {
                    best_final_consensus =
                        Some((epoch, final_consensus.number, final_consensus.hash));
                }
            }
            Fetched::Refused { parent_mismatch } => {
                if !walked_any {
                    return Ok(CollectedPass { epoch: epoch.saturating_sub(1), definitive: true });
                }
                if parent_mismatch && !walked.is_empty() {
                    // the record after a walked epoch names another parent: this node's own
                    // record of that epoch differs from the one the network built on
                    collection.metrics.epoch_record_walk_parent_mismatches_total.increment(1);
                }
                // the cursor stays where it is: before a walk that did not land, where a pass with
                // the walk off stops, so the next pass meets the walked epoch again rather than
                // starting on it; after one that landed, on the last stored epoch. The pass ends
                // below, so what it stored is persisted.
                definitive = true;
                break;
            }
            Fetched::Missing(err) => {
                // We delibrately go past the latest epoch so this is expected to happen. Only a
                // peer saying it does not hold the epoch shows the end of the records; any other
                // failure means no peer answered (see `request_epoch_cert`).
                definitive = matches!(err, NetworkError::RPCError(_));
                info!(
                    target: "epoch-manager",
                    definitive,
                    "failed to retrieve epoch from a peer {epoch}: {err}",
                );
                if definitive && collection.may_walk_past(epoch, &walked, consensus_chain).await {
                    walked.push(epoch);
                    walked_any = true;
                    continue;
                }
            }
        }
        if result_epoch != epoch {
            break;
        }
    }
    // A persist writes only our own files, so any failure is local.
    consensus_chain
        .epochs()
        .persist()
        .await
        .wrap_err("failed to persist downloaded epoch record/certs")?;
    // Emit a single state-sync notification with the highest epoch's final consensus.
    // This unblocks nodes that missed a ConsensusResult gossip message due to a timing
    // gap (e.g. gossip arrived before the epoch record was available). We do this once
    // at the end rather than per-epoch to avoid flooding peers with concurrent requests.
    // The certified publish also replaces a gossip pin that the certified record proves
    // impossible, so a node pinned by bogus f+1 signatures heals here (issue #1771).
    if let Some((epoch, number, hash)) = best_final_consensus {
        // Don't know the output bytes so use 0.  If we could get them (already have the output) we
        // won't need them...
        if consensus_bus.publish_certified_consensus_num_hash(epoch, number, hash) {
            info!(
                target: "epoch-manager",
                "updating last published consensus num hash up to epoch {result_epoch}, final consensus at block {number} ({hash}) - notifying state sync",
            );
        }
    }
    Ok(CollectedPass { epoch: result_epoch, definitive })
}

/// Ask a peer for `epoch`'s record and certificate, then [`store_epoch_record`] the answer.
async fn fetch_epoch_record(
    epoch: Epoch,
    consensus_chain: &ConsensusChain,
    primary_handle: &PrimaryNetworkHandle,
    consensus_bus: &ConsensusBusApp,
    collection: &Collection<'_>,
) -> eyre::Result<Fetched> {
    let answer = primary_handle.request_epoch_cert(Some(epoch), None).await;
    store_epoch_record(epoch, answer, consensus_chain, consensus_bus, collection).await
}

/// Validate a peer's `answer` to a request for `epoch`'s record and certificate against this
/// node's anchor and store them.
///
/// Fails only where [`Collection::refused`] does: our own store failing, or a certified record
/// that conflicts with this node's own. A stored pair is published on
/// [`ConsensusBusApp::certified_record_epoch`].
async fn store_epoch_record(
    epoch: Epoch,
    answer: Result<(EpochRecord, EpochCertificate, BlsPublicKey), NetworkError>,
    consensus_chain: &ConsensusChain,
    consensus_bus: &ConsensusBusApp,
    collection: &Collection<'_>,
) -> eyre::Result<Fetched> {
    let (epoch_rec, cert, peer) = match answer {
        Ok(answer) => answer,
        Err(err) => return Ok(Fetched::Missing(err)),
    };
    // Validate the downloaded record against the locally-trusted committee using the same routine
    // the failed-quorum recovery path uses (see crates/node epoch_votes.rs), so a record cannot be
    // accepted under weaker rules on one path than the other.
    match consensus_chain.epochs().validate_downloaded_record(epoch, &epoch_rec, &cert).await {
        EpochRecordValidation::Valid => {
            let epoch_hash = epoch_rec.digest();
            // Capture final_consensus before save consumes epoch_rec.
            let final_consensus = epoch_rec.final_consensus;
            let signers = cert.signed_authorities.len();
            // the pair stays at hand for a conflict's log line once save consumes it
            if let Err(e) = consensus_chain.epochs().save(epoch_rec.clone(), cert.clone()).await {
                collection.refused(e, epoch, peer, (&epoch_rec, &cert), consensus_chain).await?;
                return Ok(Fetched::Refused { parent_mismatch: false });
            }
            consensus_bus.set_certified_record_epoch_if_newer(epoch);
            info!(
                target: "epoch-manager",
                %peer,
                signers,
                "retrieved cert for epoch {epoch}: {epoch_hash} from a peer",
            );
            Ok(Fetched::Stored(final_consensus))
        }
        EpochRecordValidation::Invalid {
            epoch_matches,
            parents_match,
            committee_valid,
            cert_valid,
        } => {
            error!(
                target: "epoch-manager",
                ?epoch_matches,
                ?parents_match,
                ?committee_valid,
                ?cert_valid,
                %peer,
                "got an invalid epoch record, epoch {epoch}",
            );
            Ok(Fetched::Refused { parent_mismatch: !parents_match })
        }
        // We are missing the previous epoch record (or the genesis committee), so this record
        // cannot be anchored. The caller retries once the anchor is available.
        EpochRecordValidation::NoAnchor => Ok(Fetched::Refused { parent_mismatch: false }),
    }
}

/// Ask once for the certified record of every gap of `collection`, the record collector's retry
/// (see [`spawn_epoch_record_collector_with`]).
///
/// Each gap's request is one `request_epoch_cert`, which asks up to three peers in turn. The retry
/// stops at [`GAP_RETRY_DEADLINE`] or when `node_shutdown` fires, mid-request too (only the wait
/// for peers is cut short, never a validation or a save), and the gaps not asked wait for the next
/// retry. A retry starts at [`Collection::retry_from`] and wraps around, so every open gap is
/// asked for at least once in as many retries as there are gaps, however slow the peers are on
/// the others.
///
/// A gap whose certificate is stored already, by whatever path, is dropped without a request; so
/// is one whose certificate this request stores, and the store is persisted then. Only a stored
/// certificate closes a gap: a read of the gap's epoch that fails (the store reports it as absent)
/// keeps the gap, unasked, for the next retry. Fails where [`store_epoch_record`] does, and when
/// that persist fails.
async fn retry_gaps(
    consensus_chain: &ConsensusChain,
    primary_handle: &PrimaryNetworkHandle,
    consensus_bus: &ConsensusBusApp,
    collection: &mut Collection<'_>,
    node_shutdown: &Noticer,
) -> eyre::Result<()> {
    let deadline = tokio::time::Instant::now() + GAP_RETRY_DEADLINE;
    let from = collection.retry_from;
    let open: Vec<Epoch> =
        collection.gaps.range(from..).chain(collection.gaps.range(..from)).copied().collect();
    let mut stored = false;
    for epoch in open {
        match consensus_chain.epochs().get_epoch_by_number(epoch).await {
            Some((_, Some(_))) => {
                collection.gaps.remove(&epoch);
                continue;
            }
            Some((_, None)) => {}
            // the read failed: the record was stored when the gap opened and is never removed
            None => continue,
        }
        let answer = tokio::select! {
            biased;
            _ = node_shutdown => None,
            _ = tokio::time::sleep_until(deadline) => None,
            answer = primary_handle.request_epoch_cert(Some(epoch), None) => Some(answer),
        };
        let Some(answer) = answer else {
            // the next retry starts after the gap this one stopped at
            collection.retry_from = epoch.saturating_add(1);
            break;
        };
        let closed = match store_epoch_record(
            epoch,
            answer,
            consensus_chain,
            consensus_bus,
            collection,
        )
        .await?
        {
            Fetched::Stored(_) => {
                stored = true;
                true
            }
            Fetched::Refused { .. } => false,
            Fetched::Missing(err) => {
                debug!(
                    target: "epoch-manager",
                    epoch,
                    "no peer returned the certified record of an uncertified epoch walked past: {err}",
                );
                false
            }
        };
        if closed {
            collection.gaps.remove(&epoch);
        }
    }
    if stored {
        // A persist writes only our own files, so any failure is local.
        consensus_chain
            .epochs()
            .persist()
            .await
            .wrap_err("failed to persist downloaded epoch record/certs")?;
    }
    Ok(())
}

/// Spawn a long running task to collect missing epoch records.
///
/// Most likely because a node is syncing. Our own epoch store failing to save or persist ends
/// this critical task with the error, so the node exits, and so does a certified record that
/// conflicts with this node's own (see [`collect_epoch_records`]).
pub async fn spawn_epoch_record_collector(
    consensus_chain: ConsensusChain,
    primary_handle: PrimaryNetworkHandle,
    consensus_bus: ConsensusBusApp,
    node_task_spawner: TaskSpawner,
    node_shutdown: Noticer,
    parameters: &Parameters,
) -> eyre::Result<()> {
    spawn_epoch_record_collector_with(
        consensus_chain,
        primary_handle,
        consensus_bus,
        node_task_spawner,
        node_shutdown,
        parameters.into(),
        STATE_SYNC_METRICS.clone(),
    )
    .await
}

/// [`spawn_epoch_record_collector`], with `switches` and reporting to `metrics`.
///
/// After every pass the certified prefix is read from the store, not from the pass, so a
/// certificate stored by another path (this node's own vote round, the startup sync) shows on the
/// next wake-up too (see [`certified_prefix_end`]). The collector keeps one [`Collection`] for its
/// life. Since a pass starts at the last collected epoch, the gaps a pass walked past are asked for
/// only by [`retry_gaps`], which runs after a pass that ended on a peer's answer, at most once per
/// retry interval however often the collector wakes: one `request_epoch_cert` per open gap (each
/// asks up to three peers in turn), after the pass and under [`GAP_RETRY_DEADLINE`] and the
/// shutdown signal, so the next pass's work (the certified-record and republish signals), the
/// gauge and the shutdown select wait at most one retry interval on it.
async fn spawn_epoch_record_collector_with(
    consensus_chain: ConsensusChain,
    primary_handle: PrimaryNetworkHandle,
    consensus_bus: ConsensusBusApp,
    node_task_spawner: TaskSpawner,
    node_shutdown: Noticer,
    switches: EpochRecordCollectSwitches,
    metrics: StateSyncMetrics,
) -> eyre::Result<()> {
    let mut epoch_rx = consensus_bus.requested_missing_epoch().subscribe();
    node_task_spawner.spawn_critical_task("Epoch Record Collector", async move {
        // Always start from epoch 0 so any gaps (epochs whose certs were never
        // saved, e.g. because the node was killed during a failed-quorum recovery)
        // are back-filled on restart.  Epochs that already have both a record and
        // a certificate are skipped immediately by get_epoch_by_number, so this
        // is cheap for nodes that are fully caught up.
        let mut last_epoch: Epoch = 0;
        let mut collection = Collection::new(switches, &metrics);
        let retry_interval = Duration::from_secs(EPOCH_COLLECT_RETRY_SECS);
        let mut gaps_retried_at = tokio::time::Instant::now();
        loop {
            // Every wake-up runs a pass, whether the requested epoch moved or the retry timer
            // fired. The pass starts at the last certified epoch, so on a caught-up node it costs
            // a few store reads and one peer request, and it republishes each certified record it
            // walks over. That republish is the heal for a last-published watch pinned by f+1
            // gossip signatures (issues #1771 and #1796), so it must not wait on a requested epoch
            // that can sit below the cursor. An unanswered pass needs no special handling here:
            // the retry timer below asks again. The borrow clears the change flag.
            let _ = epoch_rx.borrow_and_update();
            let pass = collect_epoch_records(
                last_epoch,
                &consensus_chain,
                &primary_handle,
                &consensus_bus,
                &mut collection,
            )
            .await
            .map_err(|e| TaskError::from_message(format!("{e:#}")))?;
            last_epoch = pass.epoch;
            // no peer answered the pass: the gaps would go unanswered too
            if pass.definitive
                && !collection.gaps.is_empty()
                && gaps_retried_at.elapsed() >= retry_interval
            {
                retry_gaps(
                    &consensus_chain,
                    &primary_handle,
                    &consensus_bus,
                    &mut collection,
                    &node_shutdown,
                )
                .await
                .map_err(|e| TaskError::from_message(format!("{e:#}")))?;
                gaps_retried_at = tokio::time::Instant::now();
            }
            certified_prefix_end(&consensus_chain, &metrics).await;
            // Wait until the watch is updated or a retry timer fires.
            // The retry timer ensures that a failed collection attempt (e.g. peers not yet
            // connected at startup) is re-attempted automatically without requiring a new
            // watch notification.
            tokio::select!(
                _ = &node_shutdown => {
                    break Ok(());  // Break the outer loop.
                },
                _ = epoch_rx.changed() => { }
                _ = tokio::time::sleep(Duration::from_secs(EPOCH_COLLECT_RETRY_SECS)) => { }
            );
        }
    });
    Ok(())
}

/// Read the end of the certified prefix of stored epoch records and set
/// `tn_state_sync.epoch_records_certified_through` from it.
///
/// Returns the first epoch without a stored record and certificate
/// (`EpochRecordDb::first_missing_historical_cert` over every epoch, cheap: the store resumes from
/// its watermark); the gauge is that epoch minus one, `-1` when epoch 0 has no certificate. A dead
/// store answers epoch 0 as missing, so its answer leaves the gauge at the last real reading.
async fn certified_prefix_end(
    consensus_chain: &ConsensusChain,
    metrics: &StateSyncMetrics,
) -> Epoch {
    let epochs = consensus_chain.epochs();
    let first_missing =
        epochs.first_missing_historical_cert(Epoch::MAX).await.unwrap_or(Epoch::MAX);
    if epochs.is_alive() {
        metrics.epoch_records_certified_through.set(f64::from(first_missing) - 1.0);
    }
    first_missing
}

#[cfg(test)]
#[path = "epoch/startup_tests.rs"]
mod startup_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use roaring::RoaringBitmap;
    use std::{collections::HashMap, num::NonZeroUsize};
    use tn_network_libp2p::types::{NetworkCommand, NetworkResponseMessage};
    use tn_primary::{
        network::{PrimaryRequest, PrimaryResponse},
        ConsensusBus,
    };
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils_committee::CommitteeFixture;
    use tn_types::{
        encode, BlsAggregateSignature, BlsKeypair, BlsPublicKey, BlsSignature, EpochCertificate,
        EpochRecord, Intent, IntentMessage, IntentScope, Signer as _,
    };
    use tokio::sync::mpsc;

    /// Aggregate an [EpochCertificate] for `record` signed by the committee
    /// members at `signer_idx` (positions into `signers`, which parallels
    /// `record.committee`).
    fn make_cert(
        record: &EpochRecord,
        signers: &[&BlsKeypair],
        signer_idx: &[u32],
    ) -> EpochCertificate {
        let epoch_hash = record.digest();
        let intent =
            encode(&IntentMessage::new(Intent::consensus(IntentScope::EpochBoundary), epoch_hash));
        let sigs: Vec<BlsSignature> =
            signer_idx.iter().map(|i| signers[*i as usize].sign(&intent)).collect();
        let signature =
            BlsAggregateSignature::aggregate(&sigs[..], true).expect("aggregate").to_signature();
        let mut signed_authorities = RoaringBitmap::new();
        for i in signer_idx {
            signed_authorities.push(*i);
        }
        EpochCertificate { epoch_hash, signature, signed_authorities }
    }

    /// Spawn a mock peer network serving `(record, cert)` pairs by epoch.
    /// Requests for epochs not in `served` get the reply a peer sends when it does not hold the
    /// record, as at the network's tip.
    fn mock_epoch_record_network(
        served: HashMap<Epoch, (EpochRecord, EpochCertificate)>,
        peer: BlsPublicKey,
        established_peers: usize,
    ) -> PrimaryNetworkHandle {
        mock_epoch_record_network_with(served, peer, established_peers, true)
    }

    /// [`mock_epoch_record_network`], where `answer_unserved == false` drops the reply channel of a
    /// request for an epoch not in `served`, as a peer between epochs drops inbound requests.
    fn mock_epoch_record_network_with(
        served: HashMap<Epoch, (EpochRecord, EpochCertificate)>,
        peer: BlsPublicKey,
        established_peers: usize,
        answer_unserved: bool,
    ) -> PrimaryNetworkHandle {
        let (tx, mut rx) = mpsc::channel(100);
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                if let NetworkCommand::EstablishedPeerCount { reply } = cmd {
                    let _ = reply.send(established_peers);
                } else if let NetworkCommand::SendRequestAny { request, reply } = cmd {
                    let PrimaryRequest::EpochRecord { epoch: Some(epoch), .. } = request else {
                        continue;
                    };
                    if let Some((record, certificate)) = served.get(&epoch) {
                        let _ = reply.send(Ok(NetworkResponseMessage {
                            peer,
                            result: PrimaryResponse::EpochRecord {
                                record: record.clone(),
                                certificate: certificate.clone(),
                            },
                        }));
                    } else if answer_unserved {
                        let _ = reply.send(Ok(NetworkResponseMessage {
                            peer,
                            result: PrimaryResponse::unavailable_epoch_record(Some(epoch), None),
                        }));
                    }
                }
            }
        });
        PrimaryNetworkHandle::new_for_test(tx)
    }

    #[tokio::test]
    async fn test_collect_epoch_records_across_ejection_epoch() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(NonZeroUsize::new(5).expect("5 > 0"))
            .build();
        let auths: Vec<_> = fixture.authorities().collect();
        let keys: Vec<BlsPublicKey> = auths.iter().map(|a| a.primary_public_key()).collect();
        let signers: Vec<&BlsKeypair> = auths.iter().map(|a| a.keypair()).collect();

        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .expect("consensus chain");

        // The local node already holds the sealed genesis record + cert.
        let rec0 = EpochRecord {
            epoch: 0,
            committee: keys.clone(),
            next_committee: keys.clone(),
            ..Default::default()
        };
        let cert0 = make_cert(&rec0, &signers, &[0, 1, 2, 3]);
        assert!(rec0.verify_with_cert(&cert0), "genesis cert must verify");
        consensus_chain.epochs().save(rec0.clone(), cert0).await.expect("save rec0");

        // Epoch 1 lost member 2 to a mid-epoch on-chain ejection (swap-and-pop),
        // so its record carries the shrunken committee and a 3-of-4 quorum cert.
        let shrunken = vec![keys[0], keys[1], keys[4], keys[3]];
        let shrunken_signers = vec![signers[0], signers[1], signers[4], signers[3]];
        let rec1 = EpochRecord {
            epoch: 1,
            committee: shrunken.clone(),
            next_committee: shrunken.clone(),
            parent_hash: rec0.digest(),
            ..Default::default()
        };
        let cert1 = make_cert(&rec1, &shrunken_signers, &[0, 1, 2]);
        assert!(rec1.verify_with_cert(&cert1), "3-of-4 cert must verify");
        let rec2 = EpochRecord {
            epoch: 2,
            committee: shrunken.clone(),
            next_committee: shrunken.clone(),
            parent_hash: rec1.digest(),
            ..Default::default()
        };
        let cert2 = make_cert(&rec2, &shrunken_signers, &[0, 1, 3]);

        let served =
            HashMap::from([(1, (rec1.clone(), cert1.clone())), (2, (rec2.clone(), cert2.clone()))]);
        let handle = mock_epoch_record_network(served, keys[0], 1);
        let consensus_bus = ConsensusBus::new();

        let collected = collect_epoch_records(
            0,
            &consensus_chain,
            &handle,
            consensus_bus.app(),
            &mut walk_off(),
        )
        .await
        .expect("no local store failure");
        assert_eq!(
            collected,
            CollectedPass { epoch: 2, definitive: true },
            "collector should fetch through the ejection epoch"
        );
        // the epoch manager is woken with the newest certified record stored
        assert_eq!(*consensus_bus.app().certified_record_epoch().borrow(), Some(2));

        // Both records validated (parent link + ejection tolerance + shrunken
        // quorum together) and were saved with their certs.
        let (saved1, saved_cert1) =
            consensus_chain.epochs().get_epoch_by_number(1).await.expect("epoch 1 saved");
        assert_eq!(saved1, rec1);
        assert_eq!(saved_cert1.expect("cert 1 saved"), cert1);
        let (saved2, saved_cert2) =
            consensus_chain.epochs().get_epoch_by_number(2).await.expect("epoch 2 saved");
        assert_eq!(saved2, rec2);
        assert_eq!(saved_cert2.expect("cert 2 saved"), cert2);
    }

    #[tokio::test]
    async fn test_collect_epoch_records_rejects_below_tolerance() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(NonZeroUsize::new(5).expect("5 > 0"))
            .build();
        let auths: Vec<_> = fixture.authorities().collect();
        let keys: Vec<BlsPublicKey> = auths.iter().map(|a| a.primary_public_key()).collect();
        let signers: Vec<&BlsKeypair> = auths.iter().map(|a| a.keypair()).collect();

        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .expect("consensus chain");

        let rec0 = EpochRecord {
            epoch: 0,
            committee: keys.clone(),
            next_committee: keys.clone(),
            ..Default::default()
        };
        let cert0 = make_cert(&rec0, &signers, &[0, 1, 2, 3]);
        consensus_chain.epochs().save(rec0.clone(), cert0).await.expect("save rec0");

        // A record claiming 3 of the 5 expected members were ejected at once:
        // its own 2-of-2 cert verifies, but the committee is below the
        // 4-member tolerance floor so the record must be rejected.
        let rec1_bad = EpochRecord {
            epoch: 1,
            committee: vec![keys[0], keys[1]],
            next_committee: vec![keys[0], keys[1]],
            parent_hash: rec0.digest(),
            ..Default::default()
        };
        let cert1_bad = make_cert(&rec1_bad, &[signers[0], signers[1]], &[0, 1]);
        assert!(rec1_bad.verify_with_cert(&cert1_bad), "cert alone verifies");

        let served = HashMap::from([(1, (rec1_bad.clone(), cert1_bad))]);
        let handle = mock_epoch_record_network(served, keys[0], 1);
        let consensus_bus = ConsensusBus::new();

        let collected = collect_epoch_records(
            0,
            &consensus_chain,
            &handle,
            consensus_bus.app(),
            &mut walk_off(),
        )
        .await
        .expect("an invalid peer record is not a local failure");
        assert_eq!(
            collected,
            CollectedPass { epoch: 0, definitive: true },
            "collector must stop at the invalid record"
        );

        // The rejected record was never saved.
        assert!(consensus_chain.epochs().get_epoch_by_number(1).await.is_none());
    }

    /// A certified record from peers that conflicts with our own uncertified record is refused by
    /// the store (`ConflictingRecord`): our execution diverged from the one a quorum certified, so
    /// the collection fails, which stops the node, and on every pass after a restart too. Our
    /// record stays.
    #[tokio::test]
    async fn test_collect_epoch_records_stops_on_a_record_conflicting_with_our_own() {
        let fixture = CommitteeFixture::builder(MemDatabase::default)
            .committee_size(NonZeroUsize::new(4).expect("4 > 0"))
            .build();
        let auths: Vec<_> = fixture.authorities().collect();
        let keys: Vec<BlsPublicKey> = auths.iter().map(|a| a.primary_public_key()).collect();
        let signers: Vec<&BlsKeypair> = auths.iter().map(|a| a.keypair()).collect();

        let temp_dir = tempfile::TempDir::new().expect("temp dir");
        let consensus_chain =
            ConsensusChain::new_for_test(temp_dir.path().to_owned(), fixture.committee())
                .await
                .expect("consensus chain");

        let rec0 = EpochRecord {
            epoch: 0,
            committee: keys.clone(),
            next_committee: keys.clone(),
            ..Default::default()
        };
        let cert0 = make_cert(&rec0, &signers, &[0, 1, 2]);
        consensus_chain.epochs().save(rec0.clone(), cert0).await.expect("save rec0");
        // Our own epoch 1 record, still uncertified.
        let ours = EpochRecord {
            epoch: 1,
            committee: keys.clone(),
            next_committee: keys.clone(),
            parent_hash: rec0.digest(),
            ..Default::default()
        };
        consensus_chain.epochs().save_record(ours.clone()).await.expect("save our record");

        // The peer's epoch 1 record anchors and verifies but is not ours.
        let mut theirs = ours.clone();
        theirs.final_state.number = 7;
        let cert = make_cert(&theirs, &signers, &[0, 1, 2]);
        let handle = mock_epoch_record_network(HashMap::from([(1, (theirs, cert))]), keys[0], 1);
        let consensus_bus = ConsensusBus::new();

        for _ in 0..2 {
            let err = collect_epoch_records(
                0,
                &consensus_chain,
                &handle,
                consensus_bus.app(),
                &mut walk_off(),
            )
            .await
            .expect_err("a certified record conflicting with ours stops the collector");
            assert!(format!("{err:#}").contains("epoch 1"), "unexpected error: {err:#}");
        }
        assert_eq!(
            consensus_chain.epochs().get_epoch_by_number(1).await,
            Some((ours, None)),
            "our record stays, uncertified",
        );
    }

    /// A pass ending on a peer's "not held" reply is definitive; a pass ending on a dropped request
    /// is not, wherever the cursor stopped (issue #1792).
    #[tokio::test]
    async fn test_collect_epoch_records_tells_a_dropped_request_from_an_answer() -> eyre::Result<()>
    {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let authorities: Vec<_> = fixture.authorities().collect();
        let keys: Vec<_> =
            authorities.iter().map(|authority| authority.primary_public_key()).collect();
        let signers: Vec<_> = authorities.iter().map(|authority| authority.keypair()).collect();
        let peer = keys.first().copied().ok_or_else(|| eyre::eyre!("empty committee fixture"))?;
        let record0 = EpochRecord {
            committee: keys.clone(),
            next_committee: keys.clone(),
            ..Default::default()
        };
        let record1 = EpochRecord {
            epoch: 1,
            parent_hash: record0.digest(),
            committee: keys.clone(),
            next_committee: keys,
            ..Default::default()
        };
        let cert0 = make_cert(&record0, &signers, &[0, 1, 2]);
        let cert1 = make_cert(&record1, &signers, &[0, 1, 2]);
        let served = HashMap::from([(1, (record1, cert1))]);
        let bus = ConsensusBus::new();
        for (answer_unserved, definitive) in [(false, false), (true, true)] {
            let directory = tempfile::TempDir::new()?;
            let chain =
                ConsensusChain::new_for_test(directory.path().to_owned(), fixture.committee())
                    .await?;
            chain.epochs().save(record0.clone(), cert0.clone()).await?;
            let handle = mock_epoch_record_network_with(served.clone(), peer, 1, answer_unserved);
            let collected =
                collect_epoch_records(0, &chain, &handle, bus.app(), &mut walk_off()).await?;
            assert_eq!(collected, CollectedPass { epoch: 1, definitive }, "{answer_unserved}");
        }
        Ok(())
    }

    /// Our own epoch store failing ends collection with an error, which fails startup sync and
    /// ends the critical "Epoch Record Collector". The store here is closed (a dead actor,
    /// so the persist fails `SendFailed`): nothing a peer sends can cause that.
    #[tokio::test]
    async fn test_collect_epoch_records_stops_on_a_failed_store() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let peer = fixture
            .authorities()
            .next()
            .map(|authority| authority.primary_public_key())
            .ok_or_else(|| eyre::eyre!("empty committee fixture"))?;
        let directory = tempfile::TempDir::new()?;
        let chain =
            ConsensusChain::new_for_test(directory.path().to_owned(), fixture.committee()).await?;
        chain.clone().close().await;
        let handle = mock_epoch_record_network(HashMap::new(), peer, 1);
        let bus = ConsensusBus::new();

        let Err(err) = collect_epoch_records(0, &chain, &handle, bus.app(), &mut walk_off()).await
        else {
            panic!("a failed epoch store must end collection with an error");
        };
        assert!(format!("{err:#}").contains("failed to persist"), "unexpected error: {err:#}");

        let shutdown = tn_types::ShutdownNotifier::new();
        let Err(err) = sync_epoch_records_to_tip(
            &chain,
            &handle,
            bus.app(),
            shutdown.subscribe(),
            &Parameters::default(),
        )
        .await
        else {
            panic!("a failed epoch store must fail startup sync");
        };
        assert!(format!("{err:#}").contains("failed to persist"), "unexpected error: {err:#}");

        let mut task_manager = tn_types::TaskManager::default();
        spawn_epoch_record_collector(
            chain.clone(),
            handle,
            bus.app().clone(),
            task_manager.get_spawner(),
            shutdown.subscribe(),
            &Parameters::default(),
        )
        .await?;
        let ended =
            tokio::time::timeout(Duration::from_secs(5), task_manager.until_task_ends(shutdown))
                .await?;
        let Err(tn_types::TaskJoinError::CriticalExitError(name, err)) = ended else {
            panic!("expected the record collector's error, got {ended:?}");
        };
        assert_eq!(name, "Epoch Record Collector");
        assert!(err.to_string().contains("failed to persist"), "unexpected error: {err}");
        Ok(())
    }

    /// The split behind a downloaded record's save: our own store failing is returned (the node
    /// stops), and so is a certified record that conflicts with this node's own, counted as a
    /// final-state mismatch; any other refused record, which a peer can send, is only logged.
    #[test]
    fn test_only_local_store_failures_and_conflicts_stop_collection() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let digest = EpochRecord::default().digest();
        ::metrics::with_local_recorder(&recorder, || {
            let conflict =
                EpochDbError::ConflictingRecord { epoch: 1, stored: digest, offered: digest };
            let err = stop_on_local_failure(conflict, 1).expect_err("a conflict stops the node");
            assert!(format!("{err:#}").contains("diverged"), "unexpected error: {err:#}");
            let mismatch = EpochDbError::CertificateMismatch { record: digest, certified: digest };
            assert!(stop_on_local_failure(mismatch, 1).is_ok());
            let err = stop_on_local_failure(EpochDbError::SendFailed, 1)
                .expect_err("a dead epoch store is local");
            assert!(format!("{err:#}").contains("epoch 1"), "unexpected error: {err:#}");
        });

        let snapshot = snapshotter.snapshot().into_vec();
        let (_, _, _, value) = snapshot
            .iter()
            .find(|(key, ..)| key.key().name() == FINAL_STATE_MISMATCHES)
            .expect("the conflict is counted");
        assert!(matches!(value, DebugValue::Counter(1)), "{value:?}");
    }

    /// The public gate replaces the dummy anchor and saves a peer's certified epoch chain.
    #[tokio::test]
    async fn startup_sync_downloads_from_dummy_anchor() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let authorities: Vec<_> = fixture.authorities().collect();
        let keys: Vec<_> =
            authorities.iter().map(|authority| authority.primary_public_key()).collect();
        let signers: Vec<_> = authorities.iter().map(|authority| authority.keypair()).collect();
        let peer = keys.first().copied().ok_or_else(|| eyre::eyre!("empty committee fixture"))?;
        let directory = tempfile::TempDir::new()?;
        let chain =
            ConsensusChain::new_for_test(directory.path().to_owned(), fixture.committee()).await?;
        let record0 = EpochRecord {
            committee: keys.clone(),
            next_committee: keys.clone(),
            ..Default::default()
        };
        chain.epochs().save_dummy_epoch0(record0.clone()).await?;
        let record1 = EpochRecord {
            epoch: 1,
            parent_hash: record0.digest(),
            committee: keys.clone(),
            next_committee: keys,
            ..Default::default()
        };
        let cert0 = make_cert(&record0, &signers, &[0, 1, 2]);
        let cert1 = make_cert(&record1, &signers, &[0, 1, 2]);
        let handle = mock_epoch_record_network(
            HashMap::from([(0, (record0, cert0)), (1, (record1.clone(), cert1.clone()))]),
            peer,
            1,
        );
        let bus = ConsensusBus::new();
        let shutdown = tn_types::ShutdownNotifier::new();
        sync_epoch_records_to_tip(
            &chain,
            &handle,
            bus.app(),
            shutdown.subscribe(),
            &Parameters::default(),
        )
        .await?;
        assert_eq!(chain.epochs().latest_record().await.map(|record| record.epoch), Some(1));
        assert!(!chain.epochs().contains_dummy_epoch0().await);
        assert_eq!(chain.epochs().get_epoch_by_number(1).await, Some((record1, Some(cert1))));
        Ok(())
    }

    /// Pending dials cannot make an isolated node fetch a record, even if the mock could serve it.
    #[tokio::test(start_paused = true)]
    async fn startup_sync_zero_peers_preserves_dummy() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let authorities: Vec<_> = fixture.authorities().collect();
        let keys: Vec<_> =
            authorities.iter().map(|authority| authority.primary_public_key()).collect();
        let signers: Vec<_> = authorities.iter().map(|authority| authority.keypair()).collect();
        let peer = keys.first().copied().ok_or_else(|| eyre::eyre!("empty committee fixture"))?;
        let directory = tempfile::TempDir::new()?;
        let chain =
            ConsensusChain::new_for_test(directory.path().to_owned(), fixture.committee()).await?;
        let record =
            EpochRecord { committee: keys.clone(), next_committee: keys, ..Default::default() };
        chain.epochs().save_dummy_epoch0(record.clone()).await?;
        let cert = make_cert(&record, &signers, &[0, 1, 2]);
        let handle = mock_epoch_record_network(HashMap::from([(0, (record, cert))]), peer, 0);
        let bus = ConsensusBus::new();
        let shutdown = tn_types::ShutdownNotifier::new();
        sync_epoch_records_to_tip(
            &chain,
            &handle,
            bus.app(),
            shutdown.subscribe(),
            &Parameters::default(),
        )
        .await?;
        assert_eq!(chain.epochs().latest_record().await.map(|record| record.epoch), Some(0));
        assert!(chain.epochs().contains_dummy_epoch0().await);
        Ok(())
    }

    // ---- bench7 epoch-record-collector-hardening ----

    /// Records every `epoch-manager` info and error line as `message key=value ...` on the thread
    /// it is installed on with [`tracing::subscriber::set_default`].
    struct EpochManagerLines(std::sync::Arc<parking_lot::Mutex<Vec<String>>>);

    /// Collects an event's message and its other fields, in the order the event records them.
    #[derive(Default)]
    struct LineVisitor {
        message: String,
        fields: String,
    }

    impl tracing::field::Visit for LineVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.message = format!("{value:?}");
            } else {
                self.fields.push_str(&format!(" {}={value:?}", field.name()));
            }
        }
    }

    impl tracing::Subscriber for EpochManagerLines {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == "epoch-manager"
                && [tracing::Level::INFO, tracing::Level::ERROR].contains(metadata.level())
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            let mut visitor = LineVisitor::default();
            event.record(&mut visitor);
            self.0.lock().push(format!("{}{}", visitor.message, visitor.fields));
        }

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// Records `0..=last`, chained from epoch 0 under one committee (`keys`), each with a
    /// certificate signed by the first three of `signers`.
    fn certified_chain(
        keys: &[BlsPublicKey],
        signers: &[&BlsKeypair],
        last: Epoch,
    ) -> Vec<(EpochRecord, EpochCertificate)> {
        let mut chain: Vec<(EpochRecord, EpochCertificate)> = Vec::new();
        for epoch in 0..=last {
            let parent_hash = chain.last().map(|(record, _)| record.digest()).unwrap_or_default();
            let record = EpochRecord {
                epoch,
                committee: keys.to_vec(),
                next_committee: keys.to_vec(),
                parent_hash,
                ..Default::default()
            };
            let cert = make_cert(&record, signers, &[0, 1, 2]);
            chain.push((record, cert));
        }
        chain
    }

    /// The served record's `retrieved cert` line names the peer that served it and the number of
    /// signers its certificate carries; the message text is unchanged.
    #[tokio::test]
    async fn test_retrieved_cert_line_names_the_peer_and_signers() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let authorities: Vec<_> = fixture.authorities().collect();
        let keys: Vec<_> =
            authorities.iter().map(|authority| authority.primary_public_key()).collect();
        let signers: Vec<_> = authorities.iter().map(|authority| authority.keypair()).collect();
        let mut records = certified_chain(&keys, &signers, 1).into_iter();
        let (record0, cert0) = records.next().ok_or_else(|| eyre::eyre!("no record 0"))?;
        let (record1, cert1) = records.next().ok_or_else(|| eyre::eyre!("no record 1"))?;
        let directory = tempfile::TempDir::new()?;
        let chain =
            ConsensusChain::new_for_test(directory.path().to_owned(), fixture.committee()).await?;
        chain.epochs().save(record0, cert0).await?;
        let digest = record1.digest();
        // a peer other than the first, so the line cannot pass by naming a default
        let peer = keys[1];
        let handle = mock_epoch_record_network(HashMap::from([(1, (record1, cert1))]), peer, 1);
        let bus = ConsensusBus::new();

        let lines = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let _log_guard = tracing::subscriber::set_default(EpochManagerLines(lines.clone()));
        collect_epoch_records(0, &chain, &handle, bus.app(), &mut walk_off()).await?;

        let lines = lines.lock().clone();
        let prefix = format!("retrieved cert for epoch 1: {digest} from a peer");
        let line = lines
            .iter()
            .find(|line| line.starts_with(&prefix))
            .ok_or_else(|| eyre::eyre!("no retrieved line in {lines:?}"))?;
        assert!(line.contains(&format!(" peer={peer}")), "{line}");
        assert!(line.contains(" signers=3"), "{line}");
        Ok(())
    }

    /// A [`StateSyncMetrics`] registered on a local recorder, and the snapshotter that reads it.
    fn local_metrics() -> (StateSyncMetrics, metrics_util::debugging::Snapshotter) {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let metrics = ::metrics::with_local_recorder(&recorder, || {
            StateSyncMetrics::new_with_labels(Vec::<::metrics::Label>::new())
        });
        (metrics, snapshotter)
    }

    /// `tn_state_sync.epoch_records_certified_through`, once set.
    fn certified_through(snapshotter: &metrics_util::debugging::Snapshotter) -> Option<f64> {
        snapshotter.snapshot().into_vec().into_iter().find_map(|(key, .., value)| match value {
            metrics_util::debugging::DebugValue::Gauge(gauge)
                if key.key().name() == "tn_state_sync.epoch_records_certified_through" =>
            {
                Some(gauge.0)
            }
            _ => None,
        })
    }

    /// The gauge stops at the first epoch without a certificate: a certificate stored past the
    /// gap does not move it. Epochs 0, 1 and 3 certified, epoch 2 stored without one: 1.
    #[tokio::test]
    async fn test_certified_through_stops_at_a_gap() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let authorities: Vec<_> = fixture.authorities().collect();
        let keys: Vec<_> =
            authorities.iter().map(|authority| authority.primary_public_key()).collect();
        let signers: Vec<_> = authorities.iter().map(|authority| authority.keypair()).collect();
        let directory = tempfile::TempDir::new()?;
        let chain =
            ConsensusChain::new_for_test(directory.path().to_owned(), fixture.committee()).await?;
        for (record, cert) in certified_chain(&keys, &signers, 3) {
            if record.epoch == 2 {
                chain.epochs().save_record(record).await?;
            } else {
                chain.epochs().save(record, cert).await?;
            }
        }
        let (metrics, snapshotter) = local_metrics();
        assert_eq!(certified_prefix_end(&chain, &metrics).await, 2);
        assert_eq!(certified_through(&snapshotter), Some(1.0));
        Ok(())
    }

    /// The startup sync sets the gauge from what it downloads: from the unsigned genesis anchor,
    /// which reads as none (-1), to epoch 1.
    #[tokio::test]
    async fn test_startup_sync_sets_certified_through() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let authorities: Vec<_> = fixture.authorities().collect();
        let keys: Vec<_> =
            authorities.iter().map(|authority| authority.primary_public_key()).collect();
        let signers: Vec<_> = authorities.iter().map(|authority| authority.keypair()).collect();
        let records = certified_chain(&keys, &signers, 1);
        let directory = tempfile::TempDir::new()?;
        let chain =
            ConsensusChain::new_for_test(directory.path().to_owned(), fixture.committee()).await?;
        let record0 = records.first().map(|(record, _)| record.clone());
        chain
            .epochs()
            .save_dummy_epoch0(record0.ok_or_else(|| eyre::eyre!("no record 0"))?)
            .await?;
        let handle = mock_epoch_record_network(
            records.into_iter().enumerate().map(|(epoch, pair)| (epoch as Epoch, pair)).collect(),
            keys[0],
            1,
        );
        let bus = ConsensusBus::new();
        let shutdown = tn_types::ShutdownNotifier::new();
        let (metrics, snapshotter) = local_metrics();

        assert_eq!(certified_prefix_end(&chain, &metrics).await, 0);
        assert_eq!(
            certified_through(&snapshotter),
            Some(-1.0),
            "the dummy anchor is not certified"
        );
        sync_epoch_records_to_tip_with(
            &chain,
            &handle,
            bus.app(),
            shutdown.subscribe(),
            EpochRecordCollectSwitches::default(),
            &metrics,
        )
        .await?;
        assert_eq!(certified_through(&snapshotter), Some(1.0));
        Ok(())
    }

    /// After a restart the gauge reports the certificates the store already holds before any peer
    /// answers: the startup sync reads the store before its first pass (here shutdown ends the
    /// sync before any pass runs).
    #[tokio::test]
    async fn test_certified_through_is_read_from_the_store_after_a_restart() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let authorities: Vec<_> = fixture.authorities().collect();
        let keys: Vec<_> =
            authorities.iter().map(|authority| authority.primary_public_key()).collect();
        let signers: Vec<_> = authorities.iter().map(|authority| authority.keypair()).collect();
        let directory = tempfile::TempDir::new()?;
        let chain =
            ConsensusChain::new_for_test(directory.path().to_owned(), fixture.committee()).await?;
        for (record, cert) in certified_chain(&keys, &signers, 1) {
            chain.epochs().save(record, cert).await?;
        }
        chain.epochs().persist().await?;
        chain.close().await;

        let chain = ConsensusChain::new(directory.path().to_owned(), fixture.committee())?;
        let handle = mock_epoch_record_network(HashMap::new(), keys[0], 1);
        let bus = ConsensusBus::new();
        let shutdown = tn_types::ShutdownNotifier::new();
        shutdown.notify();
        let (metrics, snapshotter) = local_metrics();
        sync_epoch_records_to_tip_with(
            &chain,
            &handle,
            bus.app(),
            shutdown.subscribe(),
            EpochRecordCollectSwitches::default(),
            &metrics,
        )
        .await?;
        assert_eq!(certified_through(&snapshotter), Some(1.0));
        Ok(())
    }

    /// The record collector sets the gauge after each pass: first from the certificate the pass
    /// downloads, then from one stored by another path while it waited, as this node's own vote
    /// round stores a certificate on reaching quorum (`save_certificate`), after the next wake-up.
    #[tokio::test]
    async fn test_record_collector_sets_certified_through() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let authorities: Vec<_> = fixture.authorities().collect();
        let keys: Vec<_> =
            authorities.iter().map(|authority| authority.primary_public_key()).collect();
        let signers: Vec<_> = authorities.iter().map(|authority| authority.keypair()).collect();
        let records = certified_chain(&keys, &signers, 2);
        let directory = tempfile::TempDir::new()?;
        let chain =
            ConsensusChain::new_for_test(directory.path().to_owned(), fixture.committee()).await?;
        // epoch 0 certified; our own records 1 and 2 stored without certificates
        for (record, cert) in records.iter().cloned() {
            if record.epoch == 0 {
                chain.epochs().save(record, cert).await?;
            } else {
                chain.epochs().save_record(record).await?;
            }
        }
        // a peer serves only epoch 1's certificate
        let handle = mock_epoch_record_network(
            records
                .iter()
                .filter(|(record, _)| record.epoch == 1)
                .map(|pair| (1, pair.clone()))
                .collect(),
            keys[0],
            1,
        );
        let bus = ConsensusBus::new();
        let shutdown = tn_types::ShutdownNotifier::new();
        let task_manager = tn_types::TaskManager::default();
        let (metrics, snapshotter) = local_metrics();
        spawn_epoch_record_collector_with(
            chain.clone(),
            handle,
            bus.app().clone(),
            task_manager.get_spawner(),
            shutdown.subscribe(),
            EpochRecordCollectSwitches::default(),
            metrics,
        )
        .await?;
        let reads = |expected: f64| {
            let snapshotter = &snapshotter;
            move || async move { Ok(certified_through(snapshotter) == Some(expected)) }
        };
        tn_test_utils::wait_until(Duration::from_secs(10), "the downloaded epoch 1", reads(1.0))
            .await?;

        let (record2, cert2) =
            records.into_iter().last().ok_or_else(|| eyre::eyre!("no record 2"))?;
        chain.epochs().save_certificate(record2.digest(), cert2).await?;
        bus.app().requested_missing_epoch().send_replace(2);
        tn_test_utils::wait_until(Duration::from_secs(10), "the own-quorum epoch 2", reads(2.0))
            .await?;
        shutdown.notify();
        Ok(())
    }

    /// The walk switched on.
    const WALK: EpochRecordCollectSwitches = EpochRecordCollectSwitches { past_uncertified: true };

    /// `tn_state_sync.epoch_record_gap_walks_total`.
    const GAP_WALKS: &str = "tn_state_sync.epoch_record_gap_walks_total";

    /// `tn_state_sync.epoch_record_walk_parent_mismatches_total`.
    const PARENT_MISMATCHES: &str = "tn_state_sync.epoch_record_walk_parent_mismatches_total";

    /// `tn_state_sync.epoch_record_conflicts_total`.
    const CONFLICTS: &str = "tn_state_sync.epoch_record_conflicts_total";

    /// A collection with the walk off, reporting to the process-wide metrics: the collection from
    /// before the switch existed.
    fn walk_off() -> Collection<'static> {
        Collection::new(EpochRecordCollectSwitches::default(), &STATE_SYNC_METRICS)
    }

    /// Every registered counter's increments since the previous snapshot, by name: the debugging
    /// recorder drains every counter on each snapshot, so a test reads them all at once.
    fn counters(
        snapshotter: &metrics_util::debugging::Snapshotter,
    ) -> std::collections::BTreeMap<String, u64> {
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter_map(|(key, .., value)| match value {
                metrics_util::debugging::DebugValue::Counter(count) => {
                    Some((key.key().name().to_string(), count))
                }
                _ => None,
            })
            .collect()
    }

    /// The committee keys of `fixture` and their key pairs, in the same order.
    fn keys_and_signers(
        fixture: &CommitteeFixture<MemDatabase>,
    ) -> (Vec<BlsPublicKey>, Vec<&BlsKeypair>) {
        fixture
            .authorities()
            .map(|authority| (authority.primary_public_key(), authority.keypair()))
            .unzip()
    }

    /// A node of `fixture`'s committee that holds record 0 with its certificate and its own
    /// record 1 without one, as after a vote round on epoch 1 that never reached quorum anywhere;
    /// and the network's certified records `0..=last`, chained on that same record 1.
    async fn own_uncertified_epoch1(
        fixture: &CommitteeFixture<MemDatabase>,
        last: Epoch,
    ) -> eyre::Result<(tempfile::TempDir, ConsensusChain, Vec<(EpochRecord, EpochCertificate)>)>
    {
        let (keys, signers) = keys_and_signers(fixture);
        let records = certified_chain(&keys, &signers, last);
        let directory = tempfile::TempDir::new()?;
        let chain =
            ConsensusChain::new_for_test(directory.path().to_owned(), fixture.committee()).await?;
        for (record, cert) in records.iter().take(2).cloned() {
            if record.epoch == 0 {
                chain.epochs().save(record, cert).await?;
            } else {
                chain.epochs().save_record(record).await?;
            }
        }
        Ok((directory, chain, records))
    }

    /// The pairs of `records` for `epochs`, as a peer serves them.
    fn served(
        records: &[(EpochRecord, EpochCertificate)],
        epochs: &[Epoch],
    ) -> HashMap<Epoch, (EpochRecord, EpochCertificate)> {
        records
            .iter()
            .filter(|(record, _)| epochs.contains(&record.epoch))
            .map(|pair| (pair.0.epoch, pair.clone()))
            .collect()
    }

    /// With the walk on, a peer's "not held" for the epoch this node holds as its own uncertified
    /// record no longer ends the pass: the certified records after it validate against that record
    /// and are stored, the epoch is an open gap, counted and logged once, and the certified-prefix
    /// gauge stays below it.
    #[tokio::test]
    async fn test_collect_epoch_records_walks_past_an_own_uncertified_epoch() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (_directory, chain, records) = own_uncertified_epoch1(&fixture, 3).await?;
        let peer = keys_and_signers(&fixture).0[0];
        let handle = mock_epoch_record_network(served(&records, &[2, 3]), peer, 1);
        let bus = ConsensusBus::new();
        let (metrics, snapshotter) = local_metrics();
        let mut collection = Collection::new(WALK, &metrics);

        let lines = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let _log_guard = tracing::subscriber::set_default(EpochManagerLines(lines.clone()));
        let collected =
            collect_epoch_records(0, &chain, &handle, bus.app(), &mut collection).await?;
        assert_eq!(collected, CollectedPass { epoch: 3, definitive: true });
        assert_eq!(collection.gaps, BTreeSet::from([1]));
        for (record, cert) in records.iter().skip(2) {
            assert_eq!(
                chain.epochs().get_epoch_by_number(record.epoch).await,
                Some((record.clone(), Some(cert.clone()))),
                "epoch {}",
                record.epoch
            );
        }
        assert_eq!(*bus.app().certified_record_epoch().borrow(), Some(3));
        assert_eq!(chain.epochs().get_epoch_by_number(1).await.and_then(|(_, cert)| cert), None);

        // a second pass from the cursor neither walks nor counts again
        let collected =
            collect_epoch_records(3, &chain, &handle, bus.app(), &mut collection).await?;
        assert_eq!(collected, CollectedPass { epoch: 3, definitive: true });
        assert_eq!(counters(&snapshotter).get(GAP_WALKS), Some(&1));
        let walks: Vec<String> = lines
            .lock()
            .iter()
            .filter(|line| line.starts_with("epoch record collector walking past"))
            .cloned()
            .collect();
        assert_eq!(
            walks,
            ["epoch record collector walking past an uncertified epoch epoch=1 gaps=1"]
        );

        // the gauge reports the certified prefix, which ends below the open gap
        assert_eq!(certified_prefix_end(&chain, &metrics).await, 1);
        assert_eq!(certified_through(&snapshotter), Some(0.0));
        Ok(())
    }

    /// With the walk off (the default) the pass stops at the epoch the network left uncertified,
    /// however many certified records the network holds after it, pass after pass: gap G1 of
    /// issue #1850.
    #[tokio::test]
    async fn test_collect_epoch_records_stops_at_an_uncertified_epoch_with_the_walk_off(
    ) -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (_directory, chain, records) = own_uncertified_epoch1(&fixture, 3).await?;
        let peer = keys_and_signers(&fixture).0[0];
        let handle = mock_epoch_record_network(served(&records, &[2, 3]), peer, 1);
        let bus = ConsensusBus::new();
        let mut collection = walk_off();
        for _ in 0..2 {
            let collected =
                collect_epoch_records(0, &chain, &handle, bus.app(), &mut collection).await?;
            assert_eq!(collected, CollectedPass { epoch: 0, definitive: true });
        }
        assert!(collection.gaps.is_empty());
        assert_eq!(chain.epochs().get_epoch_by_number(2).await, None);
        Ok(())
    }

    /// A node without its own record of the epoch never walks past it, walk on or not: an observer
    /// behind (record 0 certified, no record 1) stops as before, and the epoch-0 dummy anchor of a
    /// fresh node is no record to walk past, while a real record 0 of its own is.
    #[tokio::test]
    async fn test_collect_epoch_records_does_not_walk_past_a_missing_record() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (keys, signers) = keys_and_signers(&fixture);
        let records = certified_chain(&keys, &signers, 3);
        let handle = mock_epoch_record_network(served(&records, &[2, 3]), keys[0], 1);
        let bus = ConsensusBus::new();
        let (metrics, snapshotter) = local_metrics();

        let directory = tempfile::TempDir::new()?;
        let chain =
            ConsensusChain::new_for_test(directory.path().to_owned(), fixture.committee()).await?;
        let (record0, cert0) = records[0].clone();
        chain.epochs().save(record0.clone(), cert0).await?;
        let mut collection = Collection::new(WALK, &metrics);
        let collected =
            collect_epoch_records(0, &chain, &handle, bus.app(), &mut collection).await?;
        assert_eq!(collected, CollectedPass { epoch: 0, definitive: true });
        assert!(collection.gaps.is_empty());
        assert_eq!(chain.epochs().get_epoch_by_number(2).await, None);
        assert_eq!(counters(&snapshotter).get(GAP_WALKS), Some(&0));

        let directory = tempfile::TempDir::new()?;
        let fresh =
            ConsensusChain::new_for_test(directory.path().to_owned(), fixture.committee()).await?;
        fresh.epochs().save_dummy_epoch0(record0.clone()).await?;
        assert!(!collection.may_walk_past(0, &[], &fresh).await, "the dummy is no own record");
        fresh.epochs().save_record(record0).await?;
        assert!(collection.may_walk_past(0, &[], &fresh).await, "a real own record 0 is");
        Ok(())
    }

    /// When this node's own record of the walked epoch differs from the one the network built on,
    /// the next epoch's certified record names another parent: the walk stops there as a pass
    /// with the walk off would, opens no gap, and the case is counted.
    #[tokio::test]
    async fn test_collect_epoch_records_stops_at_a_parent_mismatch_after_a_walk() -> eyre::Result<()>
    {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (keys, signers) = keys_and_signers(&fixture);
        let (_directory, chain, records) = own_uncertified_epoch1(&fixture, 1).await?;
        // the network's epoch 1 differs from ours and was never certified; its epoch 2 was
        let mut theirs1 = records[1].0.clone();
        theirs1.final_state.number = 7;
        let theirs2 = EpochRecord {
            epoch: 2,
            committee: keys.clone(),
            next_committee: keys.clone(),
            parent_hash: theirs1.digest(),
            ..Default::default()
        };
        let cert2 = make_cert(&theirs2, &signers, &[0, 1, 2]);
        let handle = mock_epoch_record_network(HashMap::from([(2, (theirs2, cert2))]), keys[0], 1);
        let bus = ConsensusBus::new();
        let (metrics, snapshotter) = local_metrics();
        let mut collection = Collection::new(WALK, &metrics);

        // the cursor stays before the walked epoch, so the next pass meets it again
        for _ in 0..2 {
            let collected =
                collect_epoch_records(0, &chain, &handle, bus.app(), &mut collection).await?;
            assert_eq!(collected, CollectedPass { epoch: 0, definitive: true });
            let counted = counters(&snapshotter);
            assert_eq!(counted.get(PARENT_MISMATCHES), Some(&1));
            assert_eq!(counted.get(GAP_WALKS), Some(&0));
        }
        assert!(collection.gaps.is_empty());
        assert_eq!(chain.epochs().get_epoch_by_number(2).await, None);
        Ok(())
    }

    /// At most [`MAX_UNCERTIFIED_GAPS`] gaps are open: with all of them open the pass stops at the
    /// next uncertified epoch as with the walk off; with one closed it walks and fills the bound.
    #[tokio::test]
    async fn test_collect_epoch_records_bounds_the_open_gaps() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (_directory, chain, records) = own_uncertified_epoch1(&fixture, 3).await?;
        let peer = keys_and_signers(&fixture).0[0];
        let handle = mock_epoch_record_network(served(&records, &[2, 3]), peer, 1);
        let bus = ConsensusBus::new();
        let (metrics, _snapshotter) = local_metrics();
        let mut collection = Collection::new(WALK, &metrics);
        let full = 100..100 + MAX_UNCERTIFIED_GAPS as Epoch;
        collection.gaps.extend(full.clone());

        let collected =
            collect_epoch_records(0, &chain, &handle, bus.app(), &mut collection).await?;
        assert_eq!(collected, CollectedPass { epoch: 0, definitive: true });
        assert_eq!(collection.gaps, full.clone().collect::<BTreeSet<_>>());
        assert_eq!(chain.epochs().get_epoch_by_number(2).await, None);

        collection.gaps.remove(&full.start);
        let collected =
            collect_epoch_records(0, &chain, &handle, bus.app(), &mut collection).await?;
        assert_eq!(collected, CollectedPass { epoch: 3, definitive: true });
        assert!(collection.gaps.contains(&1));
        assert_eq!(collection.gaps.len(), MAX_UNCERTIFIED_GAPS);
        Ok(())
    }

    /// A gap whose certificate another path stored, as this node's own vote round does
    /// (`save_certificate`), leaves the set at the next retry without being asked for; one still
    /// uncertified everywhere stays.
    #[tokio::test]
    async fn test_retry_gaps_drops_a_gap_another_path_certified() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (_directory, chain, records) = own_uncertified_epoch1(&fixture, 1).await?;
        let peer = keys_and_signers(&fixture).0[0];
        let handle = mock_epoch_record_network(HashMap::new(), peer, 1);
        let bus = ConsensusBus::new();
        let (metrics, _snapshotter) = local_metrics();
        let mut collection = Collection::new(WALK, &metrics);
        collection.gaps.insert(1);
        let shutdown = tn_types::ShutdownNotifier::new();

        retry_gaps(&chain, &handle, bus.app(), &mut collection, &shutdown.subscribe()).await?;
        assert_eq!(collection.gaps, BTreeSet::from([1]), "no peer holds it yet");

        let (record1, cert1) = records[1].clone();
        chain.epochs().save_certificate(record1.digest(), cert1).await?;
        retry_gaps(&chain, &handle, bus.app(), &mut collection, &shutdown.subscribe()).await?;
        assert!(collection.gaps.is_empty());
        Ok(())
    }

    /// A mock peer network that answers each epoch record request from `served` as it is at the
    /// time of the request ("not held" for an epoch it lacks), or not at all for an epoch in
    /// `silent` at that time, and then sends the requested epoch on `requests`.
    fn mock_changing_network(
        served: std::sync::Arc<parking_lot::Mutex<HashMap<Epoch, (EpochRecord, EpochCertificate)>>>,
        silent: std::sync::Arc<parking_lot::Mutex<BTreeSet<Epoch>>>,
        peer: BlsPublicKey,
        requests: mpsc::UnboundedSender<Epoch>,
    ) -> PrimaryNetworkHandle {
        let (tx, mut rx) = mpsc::channel(100);
        tokio::spawn(async move {
            // a silent request's reply is held, so its requester waits as on a peer that accepted
            // the request and never answers
            let mut held = Vec::new();
            while let Some(cmd) = rx.recv().await {
                let NetworkCommand::SendRequestAny { request, reply } = cmd else {
                    continue;
                };
                let PrimaryRequest::EpochRecord { epoch: Some(epoch), .. } = request else {
                    continue;
                };
                if silent.lock().contains(&epoch) {
                    held.push(reply);
                } else {
                    let pair = served.lock().get(&epoch).cloned();
                    let result = match pair {
                        Some((record, certificate)) => {
                            PrimaryResponse::EpochRecord { record, certificate }
                        }
                        None => PrimaryResponse::unavailable_epoch_record(Some(epoch), None),
                    };
                    let _ = reply.send(Ok(NetworkResponseMessage { peer, result }));
                }
                let _ = requests.send(epoch);
            }
        });
        PrimaryNetworkHandle::new_for_test(tx)
    }

    /// The next `expected.len()` epochs requested, in order, must be `expected`.
    async fn expect_requests(
        requests: &mut mpsc::UnboundedReceiver<Epoch>,
        expected: &[Epoch],
    ) -> eyre::Result<()> {
        let mut seen = Vec::with_capacity(expected.len());
        while seen.len() < expected.len() {
            seen.push(requests.recv().await.ok_or_else(|| eyre::eyre!("mock network ended"))?);
        }
        assert_eq!(seen, expected);
        Ok(())
    }

    /// The record collector walks past this node's own uncertified epoch 1, then, driven by the
    /// retry timer alone (no requested epoch ever changes), asks for it after a pass once the retry
    /// interval has passed, and stores its certificate as soon as the network serves one, which
    /// closes the gap and moves the gauge to the tip. A peer's "not held" answer is asked three
    /// times (`request_epoch_cert` tries three peers). The test waits on the requests, never on a
    /// timer, so the paused clock cannot fire a timeout while a store read runs on the store's own
    /// thread.
    #[tokio::test(start_paused = true)]
    async fn test_record_collector_certifies_from_the_retry_timer_alone() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (_directory, chain, records) = own_uncertified_epoch1(&fixture, 3).await?;
        let peer = keys_and_signers(&fixture).0[0];
        let answers = std::sync::Arc::new(parking_lot::Mutex::new(served(&records, &[2, 3])));
        let (requests_tx, mut requests) = mpsc::unbounded_channel();
        let handle = mock_changing_network(answers.clone(), Default::default(), peer, requests_tx);
        let bus = ConsensusBus::new();
        let shutdown = tn_types::ShutdownNotifier::new();
        let task_manager = tn_types::TaskManager::default();
        let (metrics, snapshotter) = local_metrics();
        spawn_epoch_record_collector_with(
            chain.clone(),
            handle,
            bus.app().clone(),
            task_manager.get_spawner(),
            shutdown.subscribe(),
            WALK,
            metrics,
        )
        .await?;

        // the first pass walks past epoch 1, stores 2 and 3 and stops at the tip; the gap it just
        // asked for is not retried in the same iteration
        expect_requests(&mut requests, &[1, 1, 1, 2, 3, 4, 4, 4]).await?;
        // the timer's next pass, then the retry of the gap, which no peer holds yet
        expect_requests(&mut requests, &[4, 4, 4, 1, 1, 1]).await?;
        assert_eq!(certified_through(&snapshotter), Some(0.0), "below the open gap");

        answers.lock().insert(1, records[1].clone());
        expect_requests(&mut requests, &[4, 4, 4, 1]).await?;
        // the gap is closed: the next iteration asks for the tip only
        expect_requests(&mut requests, &[4, 4, 4]).await?;
        assert_eq!(certified_through(&snapshotter), Some(3.0));
        assert_eq!(*bus.app().requested_missing_epoch().borrow(), 0);
        shutdown.notify();
        assert_eq!(
            chain.epochs().get_epoch_by_number(1).await,
            Some((records[1].0.clone(), Some(records[1].1.clone())))
        );
        Ok(())
    }

    /// A peer's certified record that validates against this node's record 0 but differs from its
    /// own record 1 is counted and stops the collection with the walk off or on: the pass, a gap
    /// retry and the critical record collector all end with the error that says this node's
    /// execution diverged, and the node's own record stays.
    #[tokio::test]
    async fn test_collect_epoch_records_counts_and_stops_on_a_conflicting_certified_record(
    ) -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (_, signers) = keys_and_signers(&fixture);
        let (_directory, chain, records) = own_uncertified_epoch1(&fixture, 1).await?;
        let ours = records[1].0.clone();
        let mut theirs = ours.clone();
        theirs.final_state.number = 7;
        let cert = make_cert(&theirs, &signers, &[0, 1, 2]);
        let peer = keys_and_signers(&fixture).0[1];
        let handle = mock_epoch_record_network(HashMap::from([(1, (theirs, cert))]), peer, 1);
        let bus = ConsensusBus::new();
        let (metrics, snapshotter) = local_metrics();
        let diverged = |err: &eyre::Report| {
            let text = format!("{err:#}");
            text.contains("epoch 1") && text.contains("diverged")
        };

        for switches in [EpochRecordCollectSwitches::default(), WALK] {
            let mut collection = Collection::new(switches, &metrics);
            let err = collect_epoch_records(0, &chain, &handle, bus.app(), &mut collection)
                .await
                .expect_err("a conflicting certified record ends the pass");
            assert!(diverged(&err), "unexpected error: {err:#}");
            assert_eq!(counters(&snapshotter).get(CONFLICTS), Some(&1), "{switches:?}");
        }

        let mut walking = Collection::new(WALK, &metrics);
        walking.gaps.insert(1);
        let shutdown = tn_types::ShutdownNotifier::new();
        let err = retry_gaps(&chain, &handle, bus.app(), &mut walking, &shutdown.subscribe())
            .await
            .expect_err("a conflicting certified record ends a gap retry");
        assert!(diverged(&err), "unexpected error: {err:#}");
        assert_eq!(counters(&snapshotter).get(CONFLICTS), Some(&1));
        drop(walking);

        let shutdown = tn_types::ShutdownNotifier::new();
        let mut task_manager = tn_types::TaskManager::default();
        spawn_epoch_record_collector_with(
            chain.clone(),
            handle,
            bus.app().clone(),
            task_manager.get_spawner(),
            shutdown.subscribe(),
            EpochRecordCollectSwitches::default(),
            metrics,
        )
        .await?;
        let ended =
            tokio::time::timeout(Duration::from_secs(5), task_manager.until_task_ends(shutdown))
                .await?;
        let Err(tn_types::TaskJoinError::CriticalExitError(name, err)) = ended else {
            panic!("expected the record collector's error, got {ended:?}");
        };
        assert_eq!(name, "Epoch Record Collector");
        assert!(err.to_string().contains("diverged"), "unexpected error: {err}");
        assert_eq!(chain.epochs().get_epoch_by_number(1).await, Some((ours, None)));
        Ok(())
    }

    /// The startup sync runs under the same switch, read from [`Parameters`]: with the walk on it
    /// stores the certified records past this node's own uncertified epoch 1 and with it off it
    /// stops there; a conflicting certified record fails it either way.
    #[tokio::test]
    async fn test_startup_sync_walks_under_the_switch_and_stops_on_a_conflict() -> eyre::Result<()>
    {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (keys, signers) = keys_and_signers(&fixture);
        let bus = ConsensusBus::new();
        let shutdown = tn_types::ShutdownNotifier::new();
        for walk in [false, true] {
            let (_directory, chain, records) = own_uncertified_epoch1(&fixture, 3).await?;
            let handle = mock_epoch_record_network(served(&records, &[2, 3]), keys[0], 1);
            let parameters =
                Parameters { epoch_record_collect_past_uncertified: walk, ..Parameters::default() };
            sync_epoch_records_to_tip(
                &chain,
                &handle,
                bus.app(),
                shutdown.subscribe(),
                &parameters,
            )
            .await?;
            let stored3 = chain.epochs().get_epoch_by_number(3).await;
            assert_eq!(stored3.is_some(), walk, "walk {walk}");
        }

        for walk in [false, true] {
            let (_directory, chain, records) = own_uncertified_epoch1(&fixture, 1).await?;
            let mut theirs = records[1].0.clone();
            theirs.final_state.number = 7;
            let cert = make_cert(&theirs, &signers, &[0, 1, 2]);
            let handle =
                mock_epoch_record_network(HashMap::from([(1, (theirs, cert))]), keys[0], 1);
            let parameters =
                Parameters { epoch_record_collect_past_uncertified: walk, ..Parameters::default() };
            let err = sync_epoch_records_to_tip(
                &chain,
                &handle,
                bus.app(),
                shutdown.subscribe(),
                &parameters,
            )
            .await
            .expect_err("a conflicting certified record fails startup");
            assert!(format!("{err:#}").contains("diverged"), "walk {walk}: {err:#}");
        }
        Ok(())
    }

    /// Every conflicting certified record a collection meets is counted in
    /// `tn_state_sync.epoch_record_conflicts_total` and then handed to [`stop_on_local_failure`],
    /// which counts it in `tn_epoch.final_state_mismatches_total` and stops the node, with the walk
    /// off or on; any other refusal is neither counted as a conflict nor fatal.
    #[tokio::test]
    async fn test_every_conflict_reaches_the_final_state_mismatch_stop() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (keys, signers) = keys_and_signers(&fixture);
        let (_directory, chain, records) = own_uncertified_epoch1(&fixture, 1).await?;
        let mut theirs = records[1].0.clone();
        theirs.final_state.number = 7;
        let cert = make_cert(&theirs, &signers, &[0, 1, 2]);
        let (stored, offered) = (records[1].0.digest(), theirs.digest());

        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _recorder_guard = ::metrics::set_default_local_recorder(&recorder);
        let metrics = StateSyncMetrics::new_with_labels(Vec::<::metrics::Label>::new());
        for switches in [EpochRecordCollectSwitches::default(), WALK] {
            let collection = Collection::new(switches, &metrics);
            let conflict = EpochDbError::ConflictingRecord { epoch: 1, stored, offered };
            let err = collection
                .refused(conflict, 1, keys[0], (&theirs, &cert), &chain)
                .await
                .expect_err("a conflict stops the node");
            assert!(format!("{err:#}").contains("diverged"), "unexpected error: {err:#}");
            let mismatch = EpochDbError::CertificateMismatch { record: offered, certified: stored };
            let refused = collection.refused(mismatch, 1, keys[0], (&theirs, &cert), &chain).await;
            assert!(refused.is_ok(), "{switches:?}: {refused:?}");
        }
        let counted = counters(&snapshotter);
        assert_eq!(counted.get(CONFLICTS), Some(&2));
        assert_eq!(counted.get(FINAL_STATE_MISMATCHES), Some(&2));
        Ok(())
    }

    /// The `epoch-manager` lines recorded while `run` runs on this thread.
    async fn lines_of<T>(run: impl Future<Output = T>) -> (T, Vec<String>) {
        let lines = std::sync::Arc::new(parking_lot::Mutex::new(Vec::new()));
        let _log_guard = tracing::subscriber::set_default(EpochManagerLines(lines.clone()));
        let output = run.await;
        let lines = lines.lock().clone();
        (output, lines)
    }

    /// The stable start of a conflict's evidence line.
    const CONFLICT_PHRASE: &str = "epoch record conflict:";

    /// A conflict's line carries what an operator needs to look into it: both digests, the serving
    /// peer, the certificate's signers as authority ids, how many of the members of the committee
    /// the record before it names signed, and the offered record's epoch, parent and final
    /// execution state. It starts with [`CONFLICT_PHRASE`], says that the node stops, and comes
    /// before the stop's own line. A refused invalid record's line names the peer.
    #[tokio::test]
    async fn test_conflict_and_invalid_record_lines_carry_the_evidence() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (keys, signers) = keys_and_signers(&fixture);
        let (_directory, chain, records) = own_uncertified_epoch1(&fixture, 1).await?;
        let mut theirs = records[1].0.clone();
        theirs.final_state.number = 7;
        let cert = make_cert(&theirs, &signers, &[0, 1, 3]);
        // a peer that is not a signer: the line must not name the relay as one
        let peer = keys[2];
        let handle =
            mock_epoch_record_network(HashMap::from([(1, (theirs.clone(), cert))]), peer, 1);
        let bus = ConsensusBus::new();
        let (metrics, _snapshotter) = local_metrics();

        let mut collection = Collection::new(EpochRecordCollectSwitches::default(), &metrics);
        let (collected, lines) =
            lines_of(collect_epoch_records(0, &chain, &handle, bus.app(), &mut collection)).await;
        let err = collected.expect_err("a conflicting certified record stops the collection");
        assert!(format!("{err:#}").contains("diverged"), "unexpected error: {err:#}");
        let conflict_lines: Vec<&String> =
            lines.iter().filter(|line| line.contains(" signer_ids=")).collect();
        assert_eq!(conflict_lines.len(), 1, "{lines:?}");
        let text = format!(
            "{CONFLICT_PHRASE} this node's stored record of epoch 1 differs from a record \
             certified by 3 of the 4 members of the committee the record before it names; this \
             node stops; do not delete its chain data before the cause is known "
        );
        assert!(conflict_lines[0].starts_with(&text), "{}", conflict_lines[0]);
        let position = |prefix: &str| lines.iter().position(|line| line.starts_with(prefix));
        let stop = "a certified epoch record from peers conflicts with this node's own record";
        assert!(
            matches!((position(CONFLICT_PHRASE), position(stop)), (Some(at), Some(then)) if at < then),
            "the evidence line comes before the stop's line: {lines:?}"
        );
        let ids: Vec<String> =
            [0, 1, 3].iter().map(|i| AuthorityIdentifier::from(keys[*i]).to_string()).collect();
        let fields = [
            " epoch=1".to_string(),
            format!(" stored={}", records[1].0.digest()),
            format!(" offered={}", theirs.digest()),
            format!(" peer={peer}"),
            " signers=3".to_string(),
            " committee=4".to_string(),
            format!(" signer_ids=[{}]", ids.join(",")),
            " offered_epoch=1".to_string(),
            format!(" offered_parent={}", theirs.parent_hash),
            " offered_final_number=7".to_string(),
            format!(" offered_final_hash={}", theirs.final_state.hash),
        ];
        for line in &conflict_lines {
            for field in &fields {
                assert!(line.contains(field.as_str()), "{field} missing in {line}");
            }
        }

        // two signers of four: the certificate fails validation
        let short = make_cert(&theirs, &signers, &[0, 1]);
        let handle = mock_epoch_record_network(HashMap::from([(1, (theirs, short))]), peer, 1);
        let mut collection = walk_off();
        let (collected, lines) =
            lines_of(collect_epoch_records(0, &chain, &handle, bus.app(), &mut collection)).await;
        assert_eq!(collected?, CollectedPass { epoch: 0, definitive: true });
        let invalid = lines
            .iter()
            .find(|line| line.starts_with("got an invalid epoch record, epoch 1"))
            .ok_or_else(|| eyre::eyre!("no invalid-record line in {lines:?}"))?;
        assert!(invalid.contains(&format!(" peer={peer}")), "{invalid}");
        Ok(())
    }

    /// The gap retry cannot hold the record collector loop. With the peers silent on a gap's
    /// request, the retry lets go at its deadline, so the pass after it starts within
    /// [`GAP_RETRY_DEADLINE`], and a shutdown during the retry ends the collector at once. The
    /// clock is paused and, while the retry waits, its own deadline is the only timer pending,
    /// so the measured times are exact.
    #[tokio::test(start_paused = true)]
    async fn test_gap_retry_lets_go_at_its_deadline_and_on_shutdown() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (_directory, chain, records) = own_uncertified_epoch1(&fixture, 3).await?;
        let peer = keys_and_signers(&fixture).0[0];
        let answers = std::sync::Arc::new(parking_lot::Mutex::new(served(&records, &[2, 3])));
        let silent = std::sync::Arc::new(parking_lot::Mutex::new(BTreeSet::new()));
        let (requests_tx, mut requests) = mpsc::unbounded_channel();
        let handle = mock_changing_network(answers, silent.clone(), peer, requests_tx);
        let bus = ConsensusBus::new();
        let collector_shutdown = tn_types::ShutdownNotifier::new();
        // never notified: the task manager reports the collector's own exit
        let manager_shutdown = tn_types::ShutdownNotifier::new();
        let mut task_manager = tn_types::TaskManager::default();
        let (metrics, _snapshotter) = local_metrics();
        spawn_epoch_record_collector_with(
            chain.clone(),
            handle,
            bus.app().clone(),
            task_manager.get_spawner(),
            collector_shutdown.subscribe(),
            WALK,
            metrics,
        )
        .await?;

        // the first pass opens epoch 1 as a gap; from then on no peer answers a request for it
        expect_requests(&mut requests, &[1, 1, 1, 2, 3, 4, 4, 4]).await?;
        silent.lock().insert(1);
        // the timer's next pass, then the retry, held on its first request
        expect_requests(&mut requests, &[4, 4, 4, 1]).await?;
        let retry_asked = tokio::time::Instant::now();
        // a requested epoch wakes the loop as soon as the retry lets go of it
        bus.app().requested_missing_epoch().send_replace(4);
        expect_requests(&mut requests, &[4]).await?;
        assert!(retry_asked.elapsed() <= GAP_RETRY_DEADLINE, "{:?}", retry_asked.elapsed());
        expect_requests(&mut requests, &[4, 4]).await?;

        // the timer's pass after that and the next retry, held again: shutdown ends the collector
        expect_requests(&mut requests, &[4, 4, 4, 1]).await?;
        let notified = tokio::time::Instant::now();
        collector_shutdown.notify();
        let ended = task_manager.until_task_ends(manager_shutdown).await;
        let Err(tn_types::TaskJoinError::CriticalExitOk(name)) = ended else {
            panic!("expected the record collector's own exit, got {ended:?}");
        };
        assert_eq!(name, "Epoch Record Collector");
        assert!(notified.elapsed() < GAP_RETRY_DEADLINE, "{:?}", notified.elapsed());
        Ok(())
    }

    /// A gap closes only on a stored certificate. One whose read fails (here an epoch the store
    /// does not hold, which a failed read reports the same way) stays open and is not asked for;
    /// an uncertified one is asked for and stays open while no peer holds it.
    #[tokio::test]
    async fn test_retry_gaps_keeps_a_gap_whose_read_fails() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (_directory, chain, _records) = own_uncertified_epoch1(&fixture, 1).await?;
        let peer = keys_and_signers(&fixture).0[0];
        let (requests_tx, mut requests) = mpsc::unbounded_channel();
        let handle =
            mock_changing_network(Default::default(), Default::default(), peer, requests_tx);
        let bus = ConsensusBus::new();
        let (metrics, _snapshotter) = local_metrics();
        let mut collection = Collection::new(WALK, &metrics);
        collection.gaps.extend([1, 5]);
        let shutdown = tn_types::ShutdownNotifier::new();

        retry_gaps(&chain, &handle, bus.app(), &mut collection, &shutdown.subscribe()).await?;
        assert_eq!(collection.gaps, BTreeSet::from([1, 5]));
        expect_requests(&mut requests, &[1, 1, 1]).await?;
        assert!(requests.try_recv().is_err(), "the unreadable gap must not be asked for");
        Ok(())
    }

    /// A refused record after a walk ends the pass at its end, not with an early return: what the
    /// pass stored is persisted and its newest final consensus header is published, as after any
    /// other pass. Here the pass stores epoch 1's certificate, walks past this node's own
    /// uncertified epoch 2, and refuses epoch 3, whose record names another parent.
    #[tokio::test]
    async fn test_a_refusal_after_a_walk_reaches_the_end_of_the_pass() -> eyre::Result<()> {
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let (keys, signers) = keys_and_signers(&fixture);
        let mut records = certified_chain(&keys, &signers, 2);
        // epoch 1 ends on a real consensus header, so storing its certificate publishes one
        let final_consensus =
            ConsensusNumHash { number: 9, hash: ConsensusHeaderDigest::from([9; 32]) };
        records[1].0.final_consensus = final_consensus;
        records[1].1 = make_cert(&records[1].0, &signers, &[0, 1, 2]);
        records[2].0.parent_hash = records[1].0.digest();
        records[2].1 = make_cert(&records[2].0, &signers, &[0, 1, 2]);
        let directory = tempfile::TempDir::new()?;
        let chain =
            ConsensusChain::new_for_test(directory.path().to_owned(), fixture.committee()).await?;
        chain.epochs().save(records[0].0.clone(), records[0].1.clone()).await?;
        chain.epochs().save_record(records[1].0.clone()).await?;
        chain.epochs().save_record(records[2].0.clone()).await?;
        // the network's epoch 3 builds on another record than this node's epoch 2
        let theirs3 = EpochRecord {
            epoch: 3,
            committee: keys.clone(),
            next_committee: keys.clone(),
            parent_hash: records[1].0.digest(),
            ..Default::default()
        };
        let cert3 = make_cert(&theirs3, &signers, &[0, 1, 2]);
        let mut answers = served(&records, &[1]);
        answers.insert(3, (theirs3, cert3));
        let handle = mock_epoch_record_network(answers, keys[0], 1);
        let bus = ConsensusBus::new();
        let (metrics, snapshotter) = local_metrics();
        let mut collection = Collection::new(WALK, &metrics);

        let collected =
            collect_epoch_records(0, &chain, &handle, bus.app(), &mut collection).await?;
        assert_eq!(collected, CollectedPass { epoch: 1, definitive: true });
        assert_eq!(counters(&snapshotter).get(PARENT_MISMATCHES), Some(&1));
        assert!(collection.gaps.is_empty());
        assert_eq!(
            *bus.app().last_published_consensus_num_hash().borrow(),
            (1, final_consensus.number, final_consensus.hash)
        );
        assert_eq!(
            chain.epochs().get_epoch_by_number(1).await,
            Some((records[1].0.clone(), Some(records[1].1.clone())))
        );
        Ok(())
    }
}
