//! Tasks and helpers for collecting epoch records trustlessly.

#[cfg(test)]
use tn_test_utils as _;

use std::{collections::BTreeSet, time::Duration};

use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp};
use tn_storage::consensus::ConsensusChain;
use tn_types::{
    BlsPublicKey, ConsensusHeaderDigest, Epoch, EpochDigest, EpochRecord, Noticer, TaskSpawner,
};
use tracing::{debug, error, info, warn};

/// How long to wait before retrying a failed epoch record collection.
const EPOCH_COLLECT_RETRY_SECS: u64 = 5;

/// Hard cap on the startup record-sync gate.
///
/// The gate normally exits after two quick quiescent passes; this deadline bounds startup
/// when peers keep failing or records keep trickling in. Sized so existing e2e timing
/// budgets still hold with the soft peer wait that runs before the gate.
const RECORD_SYNC_DEADLINE: Duration = Duration::from_secs(20);

/// Longest a single gate pass may run without landing a new record in the DB.
///
/// A pass stalls rather than fails when every connected peer is itself still starting up
/// (a whole cohort booting or restarting at once): those peers accept the record request
/// but are not serving yet, so no response ever comes back quickly. Records save to the DB
/// as they verify, so cutting a pass after a few seconds and consulting the DB separates
/// real progress (keep going) from starvation (stop; the epoch record collector takes
/// over) without burning the whole [`RECORD_SYNC_DEADLINE`].
const RECORD_SYNC_PASS_CAP: Duration = Duration::from_secs(5);

/// Return true if committee is compatable with epoch_rec_committee.
/// These will usually be equal but it is possible for a validator to be
/// booted and still in committee but not in epoch_rec.committee.
/// This is very unlikely, but check for it just in case.
fn epoch_committee_valid(epoch_rec: &EpochRecord, committee: &BTreeSet<BlsPublicKey>) -> bool {
    let epoch_len = epoch_rec.committee.len();
    let committee_len = committee.len();
    let epoch_committee: BTreeSet<BlsPublicKey> = epoch_rec.committee.iter().copied().collect();
    match committee_len.cmp(&epoch_len) {
        std::cmp::Ordering::Less => false,
        std::cmp::Ordering::Equal => committee == &epoch_committee,
        std::cmp::Ordering::Greater => {
            if epoch_len < 4 || epoch_len < ((committee_len / 3) * 2) {
                // Make sure we have a reasonable committe size, i.e. don't let
                // a bogus record with one signer through, etc.
                false
            } else {
                for k in &epoch_rec.committee {
                    if !committee.contains(k) {
                        return false;
                    }
                }
                true
            }
        }
    }
}

/// Asks peers for records from last_epoch to requested_epoch.
/// Returns the Epoch that was last retrieved.
async fn collect_epoch_records(
    last_epoch: Epoch,
    consensus_chain: &ConsensusChain,
    primary_handle: &PrimaryNetworkHandle,
    consensus_bus: &ConsensusBusApp,
) -> Epoch {
    let mut result_epoch = last_epoch;
    // Track the highest final_consensus seen across all downloaded epoch records.
    // We emit a single state-sync notification at the end rather than one per epoch,
    // to avoid flooding peers with concurrent request_consensus calls during catch-up.
    let mut best_final_consensus: Option<(Epoch, u64, ConsensusHeaderDigest)> = None;
    for epoch in last_epoch.. {
        // If we already have epoch record AND it's certificate then continue.
        if let Some((rec, Some(_))) = consensus_chain.epochs().get_epoch_by_number(epoch).await {
            // Advance result_epoch so we correctly track the last confirmed epoch,
            // allowing last_epoch to advance past already-complete epochs on future calls.
            result_epoch = epoch;

            let number = rec.final_consensus.number;
            let hash = rec.final_consensus.hash;
            if consensus_bus.publish_consensus_num_hash_if_newer(epoch, number, hash) {
                debug!(
                    target: "epoch-manager",
                    "epoch record sync downloaded up to epoch {result_epoch}, final consensus at block {number} ({hash}) - notifying state sync",
                );
            }

            continue;
        }
        // Try to recover by downloading the epoch record and cert from a peer.
        match primary_handle.request_epoch_cert(Some(epoch), None).await {
            Ok((epoch_rec, cert)) => {
                let (parent_hash, committee) = if epoch == 0 {
                    // If we can't find the genesis committee something is very wrong.
                    let committee = consensus_chain
                        .epochs()
                        .get_committee_keys(0)
                        .await
                        .expect("always can retrieve epoch 0 committee");
                    (EpochDigest::default(), committee)
                } else if let Some(prev) = consensus_chain.epochs().record_by_epoch(epoch - 1).await
                {
                    (prev.digest(), prev.next_committee.iter().copied().collect())
                } else {
                    // We are missing epoch records.
                    // Should not be here but if so just skipping won't really help...
                    // Reduce last_epoch by one and once this loop finishes skipping we can
                    // try to get the missing epoch again.
                    return epoch.saturating_sub(1);
                };
                // Verify the epoch has the expected parent and committee and is signed by
                // that committee.
                let parents_match = parent_hash == epoch_rec.parent_hash;
                let epoch_committee_valid = epoch_committee_valid(&epoch_rec, &committee);
                let epoch_valid = epoch_rec.verify_with_cert(&cert);
                if parents_match && epoch_committee_valid && epoch_valid {
                    let epoch_hash = epoch_rec.digest();
                    // Capture final_consensus before save consumes epoch_rec.
                    let final_consensus = epoch_rec.final_consensus;
                    if let Err(e) = consensus_chain.epochs().save(epoch_rec, cert).await {
                        error!(
                            target: "epoch-manager",
                            ?e,
                            "failed to save epoch record/cert for epoch {epoch}",
                        );
                        return epoch.saturating_sub(1);
                    }
                    result_epoch = epoch;
                    info!(
                        target: "epoch-manager",
                        "retrieved cert for epoch {epoch}: {epoch_hash} from a peer",
                    );
                    // Track the highest final_consensus across downloaded epochs.
                    if final_consensus.hash != ConsensusHeaderDigest::default()
                        && final_consensus.number
                            > best_final_consensus.map(|(_, n, _)| n).unwrap_or(0)
                    {
                        best_final_consensus =
                            Some((epoch, final_consensus.number, final_consensus.hash));
                    }
                } else {
                    error!(
                        target: "epoch-manager",
                        ?parents_match,
                        ?epoch_committee_valid,
                        ?epoch_valid,
                        "got an invalid epoch record, epoch {epoch}",
                    );
                    return epoch.saturating_sub(1);
                }
            }
            Err(err) => {
                // We delibrately go past the latest epoch so this is expected to happen.
                info!(
                    target: "epoch-manager",
                    "failed to retrieve epoch from a peer {epoch}: {err}",
                );
            }
        }
        if result_epoch != epoch {
            break;
        }
    }
    if let Err(e) = consensus_chain.epochs().persist().await {
        error!(
            target: "epoch-manager",
            ?e,
            "failed to persist downloaded epoch record/certs",
        );
    }
    // Emit a single state-sync notification with the highest epoch's final consensus.
    // This unblocks nodes that missed a ConsensusResult gossip message due to a timing
    // gap (e.g. gossip arrived before the epoch record was available). We do this once
    // at the end rather than per-epoch to avoid flooding peers with concurrent requests.
    if let Some((epoch, number, hash)) = best_final_consensus {
        if consensus_bus.publish_consensus_num_hash_if_newer(epoch, number, hash) {
            info!(
                target: "epoch-manager",
                "updating last published consensus num hash up to epoch {result_epoch}, final consensus at block {number} ({hash}) - notifying state sync",
            );
        }
    }
    result_epoch
}

/// Sync the quorum-certified [`EpochRecord`] chain to the network tip, blocking until quiescent.
///
/// Called once during node startup, before the epoch loop begins, so startup decisions
/// (current committee, node role) read the network's certified view rather than stale local
/// state. Runs [`collect_epoch_records`] passes starting from epoch 0 (epochs already
/// complete locally are skipped cheaply) until two consecutive passes return the same epoch,
/// the second pass confirming nothing newer was available. Requires the epoch-0 record
/// (real or dummy) to already be in the DB so record verification can anchor on it.
///
/// This never hard-fails. It returns the last confirmed epoch when:
/// - two consecutive passes agree (the normal, quiescent exit),
/// - the node has no connected peers (e.g. the first node of a fresh network),
/// - a pass runs [`RECORD_SYNC_PASS_CAP`] without landing a new record in the DB — peers are
///   connected but not serving yet, e.g. a whole cohort booting or restarting at once,
/// - [`RECORD_SYNC_DEADLINE`] expires (a pass cancelled mid-flight is safe: collection is
///   verify-then-save and idempotent, and the epoch record collector re-runs it once spawned), or
/// - node shutdown is signaled.
pub async fn sync_epoch_records_to_tip(
    consensus_chain: &ConsensusChain,
    primary_handle: &PrimaryNetworkHandle,
    consensus_bus: &ConsensusBusApp,
    node_shutdown: Noticer,
) -> Epoch {
    let started = tokio::time::Instant::now();
    let mut previous_pass: Option<Epoch> = None;
    let mut latest: Epoch = 0;
    loop {
        // a node with no peers has no tip to sync toward (cold genesis boots alone)
        if primary_handle.connected_peers_count().await.unwrap_or(0) == 0 {
            info!(target: "epoch-manager", latest, "record sync: no connected peers, proceeding");
            return latest;
        }
        let remaining = RECORD_SYNC_DEADLINE.saturating_sub(started.elapsed());
        let pass_cap = RECORD_SYNC_PASS_CAP.min(remaining);
        // collect_epoch_records is not shutdown-aware, so select shutdown per pass
        let pass = tokio::select! {
            _ = &node_shutdown => return latest,
            result = tokio::time::timeout(
                pass_cap,
                collect_epoch_records(latest, consensus_chain, primary_handle, consensus_bus),
            ) => match result {
                Ok(reached) => reached,
                Err(_) => {
                    // the pass was cut mid-flight; records save as they verify, so the db
                    // shows whether it was progressing or starving on peers that are not
                    // serving yet (e.g. the whole cohort restarting at once)
                    let db_latest = consensus_chain
                        .epochs()
                        .latest_record()
                        .await
                        .map(|record| record.epoch)
                        .unwrap_or_default();
                    if db_latest > latest {
                        db_latest
                    } else {
                        warn!(target: "epoch-manager", latest, "record sync pass starved with no progress, proceeding");
                        return latest;
                    }
                }
            },
        };
        if record_sync_quiescent(previous_pass, pass, started.elapsed(), RECORD_SYNC_DEADLINE) {
            info!(target: "epoch-manager", epoch = pass, "record sync reached the network tip");
            return pass;
        }
        previous_pass = Some(pass);
        latest = pass;
    }
}

/// Decide whether the startup record-sync gate can stop after a completed pass.
///
/// Quiescence is *equality* across two consecutive passes: the latest pass confirmed
/// nothing new past the previous one. `<=` would be wrong here - a pass can return below
/// its input when it regresses to repair a missing parent record, and that repair must run
/// again rather than end the gate. The deadline unconditionally ends the gate so a flaky
/// peer set cannot stall startup.
fn record_sync_quiescent(
    previous_pass: Option<Epoch>,
    current_pass: Epoch,
    elapsed: Duration,
    deadline: Duration,
) -> bool {
    previous_pass == Some(current_pass) || elapsed >= deadline
}

/// Spawn a long running task to collect missing epoch records.
///
/// Most likely because a node is syncing.
pub async fn spawn_epoch_record_collector(
    consensus_chain: ConsensusChain,
    primary_handle: PrimaryNetworkHandle,
    consensus_bus: ConsensusBusApp,
    node_task_spawner: TaskSpawner,
    node_shutdown: Noticer,
) -> eyre::Result<()> {
    let mut epoch_rx = consensus_bus.requested_missing_epoch().subscribe();
    node_task_spawner.spawn_critical_task("Epoch Record Collector", async move {
        // Always start from epoch 0 so any gaps (epochs whose certs were never
        // saved, e.g. because the node was killed during a failed-quorum recovery)
        // are back-filled on restart.  Epochs that already have both a record and
        // a certificate are skipped immediately by get_epoch_by_number, so this
        // is cheap for nodes that are fully caught up.
        let mut last_epoch: Epoch = 0;
        loop {
            let requested_epoch = *epoch_rx.borrow_and_update();
            if requested_epoch >= last_epoch {
                last_epoch = collect_epoch_records(
                    last_epoch,
                    &consensus_chain,
                    &primary_handle,
                    &consensus_bus,
                )
                .await;
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;
    use tn_types::{BlockNumHash, BlsKeypair, ConsensusNumHash, B256};

    /// Generate a deterministic test BLS public key from a seed.
    fn test_bls_key(seed: u8) -> BlsPublicKey {
        let mut rng = ChaCha8Rng::from_seed([seed; 32]);
        *BlsKeypair::generate(&mut rng).public()
    }

    /// Create a test EpochRecord with the given committee.
    fn test_epoch_record(committee: Vec<BlsPublicKey>) -> EpochRecord {
        EpochRecord {
            epoch: 1,
            committee,
            next_committee: vec![],
            parent_hash: B256::ZERO.into(),
            final_state: BlockNumHash::default(),
            final_consensus: ConsensusNumHash::default(),
        }
    }

    #[test]
    fn test_epoch_committee_valid_equal_committees() {
        // When committees are equal in size, they must be exactly equal
        let keys: Vec<_> = (0..4).map(test_bls_key).collect();
        let epoch_rec = test_epoch_record(keys.clone());
        let committee: BTreeSet<_> = keys.into_iter().collect();

        assert!(epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_epoch_committee_valid_equal_but_different() {
        // Same size but different members should fail
        let epoch_keys: Vec<_> = (0..4).map(test_bls_key).collect();
        let other_keys: Vec<_> = (10..14).map(test_bls_key).collect();

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = other_keys.into_iter().collect();

        assert!(!epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_epoch_committee_valid_committee_smaller_than_epoch() {
        // If committee is smaller than epoch_rec.committee, always invalid
        let epoch_keys: Vec<_> = (0..4).map(test_bls_key).collect();
        let smaller_keys: Vec<_> = (0..3).map(test_bls_key).collect();

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = smaller_keys.into_iter().collect();

        assert!(!epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_epoch_committee_valid_committee_larger_valid() {
        // Committee larger but all epoch members present and epoch >= 4 and >= 2/3
        let epoch_keys: Vec<_> = (0..4).map(test_bls_key).collect();
        let mut larger_keys = epoch_keys.clone();
        larger_keys.push(test_bls_key(10)); // Add one more

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = larger_keys.into_iter().collect();

        // epoch_len=4, committee_len=5, 2/3 of 5 = 3, 4 >= 3 so valid
        assert!(epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_epoch_committee_valid_epoch_too_small() {
        // Epoch committee smaller than 4 is invalid (even if all present)
        let epoch_keys: Vec<_> = (0..3).map(test_bls_key).collect();
        let mut larger_keys = epoch_keys.clone();
        larger_keys.push(test_bls_key(10));
        larger_keys.push(test_bls_key(11));

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = larger_keys.into_iter().collect();

        // epoch_len=3 < 4, so invalid
        assert!(!epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_epoch_committee_valid_epoch_less_than_two_thirds() {
        // Epoch committee less than 2/3 of committee is invalid
        let epoch_keys: Vec<_> = (0..4).map(test_bls_key).collect();
        // Add many more keys to committee so epoch is < 2/3
        let mut larger_keys = epoch_keys.clone();
        for i in 10..20 {
            larger_keys.push(test_bls_key(i));
        }

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = larger_keys.into_iter().collect();

        // epoch_len=4, committee_len=14, 2/3 of 14 = 9, 4 < 9 so invalid
        assert!(!epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_epoch_committee_valid_member_not_in_committee() {
        // Epoch has a member not in committee - invalid
        let epoch_keys: Vec<_> = (0..4).map(test_bls_key).collect();
        let mut committee_keys: Vec<_> = (0..3).map(test_bls_key).collect();
        committee_keys.push(test_bls_key(10)); // Different key
        committee_keys.push(test_bls_key(11)); // Extra to make it larger

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = committee_keys.into_iter().collect();

        // epoch key 3 is not in committee
        assert!(!epoch_committee_valid(&epoch_rec, &committee));
    }

    #[test]
    fn test_record_sync_quiescent_advance_then_stable() {
        let deadline = Duration::from_secs(20);
        let early = Duration::from_secs(1);
        // the first pass has no predecessor to agree with
        assert!(!record_sync_quiescent(None, 5, early, deadline));
        // forward progress keeps the gate open
        assert!(!record_sync_quiescent(Some(5), 8, early, deadline));
        // two equal passes in a row -> quiescent
        assert!(record_sync_quiescent(Some(8), 8, early, deadline));
    }

    #[test]
    fn test_record_sync_quiescent_regression_keeps_gate_open() {
        let deadline = Duration::from_secs(20);
        let early = Duration::from_secs(1);
        // a pass can return below its predecessor when repairing a missing parent
        // record; that must not end the gate (why the rule is equality, not <=)
        assert!(!record_sync_quiescent(Some(8), 7, early, deadline));
        // and the repaired pass advancing again still keeps it open
        assert!(!record_sync_quiescent(Some(7), 9, early, deadline));
    }

    #[test]
    fn test_record_sync_quiescent_deadline_stops() {
        let deadline = Duration::from_secs(20);
        // the deadline ends the gate even when passes are still advancing
        assert!(record_sync_quiescent(Some(3), 9, Duration::from_secs(20), deadline));
        assert!(record_sync_quiescent(None, 0, Duration::from_secs(25), deadline));
    }

    #[test]
    fn test_epoch_committee_valid_boundary_two_thirds() {
        // Test exactly at 2/3 boundary
        let epoch_keys: Vec<_> = (0..6).map(test_bls_key).collect();
        let mut larger_keys = epoch_keys.clone();
        for i in 10..13 {
            larger_keys.push(test_bls_key(i));
        }

        let epoch_rec = test_epoch_record(epoch_keys);
        let committee: BTreeSet<_> = larger_keys.into_iter().collect();

        // epoch_len=6, committee_len=9, 2/3 of 9 = 6, 6 >= 6 so valid
        assert!(epoch_committee_valid(&epoch_rec, &committee));
    }
}
