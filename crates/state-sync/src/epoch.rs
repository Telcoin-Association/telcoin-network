//! Tasks and helpers for collecting epoch records trustlessly.

#[cfg(test)]
use tn_test_utils as _;

use std::time::Duration;

use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp};
use tn_storage::{consensus::ConsensusChain, epoch_records::EpochRecordValidation};
use tn_types::{ConsensusHeaderDigest, Epoch, Noticer, TaskSpawner};
use tracing::{debug, error, info};

/// How long to wait before retrying a failed epoch record collection.
const EPOCH_COLLECT_RETRY_SECS: u64 = 5;

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
                // Validate the downloaded record against the locally-trusted committee using the
                // same routine the failed-quorum recovery path uses (see crates/node
                // epoch_votes.rs), so a record cannot be accepted under weaker rules on one path
                // than the other.
                match consensus_chain
                    .epochs()
                    .validate_downloaded_record(epoch, &epoch_rec, &cert)
                    .await
                {
                    EpochRecordValidation::Valid => {
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
                            "got an invalid epoch record, epoch {epoch}",
                        );
                        return epoch.saturating_sub(1);
                    }
                    EpochRecordValidation::NoAnchor => {
                        // We are missing the previous epoch record (or the genesis committee), so
                        // this record cannot be anchored. Reduce last_epoch by one and retry once
                        // the anchor is available.
                        return epoch.saturating_sub(1);
                    }
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
