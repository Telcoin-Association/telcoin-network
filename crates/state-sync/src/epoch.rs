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
            // Don't know the output bytes so use 0.  If we could get them (already have the output)
            // we won't need them...
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
        // Don't know the output bytes so use 0.  If we could get them (already have the output) we
        // won't need them...
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
    /// Requests for epochs not in `served` drop the reply channel so the
    /// collector sees the request fail (as when no peer has the record).
    fn mock_epoch_record_network(
        served: HashMap<Epoch, (EpochRecord, EpochCertificate)>,
        peer: BlsPublicKey,
    ) -> PrimaryNetworkHandle {
        let (tx, mut rx) = mpsc::channel(100);
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                if let NetworkCommand::SendRequestAny { request, reply } = cmd {
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
        let handle = mock_epoch_record_network(served, keys[0]);
        let consensus_bus = ConsensusBus::new();

        let collected =
            collect_epoch_records(0, &consensus_chain, &handle, consensus_bus.app()).await;
        assert_eq!(collected, 2, "collector should fetch through the ejection epoch");

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
        let handle = mock_epoch_record_network(served, keys[0]);
        let consensus_bus = ConsensusBus::new();

        let collected =
            collect_epoch_records(0, &consensus_chain, &handle, consensus_bus.app()).await;
        assert_eq!(collected, 0, "collector must stop at the invalid record");

        // The rejected record was never saved.
        assert!(consensus_chain.epochs().get_epoch_by_number(1).await.is_none());
    }
}
