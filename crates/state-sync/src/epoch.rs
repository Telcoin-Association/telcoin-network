//! Tasks and helpers for collecting epoch records trustlessly.

#[cfg(test)]
use tn_test_utils as _;

use std::{collections::BTreeSet, time::Duration};

use tn_primary::{network::PrimaryNetworkHandle, ConsensusBusApp};
use tn_storage::consensus::ConsensusChain;
use tn_types::{
    BlsPublicKey, ConsensusHeaderDigest, Epoch, EpochDigest, EpochRecord, Noticer, TaskSpawner,
};
use tracing::{debug, error, info};

/// How long to wait before retrying a failed epoch record collection.
const EPOCH_COLLECT_RETRY_SECS: u64 = 5;

/// Return true if the record's committee is compatible with `committee` (the
/// committee expected from the previous epoch record).
/// These will usually be equal but it is possible for a validator to be
/// booted (ejected on-chain) mid-epoch and still be in `committee` while
/// missing from `epoch_rec.committee`.
/// Delegates to the shared [`EpochRecord::committee_compatible`] predicate so
/// this verifier and the epoch record producer accept exactly the same shapes.
fn epoch_committee_valid(epoch_rec: &EpochRecord, committee: &BTreeSet<BlsPublicKey>) -> bool {
    epoch_rec.committee_compatible(committee)
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
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;
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
        encode, BlockNumHash, BlsAggregateSignature, BlsKeypair, BlsSignature, ConsensusNumHash,
        EpochCertificate, Intent, IntentMessage, IntentScope, Signer as _, B256,
    };
    use tokio::sync::mpsc;

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
            // final_consensus must strictly increase across epochs (see
            // `EpochRecordDb::update_finals`); rec0 left it at the zero default, so rec1 needs a
            // higher number even though this test isn't exercising consensus-number tracking.
            final_consensus: ConsensusNumHash::new(1, ConsensusHeaderDigest::default()),
            ..Default::default()
        };
        let cert1 = make_cert(&rec1, &shrunken_signers, &[0, 1, 2]);
        assert!(rec1.verify_with_cert(&cert1), "3-of-4 cert must verify");
        let rec2 = EpochRecord {
            epoch: 2,
            committee: shrunken.clone(),
            next_committee: shrunken.clone(),
            parent_hash: rec1.digest(),
            final_consensus: ConsensusNumHash::new(2, ConsensusHeaderDigest::default()),
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
