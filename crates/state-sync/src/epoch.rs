//! Tasks and helpers for collecting epoch records trustlessly.

#[cfg(test)]
use tn_test_utils as _;

use std::collections::BTreeSet;

use tn_primary::{network::PrimaryNetworkHandle, ConsensusBus};
use tn_storage::{tables::EpochRecords, EpochStore as _};
use tn_types::{
    BlsPublicKey, Database as TNDatabase, Epoch, EpochRecord, Noticer, TaskSpawner, B256,
};
use tracing::info;

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
async fn collect_epoch_records<DB>(
    last_epoch: Epoch,
    db: &DB,
    primary_handle: &PrimaryNetworkHandle,
) -> Epoch
where
    DB: TNDatabase,
{
    let mut result_epoch = last_epoch;
    for epoch in last_epoch.. {
        // If we already have epoch record AND it's certificate then continue.
        if let Some((_, Some(_))) = db.get_epoch_by_number(epoch) {
            continue;
        }
        // Try to recover by downloading the epoch record and cert from a peer.
        match primary_handle.request_epoch_cert(Some(epoch), None).await {
            Ok((epoch_rec, cert)) => {
                let (parent_hash, committee) = if epoch == 0 {
                    // If we can't find the genesis committee something is very wrong.
                    let committee =
                        db.get_committee_keys(0).expect("always can retrieve epoch 0 committee");
                    (B256::default(), committee)
                } else if let Ok(Some(prev)) = db.get::<EpochRecords>(&(epoch - 1)) {
                    (prev.digest(), prev.next_committee.iter().copied().collect())
                } else {
                    // We are missing epoch records.
                    // Should not be here but if so just skipping won't really help...
                    // Reduce last_epoch by one and once this loop finishes skipping we can
                    // try to get the missing epoch again.
                    return epoch - 1;
                };
                // Verify the epoch has the expected parent and committee and is signed by
                // that committee.
                if parent_hash == epoch_rec.parent_hash
                    && epoch_committee_valid(&epoch_rec, &committee)
                    && epoch_rec.verify_with_cert(&cert)
                {
                    let epoch_hash = epoch_rec.digest();
                    db.save_epoch_record_with_cert(&epoch_rec, &cert);
                    result_epoch = epoch;
                    info!(
                        target: "epoch-manager",
                        "retrieved cert for epoch {epoch}: {epoch_hash} from a peer",
                    );
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
    result_epoch
}

/// Spawn a long running task to collect missing epoch records.
///
/// Most likely because a node is syncing.
pub async fn spawn_epoch_record_collector<DB>(
    db: DB,
    primary_handle: PrimaryNetworkHandle,
    consensus_bus: ConsensusBus,
    node_task_spawner: TaskSpawner,
    node_shutdown: Noticer,
) -> eyre::Result<()>
where
    DB: TNDatabase,
{
    let mut epoch_rx = consensus_bus.requested_missing_epoch().subscribe();
    node_task_spawner.spawn_critical_task("Epoch Record Collector", async move {
        let mut last_epoch = if let Some((last_epoch, _)) = db.last_record::<EpochRecords>() {
            last_epoch
        } else {
            0
        };
        loop {
            let requested_epoch = *epoch_rx.borrow();
            if requested_epoch > last_epoch {
                last_epoch = collect_epoch_records(last_epoch, &db, &primary_handle).await;
                if last_epoch < requested_epoch {
                    // Small sanity check in case someone sends a malicious large epoch restore to
                    // sanity.
                    consensus_bus.requested_missing_epoch().send_replace(last_epoch);
                }
            }
            // Wait until the watch is updated to indicate we have more work to do.
            tokio::select!(
                _ = &node_shutdown => {
                    break;  // Break the outer loop.
                },
                _ = epoch_rx.changed() => { }
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
    use tn_types::{BlockNumHash, BlsKeypair};

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
            parent_hash: B256::ZERO,
            final_state: BlockNumHash::default(),
            final_consensus: BlockNumHash::default(),
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
}
