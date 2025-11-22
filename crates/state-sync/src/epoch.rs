//! Tasks and helpers for collecting epoch records trustlessly.

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
                        db.get_committee_keys(0).expect("always can retreive epoch 0 committee");
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
                    let _ = consensus_bus.requested_missing_epoch().send(last_epoch);
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
