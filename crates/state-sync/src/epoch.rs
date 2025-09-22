//! Tasks and helpers for collecting epoch records trustlessly.

use tn_config::TelcoinDirs;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBus};
use tn_storage::{tables::EpochRecords, EpochStore as _};
use tn_types::{Database as TNDatabase, Epoch, Noticer, TaskSpawner, B256};
use tracing::{error, info};

/// Asks peers for records from last_epoch to requested_epoch.
/// Returns the Epoch that was last retrieved.
async fn collect_epoch_records<DB, P>(
    last_epoch: Epoch,
    requested_epoch: Epoch,
    db: &DB,
    primary_handle: &PrimaryNetworkHandle,
    _datadir: &P, //XXXX
) -> Epoch
where
    P: TelcoinDirs + Clone + 'static,
    DB: TNDatabase,
{
    let mut result_epoch = last_epoch;
    for epoch in last_epoch..=requested_epoch {
        // If we already have epoch then continue.
        if let Ok(Some(_)) = db.get::<EpochRecords>(&(epoch)) {
            continue;
        }
        // Try to recover by downloading the epoch record and cert from a peer.
        for _ in 0..3 {
            match primary_handle.request_epoch_cert(Some(epoch), None).await {
                Ok((epoch_rec, cert)) => {
                    let (parent_hash, committee) = if epoch == 0 {
                        // If we can't find the genesis committee something is very wrong (i.e.
                        // we have a broken config, etc).
                        let committee = db
                            .get_committee_keys(0)
                            .expect("always can retreive epoch 0 committee");
                        (B256::default(), committee)
                    } else if let Ok(Some(prev)) = db.get::<EpochRecords>(&(epoch - 1)) {
                        (prev.digest(), prev.next_committee.clone())
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
                        && committee == epoch_rec.committee  // XXXX if a validator is booted could this fail?
                        && epoch_rec.verify_with_cert(&cert)
                    {
                        let epoch_hash = epoch_rec.digest();
                        db.save_epoch_record_with_cert(&epoch_rec, &cert);
                        result_epoch = epoch;
                        info!(
                            target: "epoch-manager",
                            "retrieved cert for epoch {epoch}: {epoch_hash} from a peer",
                        );
                        break; // 0..3
                    }
                }
                Err(err) => error!(
                    target: "epoch-manager",
                    "failed to retrieve epoch from a peer {epoch}: {err}",
                ),
            }
        }
    }
    result_epoch
}

/// Spawn a long running task to collect missing epoc records.
///
/// Most likely because a node is syncing.
pub async fn spawn_epoch_record_collector<DB, P>(
    db: DB,
    primary_handle: PrimaryNetworkHandle,
    datadir: P,
    consensus_bus: ConsensusBus,
    node_task_spawner: TaskSpawner,
    node_shutdown: Noticer,
) -> eyre::Result<()>
where
    P: TelcoinDirs + Clone + 'static,
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
                last_epoch = collect_epoch_records(
                    last_epoch,
                    requested_epoch,
                    &db,
                    &primary_handle,
                    &datadir,
                )
                .await;
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
