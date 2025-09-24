//! Tasks and helpers for collecting consensus headers trustlessly.

use tn_config::ConsensusConfig;
use tn_primary::{network::PrimaryNetworkHandle, ConsensusBus};
use tn_storage::{
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocksCache},
    ConsensusStore,
};
use tn_types::{Database as TNDatabase, DbTxMut as _, B256};
use tracing::{debug, error};

/// Retrieve a consensus header from a peer.
/// If we are requesting a hash then that hash should
/// have already been "validated" so the only check we
/// make is that the returned header matches the hash.
async fn get_consensus_header<DB: TNDatabase>(
    hash: B256,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    network: &PrimaryNetworkHandle,
) -> Option<B256> {
    let db = config.node_storage();
    if let Some(block) = db.get_consensus_by_hash(hash) {
        return Some(block.parent_hash);
    }
    // request consensus from any peer
    if let Ok(header) = network.request_consensus(None, Some(hash)).await {
        // The header we got will match hash (request_consensus() contract).
        let parent = header.parent_hash;
        eprintln!("XXXX got consensus {hash}");
        match db.write_txn() {
            Ok(mut txn) => {
                if let Err(e) = txn.insert::<ConsensusBlocksCache>(&header.number, &header) {
                    error!(target: "state-sync", ?e, "error saving a consensus header to persistant storage!");
                }
                if let Err(e) =
                    txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)
                {
                    error!(target: "state-sync", ?e, "error saving a consensus header number to persistant storage!");
                }
                if let Err(e) = txn.commit() {
                    error!(target: "state-sync", ?e, "error saving committing to persistant storage!");
                }
            }
            Err(e) => {
                error!(target: "state-sync", ?e, "error getting a transaction on persistant storage!");
            }
        }
        if header.number > consensus_bus.last_consensus_header().borrow().number {
            //XXXX - Update our last seen valid consensus header if it is newer.
            let _ = consensus_bus.last_consensus_header().send(header);
        }
        Some(parent)
    } else {
        None
    }
}

/// Spawn a long running task on task_manager that will keep the last_consensus_header watch on
/// consensus_bus up to date. This should only be used when NOT participating in active consensus.
pub(crate) async fn spawn_track_recent_consensus<DB: TNDatabase>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
) -> eyre::Result<()> {
    let rx_shutdown = config.shutdown().subscribe();
    let mut rx_gossip_update = consensus_bus.last_published_consensus_num_hash().subscribe();
    let mut next = None;
    loop {
        while let Some(hash) = next {
            next = get_consensus_header(hash, &config, &consensus_bus, &network).await;
            tokio::task::yield_now().await;
        }
        tokio::select! {
            _ = rx_gossip_update.changed() => {
                let (number, hash) = *rx_gossip_update.borrow_and_update();
                debug!(target: "state-sync", ?number, ?hash, "tracking recent consensus and detected change through gossip - requesting consensus from peer");

                next = get_consensus_header(hash, &config, &consensus_bus, &network).await;
            }

            _ = &rx_shutdown => {
                return Ok(())
            }
        }
    }
}
