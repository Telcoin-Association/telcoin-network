//! Code to sync consensus state between peers.
//! Currently used by nodes that are not participating in consensus
//! to follow along with consensus and execute blocks.

use std::time::Duration;

use consensus_metrics::monitored_future;
use futures::{stream::FuturesUnordered, StreamExt};
use tn_config::ConsensusConfig;
use tn_network_libp2p::PeerId;
use tn_primary::{
    consensus::ConsensusRound, network::PrimaryNetworkHandle, ConsensusBus, NodeMode,
};
use tn_storage::tables::{Batches, ConsensusBlockNumbersByDigest, ConsensusBlocks};
use tn_types::{ConsensusHeader, ConsensusOutput, Database, DbTxMut, TaskManagerClone, TnSender};
use tracing::info;

/// Return true if this node should be able to participate as a CVV, false otherwise.
///
/// Call this if you should be a committe member.  Currently it will determine if you have recent
/// enough DAG information to rejoin consensus or not.
/// This function also sets some of the round watches on the consensus bus to proper defaults on
/// startup.
pub async fn can_cvv<DB: Database>(
    consensus_bus: ConsensusBus,
    config: ConsensusConfig<DB>,
    network: PrimaryNetworkHandle,
) -> bool {
    // Get the DB and load our last executed consensus block (note there may be unexecuted
    // blocks, catch up will execute them).
    let last_executed_block =
        last_executed_consensus_block(&consensus_bus, &config).unwrap_or_default();

    // Set some of the round watches to the current default.
    // TODO- replace 0 with the epoch once we have them..
    let last_consensus_epoch = last_executed_block.sub_dag.leader.epoch();
    let last_consensus_round = last_executed_block.sub_dag.leader_round();
    let _ = consensus_bus.update_consensus_rounds(ConsensusRound::new_with_gc_depth(
        last_consensus_round,
        config.parameters().gc_depth,
    ));
    let _ = consensus_bus.primary_round_updates().send(last_consensus_round);

    let max_consensus_header = max_consensus_header_from_committee(&network, &config)
        .await
        .unwrap_or_else(|| last_executed_block.clone());
    let max_epoch = max_consensus_header.sub_dag.leader.epoch();
    let max_round = max_consensus_header.sub_dag.leader.round();
    let _ = consensus_bus.last_consensus_header().send(max_consensus_header);
    tracing::info!(target: "telcoin::subscriber",
        "CATCH UP params {max_epoch}, {max_round}, leader epoch: {last_consensus_epoch}, leader round: {last_consensus_round}, gc: {}",
        config.parameters().gc_depth
    );
    if max_epoch == last_consensus_epoch
        && (last_consensus_round + config.parameters().gc_depth) > max_round
    {
        tracing::info!(target: "telcoin::subscriber", "Node is attempting to rejoin consensus.");
        // We should be able to pick up consensus where we left off.
        true
    } else {
        tracing::info!(target: "telcoin::subscriber", "Node has fallen to far behind to rejoin consensus, just following now.");
        false
    }
}

/// Spawn the state sync tasks.
pub fn spawn_state_sync<DB: Database>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
    task_manager: TaskManagerClone,
) {
    let mode = *consensus_bus.node_mode().borrow();
    match mode {
        // If we are active then partcipate in consensus.
        NodeMode::CvvActive => {}
        NodeMode::CvvInactive | NodeMode::Observer => {
            // If we are not an active CVV then follow latest consensus from peers.
            let (config_clone, consensus_bus_clone) = (config.clone(), consensus_bus.clone());
            let network_clone = network.clone();
            task_manager.spawn_task(
                "state sync: track latest consensus header from peers",
                monitored_future!(
                    async move {
                        info!(target: "telcoin::state-sync", "Starting state sync: track latest consensus header from peers");
                        spawn_track_recent_consensus(config_clone, consensus_bus_clone, network_clone).await
                    },
                    "StateSyncLatestConsensus"
                ),
            );
            task_manager.spawn_task(
                "state sync: stream consensus headers",
                monitored_future!(
                    async move {
                        info!(target: "telcoin::state-sync", "Starting state sync: stream consensus header from peers");
                        spawn_stream_consensus_headers(config, consensus_bus, network).await
                    },
                    "StateSyncStreamConsensusHeaders"
                ),
            );
        }
    }
}

/// Write the consensus header and it's component transaction batches to the consensus DB.
///
/// An error here indicates a critical node failure.
/// Note, if this returns an error then the DB could not be written to- this is probably fatal.
pub fn save_consensus<DB: Database>(
    db: &DB,
    consensus_output: ConsensusOutput,
) -> eyre::Result<()> {
    match db.write_txn() {
        Ok(mut txn) => {
            for batch in consensus_output.batches.iter().flatten() {
                if let Err(e) = txn.insert::<Batches>(&batch.digest(), batch) {
                    tracing::error!(target: "telcoin::state-sync", ?e, "error saving a batch to persistant storage!");
                    return Err(e);
                }
            }
            let header: ConsensusHeader = consensus_output.into();
            if let Err(e) = txn.insert::<ConsensusBlocks>(&header.number, &header) {
                tracing::error!(target: "telcoin::state-sync", ?e, "error saving a consensus header to persistant storage!");
                return Err(e);
            }
            if let Err(e) =
                txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)
            {
                tracing::error!(target: "telcoin::state-sync", ?e, "error saving a consensus header number to persistant storage!");
                return Err(e);
            }
            if let Err(e) = txn.commit() {
                tracing::error!(target: "telcoin::state-sync", ?e, "error saving committing to persistant storage!");
                return Err(e);
            }
        }
        Err(e) => {
            tracing::error!(target: "telcoin::state-sync", ?e, "error getting a transaction on persistant storage!");
            return Err(e);
        }
    }
    Ok(())
}

/// Returns the ConsensusHeader that created the last executed block if can be found.
/// If we are not starting up then not finding this indicates a database issue.
pub fn last_executed_consensus_block<DB: Database>(
    consensus_bus: &ConsensusBus,
    config: &ConsensusConfig<DB>,
) -> Option<ConsensusHeader> {
    if let Some(last_executed_consensus_hash) =
        consensus_bus.recent_blocks().borrow().latest_block().header().parent_beacon_block_root
    {
        let db = config.node_storage();
        if let Ok(Some(number)) =
            db.get::<ConsensusBlockNumbersByDigest>(&last_executed_consensus_hash)
        {
            if let Ok(Some(block)) = db.get::<ConsensusBlocks>(&number) {
                Some(block)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    }
}

/// Send any consensus headers that were not executed before last shutdown to the consensus header
/// channel.
pub async fn stream_missing_consensus<DB: Database>(
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
) -> eyre::Result<()> {
    // Get the DB and load our last executed consensus block.
    let last_executed_block =
        last_executed_consensus_block(consensus_bus, config).unwrap_or_default();
    // Edge case, in case we don't hear from peers but have un-executed blocks...
    // Not sure we should handle this, but it hurts nothing.
    let db = config.node_storage();
    let (_, last_db_block) = db
        .last_record::<ConsensusBlocks>()
        .unwrap_or_else(|| (last_executed_block.number, last_executed_block.clone()));
    if last_db_block.number > last_executed_block.number {
        for consensus_block_number in last_executed_block.number + 1..=last_db_block.number {
            if let Some(consensus_header) = db.get::<ConsensusBlocks>(&consensus_block_number)? {
                consensus_bus.consensus_header().send(consensus_header).await?;
            }
        }
    }
    Ok(())
}

/// Collect and return any consensus headers that were not executed before last shutdown to the
/// consensus header channel.
pub async fn get_missing_consensus<DB: Database>(
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
) -> eyre::Result<Vec<ConsensusHeader>> {
    let mut result = Vec::new();
    // Get the DB and load our last executed consensus block.
    let last_executed_block =
        last_executed_consensus_block(consensus_bus, config).unwrap_or_default();
    // Edge case, in case we don't hear from peers but have un-executed blocks...
    // Not sure we should handle this, but it hurts nothing.
    let db = config.node_storage();
    let (_, last_db_block) = db
        .last_record::<ConsensusBlocks>()
        .unwrap_or_else(|| (last_executed_block.number, last_executed_block.clone()));
    if last_db_block.number > last_executed_block.number {
        for consensus_block_number in last_executed_block.number + 1..=last_db_block.number {
            if let Some(consensus_header) = db.get::<ConsensusBlocks>(&consensus_block_number)? {
                result.push(consensus_header);
            }
        }
    }
    Ok(result)
}

/// Spawn a long running task on task_manager that will keep the last_consensus_header watch on
/// consensus_bus up to date. This should only be used when NOT participating in active consensus.
async fn spawn_track_recent_consensus<DB: Database>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
) -> eyre::Result<()> {
    let rx_shutdown = config.shutdown().subscribe();
    let mut rx_gossip_update = consensus_bus.last_published_consensus_num_hash().subscribe();
    loop {
        tokio::select! {
            _ = rx_gossip_update.changed() => {
                let (number, _hash) = *rx_gossip_update.borrow();
                if let Ok(header) = network.request_consensus(Some(number), None).await {
                    match header.verify_certificates(config.committee()) {
                        Ok(header) => {
                            if header.number > consensus_bus.last_consensus_header().borrow().number {
                                consensus_bus.last_consensus_header().send(header)?;
                            }
                        }
                        Err(e) => {
                            tracing::error!(target: "telcoin::state-sync", "recieved a consensus header with invalid certs: {e}");
                        }
                    }
                }
            }
            _ = &rx_shutdown => {
                return Ok(())
            }
        }
    }
}

/// Spawn a long running task on task_manager that will stream consensus headers from the
/// last saved to the current and then keep up with current headers.
/// This should only be used when NOT participating in active consensus.
async fn spawn_stream_consensus_headers<DB: Database>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: PrimaryNetworkHandle,
) -> eyre::Result<()> {
    let rx_shutdown = config.shutdown().subscribe();

    let mut rx_last_consensus_header = consensus_bus.last_consensus_header().subscribe();
    //let mut last_consensus_header = catch_up_consensus(&network, &config, &consensus_bus).await?;
    let db = config.node_storage();
    let (_, mut last_consensus_header) =
        db.last_record::<ConsensusBlocks>().unwrap_or_else(|| (0, ConsensusHeader::default()));
    let mut last_consensus_height = last_consensus_header.number;
    // infinite loop over consensus output
    loop {
        tokio::select! {
            _ = rx_last_consensus_header.changed() => {
                let header = rx_last_consensus_header.borrow().clone();
                if header.number > last_consensus_height {
                    last_consensus_header = catch_up_consensus_from_to(
                        &network,
                        &config,
                        &consensus_bus,
                        last_consensus_header,
                        header,
                    )
                    .await?;
                    last_consensus_height = last_consensus_header.number;
                }
            }
            _ = &rx_shutdown => {
                return Ok(())
            }
        }
    }
}

/// Returns the latest consensus header retrieved from a committee member.
/// Note: this is only for use by committee members, otherwise they may not be peers (used by
/// can_cvv).
///
/// Will allow three seconds per client and three attempts to get the consensus info.
async fn max_consensus_header_from_committee<DB: Database>(
    network: &PrimaryNetworkHandle,
    config: &ConsensusConfig<DB>,
) -> Option<ConsensusHeader> {
    let peers = get_peers(config);
    let committee = config.committee();
    let mut result: Option<ConsensusHeader> = None;
    let mut waiting = FuturesUnordered::new();
    // Ask all our peers for their latest consensus height.
    for peer in peers.iter() {
        waiting.push(tokio::time::timeout(
            Duration::from_secs(3), /* Three seconds should be plenty of time to get the
                                     * consensus header. */
            network.request_consensus_from_peer(*peer, None, None),
        ));
    }
    while let Some(res) = waiting.next().await {
        match res {
            Ok(Ok(consensus_header)) => {
                // Validate all the certificates in this consensus header.
                let consensus_header = consensus_header.verify_certificates(committee).ok()?;
                result = if let Some(last) = result {
                    let (epoch, last_epoch) =
                        (consensus_header.sub_dag.leader.epoch(), last.sub_dag.leader.epoch());
                    let (round, last_round) =
                        (consensus_header.sub_dag.leader.round(), last.sub_dag.leader.round());
                    if epoch > last_epoch || (epoch == last_epoch && round > last_round) {
                        Some(consensus_header)
                    } else {
                        Some(last)
                    }
                } else {
                    Some(consensus_header)
                };
            }
            Ok(Err(e)) => {
                // An error with one peer should not derail us...  But log it.
                tracing::error!(target: "telcoin::state-sync", "error requesting peer consensus {e:?}")
            }
            Err(e) => {
                // An error with one peer should not derail us...  But log it.
                tracing::error!(target: "telcoin::state-sync", "error requesting peer consensus {e:?}")
            }
        }
    }
    result
}

/// Get a vector of ids for each peer.
fn get_peers<DB: Database>(config: &ConsensusConfig<DB>) -> Vec<PeerId> {
    config
        .committee()
        .others_primaries_by_id(&config.authority().id())
        .into_iter()
        .map(|(auth_id, _, _)| auth_id.peer_id())
        .collect()
}

/// Applies consensus output "from" (exclusive) to height "max_consensus_height" (inclusive).
/// Queries peers for latest height and downloads and executes any missing consensus output.
/// Returns the last ConsensusHeader that was applied on success.
async fn catch_up_consensus_from_to<DB: Database>(
    network: &PrimaryNetworkHandle,
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    from: ConsensusHeader,
    max_consensus: ConsensusHeader,
) -> eyre::Result<ConsensusHeader> {
    // Note use last_executed_block here because
    let mut last_parent = from.digest();

    // Catch up to the current chain state if we need to.
    let last_consensus_height = from.number;
    let max_consensus_height = max_consensus.number;
    if last_consensus_height >= max_consensus_height {
        return Ok(from);
    }
    let db = config.node_storage();
    let mut result_header = from;
    for number in last_consensus_height + 1..=max_consensus_height {
        tracing::debug!(target: "telcoin::state-sync", "trying to get consensus block {number}");
        // Check if we already have this consensus output in our local DB.
        // This will also allow us to pre load other consensus blocks as a future
        // optimization.
        let consensus_header = if number == max_consensus_height {
            max_consensus.clone()
        } else if let Ok(Some(block)) = db.get::<ConsensusBlocks>(&number) {
            block
        } else {
            let mut try_num = 0;
            loop {
                if try_num > 3 {
                    return Err(eyre::eyre!("unable to read a valid consensus header!"));
                }
                let header = network.request_consensus(Some(number), None).await?;
                // Validate all the certificates in this consensus header.
                match header.verify_certificates(config.committee()) {
                    Ok(header) => break header,
                    Err(e) => {
                        tracing::error!(target: "telcoin::state-sync", "received an invalid consensus header {e:?}");
                        try_num += 1;
                    }
                }
            }
        };
        let parent_hash = last_parent;
        last_parent =
            ConsensusHeader::digest_from_parts(parent_hash, &consensus_header.sub_dag, number);
        if last_parent != consensus_header.digest() {
            tracing::error!(target: "telcoin::state-sync", "consensus header digest mismatch!");
            return Err(eyre::eyre!("consensus header digest mismatch!"));
        }

        let base_execution_block = consensus_header.sub_dag.leader.header().latest_execution_block;
        // We need to make sure execution has caught up so we can verify we have not
        // forked. This will force the follow function to not outrun
        // execution...  this is probably fine. Also once we can
        // follow gossiped consensus output this will not really be
        // an issue (except during initial catch up).
        if consensus_bus.wait_for_execution(base_execution_block).await.is_err() {
            // We seem to have forked, so die.
            return Err(eyre::eyre!(
                "consensus_output has a parent not in our chain, missing {base_execution_block:?} recents: {:?}!",
                consensus_bus.recent_blocks().borrow()
            ));
        }
        consensus_bus.consensus_header().send(consensus_header.clone()).await?;
        result_header = consensus_header;
    }
    Ok(result_header)
}
