//! Code to sync consensus state between peers.
//! Currently used by nodes that are not participating in consensus
//! to follow along with consensus and execute blocks.
//!
//! This code will be re-written to work with libp2p and should become
//! less hacky.

use std::{collections::HashMap, time::Duration};

use consensus_metrics::monitored_future;
use futures::{stream::FuturesUnordered, StreamExt};
use tn_config::ConsensusConfig;
use tn_network::anemo_ext::{NetworkExt, WaitingPeer};
use tn_network_types::{ConsensusOutputRequest, PrimaryToPrimaryClient};
use tn_primary::{consensus::ConsensusRound, ConsensusBus, NodeMode};
use tn_storage::{
    tables::{Batches, ConsensusBlockNumbersByDigest, ConsensusBlocks},
};
use tn_types::{ConsensusHeader, ConsensusOutput, Database, DbTxMut, TaskManagerClone, TnSender};
use tracing::info;

/// Return true if this node should be able to participate as a CVV, false otherwise.
///
/// Call this if you should be a committe member.  Currently it will determine if you have recent
/// enough DAG information to rejoin consensus or not.
/// This logic will change in the future, currently if you can not re-join consensus then you can
/// only follow now.
/// This function also sets some of the round watches on the consensus bus to proper defaults on
/// startup.
pub async fn can_cvv<DB: Database>(
    consensus_bus: ConsensusBus,
    config: ConsensusConfig<DB>,
    network: anemo::Network,
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

    let mut clients: Vec<PrimaryToPrimaryClient<_>> = config
        .committee()
        .others_primaries_by_id(config.authority().id())
        .into_iter()
        .map(|(_, _, peer_id)| {
            PrimaryToPrimaryClient::new(network.waiting_peer(anemo::PeerId(peer_id.0.to_bytes())))
        })
        .collect();
    let max_consensus_header =
        max_consensus_header(&mut clients).await.unwrap_or_else(|| last_executed_block.clone());
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
    network: anemo::Network,
    task_manager: TaskManagerClone,
) {
    let mode = *consensus_bus.node_mode().borrow();
    match mode {
        // If we are active then partcipate in consensus.
        NodeMode::CvvActive => {}
        NodeMode::CvvInactive | NodeMode::Observer => {
            // If we are not an active CVV then follow latest consensus from peers.
            let (config_clone, consensus_bus_clone) = (config.clone(), consensus_bus.clone());
            task_manager.spawn_task(
                "state sync: track latest consensus header from peers",
                monitored_future!(
                    async move {
                        info!(target: "telcoin::state-sync", "Starting state sync: track latest consensus header from peers");
                        spawn_track_recent_consensus(config_clone, consensus_bus_clone).await
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

/// Write the consensus header and it's digest to number index to the consensus DB.
///
/// An error here indicates a critical node failure.
/// Note this function both logs and returns the error due to it's severity.
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

pub fn last_executed_consensus_block<DB: Database>(
    consensus_bus: &ConsensusBus,
    config: &ConsensusConfig<DB>,
) -> Option<ConsensusHeader> {
    if let Some(last_executed_consensus_hash) =
        consensus_bus.recent_blocks().borrow().latest_block().header().parent_beacon_block_root
    {
        let db = config.database();
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
    let db = config.database();
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

/// Send any consensus headers that were not executed before last shutdown to the consensus header
/// channel.
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
    let db = config.database();
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
///
/// NOTE: current implementation is a no-op, this is handled by the consensus header stream task.
/// Expect to have a gossip channel with libp2p soon that will make this task useful so this is a
/// placeholder for now.
async fn spawn_track_recent_consensus<DB: Database>(
    config: ConsensusConfig<DB>,
    _consensus_bus: ConsensusBus,
) -> eyre::Result<()> {
    let rx_shutdown = config.shutdown().subscribe();
    loop {
        tokio::select! {
            _ = std::future::pending() => {}
            _ = &rx_shutdown => {
                return Ok(())
            }
        }
    }
}

/// Spawn a long running task on task_manager that will keep stream consensus headers from the
/// last saved to the current and then keep up with current headers.
/// This should only be used when NOT participating in active consensus.
async fn spawn_stream_consensus_headers<DB: Database>(
    config: ConsensusConfig<DB>,
    consensus_bus: ConsensusBus,
    network: anemo::Network,
) -> eyre::Result<()> {
    let mut clients = get_clients(&config, &network);
    let rx_shutdown = config.shutdown().subscribe();

    let mut last_consensus_header =
        catch_up_consensus(&config, &consensus_bus, &mut clients).await?;
    let mut last_consensus_height = last_consensus_header.number;
    // infinite loop over consensus output
    loop {
        // Rest for bit then try see if chain has advanced and catch up if so.
        tokio::select! {
            _ = tokio::time::sleep(config.parameters().min_header_delay) => {}
            _ = &rx_shutdown => {
                return Ok(())
            }
        }
        tokio::select! {
            header = max_consensus_header(&mut clients) => {
                match header {
                    Some(max_consensus) => {
                        consensus_bus.last_consensus_header().send(max_consensus.clone())?;
                        if max_consensus.number > last_consensus_height {
                            consensus_bus.last_consensus_header().send(max_consensus.clone())?;
                            last_consensus_header = catch_up_consensus_from_to(
                                &config,
                                &consensus_bus,
                                &mut clients,
                                last_consensus_header,
                                max_consensus,
                            )
                            .await?;
                            last_consensus_height = last_consensus_header.number;
                        }
                    }
                    None => {
                        tracing::error!(target: "telcoin::state-sync", "failed to get the latest max consensus header from peers!");
                        continue;
                    }
                }
            }
            _ = &rx_shutdown => {
                return Ok(())
            }
        }
    }
}

/// Returns the latest consensus header from a quorum of peers.
///
/// Will allow three seconds per client and three attempts to get the consensus info.
async fn max_consensus_header(
    clients: &mut [PrimaryToPrimaryClient<WaitingPeer>],
) -> Option<ConsensusHeader> {
    let mut attempt = 1;
    let threshhold = ((clients.len() + 1) * 2) / 3;
    let num_peers = clients.len();
    loop {
        if attempt > 3 {
            // We can try three times to get the consensus height.
            return None;
        }
        let mut waiting = FuturesUnordered::new();
        // Ask all our peers for their latest consensus height.
        for client in clients.iter_mut() {
            waiting.push(tokio::time::timeout(
                Duration::from_secs(3), /* Three seconds should be plenty of time to get the
                                         * consensus header. */
                client.request_consensus(ConsensusOutputRequest::default()),
            ));
        }
        let mut responses = 0;
        let mut outputs = HashMap::new();
        while let Some(res) = waiting.next().await {
            match res {
                Ok(Ok(res)) => {
                    responses += 1;
                    let output = res.into_body().output;
                    if let Some((_, count)) = outputs.get_mut(&output.digest()) {
                        if (*count + 1) >= threshhold {
                            tracing::info!(target: "telcoin::state-sync", "reached consensus on current chain height of {} with {} out of {num_peers} peers agreeing out of {responses} responses",
                            output.number, (*count + 1));
                            return Some(output);
                        }
                        *count += 1;
                    } else {
                        outputs.insert(output.digest(), (output, 1_usize));
                    }
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
        attempt += 1;
    }
}

/// Get a vector of clients to each peer.
/// This should go away with libp2p.
fn get_clients<DB: Database>(
    config: &ConsensusConfig<DB>,
    network: &anemo::Network,
) -> Vec<PrimaryToPrimaryClient<WaitingPeer>> {
    config
        .committee()
        .others_primaries_by_id(config.authority().id())
        .into_iter()
        .map(|(_, _, peer_id)| {
            PrimaryToPrimaryClient::new(network.waiting_peer(anemo::PeerId(peer_id.0.to_bytes())))
        })
        .collect()
}

/// Reads the last consensus block from our consensus chain, queries peers for latest height
/// and downloads and executes any missing consensus output.
/// Returns the last ConsensusHeader that was applied on success.
async fn catch_up_consensus<DB: Database>(
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    clients: &mut [PrimaryToPrimaryClient<WaitingPeer>],
) -> eyre::Result<ConsensusHeader> {
    let db = config.database();
    let (_, last_db_block) =
        db.last_record::<ConsensusBlocks>().unwrap_or_else(|| (0, ConsensusHeader::default()));
    let Some(max_consensus) = max_consensus_header(clients).await else {
        return Ok(last_db_block);
    };
    consensus_bus.last_consensus_header().send(max_consensus.clone())?;
    catch_up_consensus_from_to(config, consensus_bus, clients, last_db_block, max_consensus).await
}

/// Applies consensus output "from" (exclusive) to height "max_consensus_height" (inclusive).
/// Queries peers for latest height and downloads and executes any missing consensus output.
/// Returns the last ConsensusHeader that was applied on success.
async fn catch_up_consensus_from_to<DB: Database>(
    config: &ConsensusConfig<DB>,
    consensus_bus: &ConsensusBus,
    clients: &mut [PrimaryToPrimaryClient<WaitingPeer>],
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
    let clients_len = clients.len();
    let mut rx_recent_blocks = consensus_bus.recent_blocks().subscribe();
    let mut latest_exec_block_num = consensus_bus.recent_blocks().borrow().latest_block_num_hash();
    let db = config.database();
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
                if (try_num + 1) % clients_len == 0 {
                    // Sleep for 5 seconds once we have tried all clients...
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                // rotate through clients attempting to get the headers.
                let client = clients
                    .get_mut((number as usize + try_num) % clients_len)
                    .expect("client found by index");
                let req = ConsensusOutputRequest { number: Some(number), hash: None };
                match client.request_consensus(req).await {
                    Ok(res) => break res.into_body().output,
                    Err(e) => {
                        tracing::error!(target: "telcoin::state-sync", "error requesting peer consensus {e:?}");
                        try_num += 1;
                        continue;
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
        let base_execution_block_num =
            consensus_header.sub_dag.leader.header().latest_execution_block_num;
        // We need to make sure execution has caught up so we can verify we have not
        // forked. This will force the follow function to not outrun
        // execution...  this is probably fine. Also once we can
        // follow gossiped consensus output this will not really be
        // an issue (except during initial catch up).
        while base_execution_block_num > latest_exec_block_num.number {
            rx_recent_blocks
                .changed()
                .await
                .map_err(|e| eyre::eyre!("recent blocks changed failed: {e}"))?;
            latest_exec_block_num = consensus_bus.recent_blocks().borrow().latest_block_num_hash();
        }
        if !consensus_bus.recent_blocks().borrow().contains_hash(base_execution_block) {
            // We seem to have forked, so die.
            return Err(eyre::eyre!(
                "consensus_output has a parent not in our chain, missing {}/{} recents: {:?}!",
                base_execution_block_num,
                base_execution_block,
                consensus_bus.recent_blocks().borrow()
            ));
        }
        consensus_bus.consensus_header().send(consensus_header.clone()).await?;
        result_header = consensus_header;
    }
    Ok(result_header)
}
