//! Subscriber handles consensus output.

use crate::{errors::SubscriberResult, SubscriberError};
use alloy::{Address, B256};
use consensus_metrics::monitored_future;
use fastcrypto::hash::Hash;
use futures::{
    stream::{FuturesOrdered, FuturesUnordered},
    StreamExt,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
    vec,
};
use tn_config::ConsensusConfig;
use tn_network::{
    anemo_ext::{NetworkExt, WaitingPeer},
    local::LocalNetwork,
    PrimaryToWorkerClient,
};
use tn_network_types::{ConsensusOutputRequest, FetchBatchesRequest, PrimaryToPrimaryClient};
use tn_primary::{consensus::ConsensusRound, ConsensusBus};
use tn_storage::{
    tables::{ConsensusBlockNumbersByDigest, ConsensusBlocks},
    traits::{Database, DbTxMut},
};
use tn_types::{
    AuthorityIdentifier, Batch, BlockHash, Certificate, CommittedSubDag, Committee,
    ConsensusHeader, ConsensusOutput, Epoch, NetworkPublicKey, Noticer, Round, TaskManager,
    Timestamp, TnReceiver, TnSender, WorkerCache, WorkerId,
};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// The `Subscriber` receives certificates sequenced by the consensus and waits until the
/// downloaded all the transactions references by the certificates; it then
/// forward the certificates to the Executor.
#[derive(Clone)]
pub struct Subscriber<DB> {
    /// Receiver for shutdown
    rx_shutdown: Noticer,
    /// Used to get the sequence receiver
    consensus_bus: ConsensusBus,
    /// Consensus configuration (contains the consensus DB)
    config: ConsensusConfig<DB>,
    /// Inner state.
    inner: Arc<Inner>,
    /// Lock and flag to only execute missing consensus output once.
    execute_missing: Arc<Mutex<bool>>,
}

struct Inner {
    authority_id: AuthorityIdentifier,
    worker_cache: WorkerCache,
    committee: Committee,
    client: LocalNetwork,
    network: anemo::Network,
}

pub fn spawn_subscriber<DB: Database>(
    config: ConsensusConfig<DB>,
    rx_shutdown: Noticer,
    consensus_bus: ConsensusBus,
    network: anemo::Network,
    task_manager: &TaskManager,
) {
    let authority_id = config.authority().id();
    let worker_cache = config.worker_cache().clone();
    let committee = config.committee().clone();
    let client = config.local_network().clone();

    let mode = *consensus_bus.node_mode().borrow();
    let subscriber = Subscriber {
        rx_shutdown,
        consensus_bus,
        config,
        inner: Arc::new(Inner { authority_id, committee, worker_cache, client, network }),
        execute_missing: Arc::new(Mutex::new(false)),
    };
    let sub_clone = subscriber.clone();
    if mode.is_cvv() {
        task_manager.spawn_task(
            "subscriber consensus",
            monitored_future!(
                async move {
                    info!(target: "telcoin::subscriber", "Starting subscriber");
                    sub_clone.run().await.expect("Failed to run subscriber")
                },
                "SubscriberTask"
            ),
        );
    }
    // Keep follow consensus warm even if a CVV, might need it.
    task_manager.spawn_task(
        "subscriber follow consensus",
        monitored_future!(
            async move {
                info!(target: "telcoin::subscriber", "Starting subscriber");
                subscriber.follow_consensus().await.expect("Failed to run subscriber")
            },
            "SubscriberFollowTask"
        ),
    );
}

/// Returns the max consensus chain block number, epoch and round from peers.
async fn max_consensus_number(
    clients: &mut [PrimaryToPrimaryClient<WaitingPeer>],
) -> Option<(u64, Epoch, Round)> {
    let mut waiting = FuturesUnordered::new();
    // Ask all our peers for their latest consensus height.
    let threshhold = ((clients.len() + 1) * 2) / 3;
    let num_peers = clients.len();
    for client in clients.iter_mut() {
        waiting.push(client.request_consensus(ConsensusOutputRequest::default()));
    }
    let mut responses = 0;
    let mut outputs = HashMap::new();
    // TODO should probably have some timeouts and also try 2-3 times in case of a race
    // condition with peers.
    while let Some(res) = waiting.next().await {
        match res {
            Ok(res) => {
                responses += 1;
                let output = res.into_body().output;
                if let Some((_, count)) = outputs.get_mut(&output.digest()) {
                    if (*count + 1) >= threshhold {
                        tracing::info!(target: "telcoin::subscriber", "reached consensus on current chain height of {} with {} out of {num_peers} peers agreeing out of {responses} responses",
                            output.number, (*count + 1));
                        return Some((
                            output.number,
                            output.sub_dag.leader.epoch(),
                            output.sub_dag.leader.round(),
                        ));
                    }
                    *count += 1;
                } else {
                    outputs.insert(output.digest(), (output, 1_usize));
                }
            }
            Err(e) => {
                // An error with one peer should not derail us...  But log it.
                tracing::error!(target: "telcoin::subscriber", "error requesting peer consensus {e:?}")
            }
        }
    }
    None
}

fn last_executed_consensus_block<DB: Database>(
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
    let _ = consensus_bus.consensus_round_updates().send(ConsensusRound::new_with_gc_depth(
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
    let (_max_consensus_height, max_epoch, max_round) =
        max_consensus_number(&mut clients).await.unwrap_or_else(|| {
            (
                last_executed_block.number,
                last_executed_block.sub_dag.leader.epoch(),
                last_executed_block.sub_dag.leader.round(),
            )
        });
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

impl<DB: Database> Subscriber<DB> {
    /// Returns the max number of sub-dag to fetch payloads concurrently.
    const MAX_PENDING_PAYLOADS: usize = 1000;

    /// Write the consensus header and it's digest to number index to the consensus DB.
    ///
    /// An error here indicates a critical node failure.
    /// Note this function both logs and returns the error due to it's severity.
    fn save_consensus(&self, consensus_output: ConsensusOutput) -> SubscriberResult<()> {
        match self.config.database().write_txn() {
            Ok(mut txn) => {
                let header: ConsensusHeader = consensus_output.into();
                if let Err(e) = txn.insert::<ConsensusBlocks>(&header.number, &header) {
                    tracing::error!(target: "engine", ?e, "error saving a consensus header to persistant storage!");
                    return Err(e.into());
                }
                if let Err(e) =
                    txn.insert::<ConsensusBlockNumbersByDigest>(&header.digest(), &header.number)
                {
                    tracing::error!(target: "engine", ?e, "error saving a consensus header number to persistant storage!");
                    return Err(e.into());
                }
                if let Err(e) = txn.commit() {
                    tracing::error!(target: "engine", ?e, "error saving committing to persistant storage!");
                    return Err(e.into());
                }
            }
            Err(e) => {
                tracing::error!(target: "engine", ?e, "error getting a transaction on persistant storage!");
                return Err(e.into());
            }
        }
        Ok(())
    }

    /// Execute any consensus output that was not executed before last shutdown.
    /// This will only execute once and will block until the first run finishes if called more than
    /// once.
    async fn execute_missing(&self) -> SubscriberResult<()> {
        let mut guard = self.execute_missing.lock().await;
        if *guard {
            return Ok(());
        }
        *guard = true;
        // Get the DB and load our last executed consensus block.
        let last_executed_block =
            last_executed_consensus_block(&self.consensus_bus, &self.config).unwrap_or_default();
        // Edge case, in case we don't hear from peers but have un-executed blocks...
        // Not sure we should handle this, but it hurts nothing.
        let db = self.config.database();
        let (_, last_db_block) = db
            .last_record::<ConsensusBlocks>()
            .unwrap_or_else(|| (last_executed_block.number, last_executed_block.clone()));
        if last_db_block.number > last_executed_block.number {
            for consensus_block_number in last_executed_block.number + 1..=last_db_block.number {
                if let Some(consensus_block) = db.get::<ConsensusBlocks>(&consensus_block_number)? {
                    let consensus_output = self
                        .fetch_batches(
                            consensus_block.sub_dag,
                            consensus_block.parent_hash,
                            consensus_block.number,
                        )
                        .await;
                    let last_round = consensus_output.leader_round();

                    let base_execution_block =
                        consensus_output.sub_dag.leader.header().latest_execution_block;
                    let base_execution_block_num =
                        consensus_output.sub_dag.leader.header().latest_execution_block_num;
                    if !self
                        .consensus_bus
                        .recent_blocks()
                        .borrow()
                        .contains_hash(base_execution_block)
                    {
                        let msg = format!("consensus_output has a parent not in our chain (while re-executing), missing {}/{} recents: {:?}!",
                            base_execution_block_num,
                            base_execution_block,
                            self.consensus_bus.recent_blocks().borrow());
                        // We seem to have forked, so die.
                        return Err(SubscriberError::NodeExecutionError(msg));
                    }

                    // We aren't doing consensus now but still need to update these watches before
                    // we send the consensus output.
                    let _ = self.consensus_bus.consensus_round_updates().send(
                        ConsensusRound::new_with_gc_depth(
                            last_round,
                            self.config.parameters().gc_depth,
                        ),
                    );
                    let _ = self.consensus_bus.primary_round_updates().send(last_round);

                    info!(target: "telcoin::subscriber", "executing previous consensus output for round {}, consensus_block {}", consensus_output.leader_round(), consensus_output.number);
                    if let Err(e) =
                        self.consensus_bus.consensus_output().send(consensus_output).await
                    {
                        error!(target: "telcoin::subscriber", "error broadcasting consensus output for authority {}: {}", self.inner.authority_id, e);
                        return Err(SubscriberError::ClosedChannel("consensus_output".to_string()));
                    }
                }
            }
        }
        Ok(())
    }

    async fn follow_consensus(&self) -> SubscriberResult<()> {
        self.execute_missing().await?;
        // Start from the last block in our DB, the call to execute_missing above should execute any
        // blocks we missed from our DB.
        let db = self.config.database();
        let (_, last_db_block) =
            db.last_record::<ConsensusBlocks>().unwrap_or_else(|| (0, ConsensusHeader::default()));
        // Note use last_executed_block here because
        let mut last_parent = last_db_block.digest();

        let mut clients: Vec<PrimaryToPrimaryClient<_>> = self
            .config
            .committee()
            .others_primaries_by_id(self.config.authority().id())
            .into_iter()
            .map(|(_, _, peer_id)| {
                PrimaryToPrimaryClient::new(
                    self.inner.network.waiting_peer(anemo::PeerId(peer_id.0.to_bytes())),
                )
            })
            .collect();
        let (mut max_consensus_height, max_epoch, max_round) =
            max_consensus_number(&mut clients).await.unwrap_or((
                last_db_block.number,
                last_db_block.sub_dag.leader.epoch(),
                last_db_block.sub_dag.leader.round(),
            ));
        // Catch up to the current chain state if we need to.
        let mut last_consensus_height = last_db_block.number;
        let clients_len = clients.len();
        let mut rx_recent_blocks = self.consensus_bus.recent_blocks().subscribe();
        let mut latest_exec_block_num =
            self.consensus_bus.recent_blocks().borrow().latest_block_num_hash();
        // infinate loop over consensus output
        loop {
            if self.consensus_bus.node_mode().borrow().is_active_cvv() {
                // If we are a CVV then do actual consensus and nothing here.
                // Sleep in case we get into a failed state and need to start working.
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = &self.rx_shutdown => {
                        return Ok(())
                    }
                }
            } else {
                // Otherwise just follow along...
                for number in last_consensus_height + 1..=max_consensus_height {
                    tracing::debug!(target: "telcoin::subscriber", "trying to get consensus block {number}");
                    // Check if we already have this consensus output in our local DB.
                    // This will also allow us to pre load other consensus blocks as a future
                    // optimization.
                    let output = if let Ok(Some(block)) = db.get::<ConsensusBlocks>(&number) {
                        block
                    } else {
                        let mut try_num = 0;
                        loop {
                            // stop trying at some point?
                            // rotate through clients attempting to get the headers.
                            let client = clients
                                .get_mut((number as usize + try_num) % clients_len)
                                .expect("client found by index");
                            let req = ConsensusOutputRequest { number: Some(number), hash: None };
                            match client.request_consensus(req).await {
                                Ok(res) => break res.into_body().output,
                                Err(e) => {
                                    tracing::error!(target: "telcoin::subscriber", "error requesting peer consensus {e:?}");
                                    try_num += 1;
                                    continue;
                                }
                            }
                        }
                    };
                    let parent_hash = last_parent;
                    last_parent =
                        ConsensusHeader::digest_from_parts(parent_hash, &output.sub_dag, number);
                    if last_parent != output.digest() {
                        tracing::error!(target: "telcoin::subscriber", "failed to execute consensus!");
                        return Err(SubscriberError::UnexpectedProtocolMessage);
                    }
                    // TODO should also verify that the block output is building on is in fact
                    // the head of our chain.
                    let consensus_output =
                        self.fetch_batches(output.sub_dag, parent_hash, number).await;
                    self.save_consensus(consensus_output.clone())?;
                    let last_round = consensus_output.leader_round();

                    let base_execution_block =
                        consensus_output.sub_dag.leader.header().latest_execution_block;
                    let base_execution_block_num =
                        consensus_output.sub_dag.leader.header().latest_execution_block_num;
                    // We need to make sure execution has caught up so we can verify we have not
                    // forked. This will force the follow function to not outrun
                    // execution...  this is probably fine. Also once we can
                    // follow gossiped consensus output this will not really be
                    // an issue (except during initial catch up).
                    while base_execution_block_num > latest_exec_block_num.number {
                        rx_recent_blocks.changed().await.map_err(|e| {
                            SubscriberError::NodeExecutionError(format!(
                                "recent blocks changed failed: {e}"
                            ))
                        })?;
                        latest_exec_block_num =
                            self.consensus_bus.recent_blocks().borrow().latest_block_num_hash();
                    }
                    if !self
                        .consensus_bus
                        .recent_blocks()
                        .borrow()
                        .contains_hash(base_execution_block)
                    {
                        // We seem to have forked, so die.
                        return Err(SubscriberError::NodeExecutionError(
                        format!("consensus_output has a parent not in our chain, missing {}/{} recents: {:?}!",
                            base_execution_block_num,
                            base_execution_block,
                            self.consensus_bus.recent_blocks().borrow())
                    ));
                    }

                    // We aren't doing consensus now but still need to update these watches before
                    // we send the consensus output.
                    let _ = self.consensus_bus.consensus_round_updates().send(
                        ConsensusRound::new_with_gc_depth(
                            last_round,
                            self.config.parameters().gc_depth,
                        ),
                    );
                    let _ = self.consensus_bus.primary_round_updates().send(last_round);

                    if let Err(e) =
                        self.consensus_bus.consensus_output().send(consensus_output).await
                    {
                        error!(target: "telcoin::subscriber", "error broadcasting consensus output for authority {}: {}", self.inner.authority_id, e);
                        return Err(SubscriberError::ClosedChannel("consensus_output".to_string()));
                    }
                }
                last_consensus_height = max_consensus_height;
                while last_consensus_height == max_consensus_height {
                    // Rest for bit then try see if chain has advanced and catch up if so.
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                        _ = &self.rx_shutdown => {
                            return Ok(())
                        }
                    }
                    let (new_max_consensus_height, _, _) = max_consensus_number(&mut clients)
                        .await
                        .unwrap_or((last_consensus_height, max_epoch, max_round));
                    max_consensus_height = new_max_consensus_height;
                }
            }
        }
    }

    /// Return the block hash and number of the last executed consensus output.
    async fn get_last_executed_consensus(&self) -> SubscriberResult<(BlockHash, u64)> {
        // Get the DB and load our last executed consensus block (note there may be unexecuted
        // blocks, catch up will execute them).
        let last_executed_block =
            last_executed_consensus_block(&self.consensus_bus, &self.config).unwrap_or_default();

        Ok((last_executed_block.digest(), last_executed_block.number))
    }

    /// Main loop connecting to the consensus to listen to sequence messages.
    async fn run(self) -> SubscriberResult<()> {
        self.execute_missing().await?;
        // It's important to have the futures in ordered fashion as we want
        // to guarantee that will deliver to the executor the certificates
        // in the same order we received from rx_sequence. So it doesn't
        // matter if we somehow managed to fetch the blocks from a later
        // certificate. Unless the earlier certificate's payload has been
        // fetched, no later certificate will be delivered.
        let mut waiting = FuturesOrdered::new();

        let (mut last_parent, mut last_number) = self.get_last_executed_consensus().await?;

        let mut rx_sequence = self.consensus_bus.sequence().subscribe();
        // Listen to sequenced consensus message and process them.
        loop {
            if self.consensus_bus.node_mode().borrow().is_active_cvv() {
                tokio::select! {
                    // Receive the ordered sequence of consensus messages from a consensus node.
                    Some(sub_dag) = rx_sequence.recv(), if waiting.len() < Self::MAX_PENDING_PAYLOADS => {
                        // We can schedule more then MAX_PENDING_PAYLOADS payloads but
                        // don't process more consensus messages when more
                        // then MAX_PENDING_PAYLOADS is pending
                        let parent_hash = last_parent;
                        let number = last_number + 1;
                        last_parent = ConsensusHeader::digest_from_parts(parent_hash, &sub_dag, number);
                        last_number += 1;
                        waiting.push_back(self.fetch_batches(sub_dag, parent_hash, number));
                    },

                    // Receive consensus messages after all transaction data is downloaded
                    // then send to the execution layer for final block production.
                    //
                    // NOTE: this broadcasts to all subscribers, but lagging receivers will lose messages
                    Some(message) = waiting.next() => {
                        self.save_consensus(message.clone())?;
                        if let Err(e) = self.consensus_bus.consensus_output().send(message).await {
                            error!(target: "telcoin::subscriber", "error broadcasting consensus output for authority {}: {}", self.inner.authority_id, e);
                            return Ok(());
                        }
                    },

                    _ = &self.rx_shutdown => {
                        return Ok(())
                    }

                }

                self.consensus_bus
                    .executor_metrics()
                    .waiting_elements_subscriber
                    .set(waiting.len() as i64);
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    _ = &self.rx_shutdown => {
                        return Ok(())
                    }
                }
            }
        }
    }

    /// Returns ordered vector of futures for downloading blocks for certificates
    /// Order of futures returned follows order of blocks in the certificates.
    /// See BlockFetcher for more details.
    async fn fetch_batches(
        &self,
        deliver: CommittedSubDag,
        parent_hash: B256,
        number: u64,
    ) -> ConsensusOutput {
        let num_blocks = deliver.num_primary_blocks();
        let num_certs = deliver.len();

        // get the execution address of the authority or use zero address
        let leader = self.inner.committee.authority(&deliver.leader.origin());
        let address = if let Some(authority) = leader {
            authority.execution_address()
        } else {
            warn!("Execution address missing for {}", &deliver.leader.origin());
            Address::ZERO
        };

        if num_blocks == 0 {
            debug!("No blocks to fetch, payload is empty");
            return ConsensusOutput {
                sub_dag: Arc::new(deliver),
                batches: vec![],
                beneficiary: address,
                batch_digests: VecDeque::new(),
                parent_hash,
                number,
            };
        }

        let sub_dag = Arc::new(deliver);
        let mut subscriber_output = ConsensusOutput {
            sub_dag: sub_dag.clone(),
            batches: Vec::with_capacity(num_certs),
            beneficiary: address,
            batch_digests: VecDeque::new(),
            parent_hash,
            number,
        };

        let mut batch_digests_and_workers: HashMap<
            NetworkPublicKey,
            (HashSet<BlockHash>, HashSet<NetworkPublicKey>),
        > = HashMap::new();

        for cert in &sub_dag.certificates {
            for (digest, (worker_id, _)) in cert.header().payload().iter() {
                let own_worker_name = self
                    .inner
                    .worker_cache
                    .worker(
                        self.inner
                            .committee
                            .authority(&self.inner.authority_id)
                            .expect("own workers in worker cache")
                            .protocol_key(),
                        worker_id,
                    )
                    .unwrap_or_else(|_| panic!("worker_id {worker_id} is not in the worker cache"))
                    .name;
                let workers = Self::workers_for_certificate(&self.inner, cert, worker_id);
                let (batch_set, worker_set) =
                    batch_digests_and_workers.entry(own_worker_name).or_default();
                batch_set.insert(*digest);
                subscriber_output.batch_digests.push_back(*digest);
                worker_set.extend(workers);
            }
        }

        let fetched_batches_timer = self
            .consensus_bus
            .executor_metrics()
            .block_fetch_for_committed_subdag_total_latency
            .start_timer();
        self.consensus_bus
            .executor_metrics()
            .committed_subdag_block_count
            .observe(num_blocks as f64);
        let fetched_batches = self.fetch_batches_from_workers(batch_digests_and_workers).await;
        drop(fetched_batches_timer);

        // Map all fetched batches to their respective certificates and submit as
        // consensus output
        for cert in &sub_dag.certificates {
            let mut output_batches = Vec::with_capacity(cert.header().payload().len());

            self.consensus_bus.executor_metrics().subscriber_current_round.set(cert.round() as i64);

            self.consensus_bus
                .executor_metrics()
                .subscriber_certificate_latency
                .observe(cert.created_at().elapsed().as_secs_f64());

            for (digest, (_, _)) in cert.header().payload().iter() {
                self.consensus_bus.executor_metrics().subscriber_processed_blocks.inc();
                let batch = fetched_batches
                    .get(digest)
                    .expect("[Protocol violation] Batch not found in fetched batches from workers of certificate signers");

                debug!(target: "telcoin::subscriber",
                    "Adding fetched batch {digest} from certificate {} to consensus output",
                    cert.digest()
                );
                output_batches.push(batch.clone());
            }
            subscriber_output.batches.push(output_batches);
        }
        subscriber_output
    }

    fn workers_for_certificate(
        inner: &Inner,
        certificate: &Certificate,
        worker_id: &WorkerId,
    ) -> Vec<NetworkPublicKey> {
        // Can include own authority and worker, but worker will always check local storage when
        // fetching paylods.
        let authorities = certificate.signed_authorities_with_committee(&inner.committee);
        authorities
            .into_iter()
            .filter_map(|authority| {
                let worker = inner.worker_cache.worker(&authority, worker_id);
                match worker {
                    Ok(worker) => Some(worker.name),
                    Err(err) => {
                        error!(target: "telcoin::subscriber",
                            "Worker {} not found for authority {}: {:?}",
                            worker_id, authority, err
                        );
                        None
                    }
                }
            })
            .collect()
    }

    async fn fetch_batches_from_workers(
        &self,
        batch_digests_and_workers: HashMap<
            NetworkPublicKey,
            (HashSet<BlockHash>, HashSet<NetworkPublicKey>),
        >,
    ) -> HashMap<BlockHash, Batch> {
        let mut fetched_blocks = HashMap::new();

        for (worker_name, (digests, known_workers)) in batch_digests_and_workers {
            debug!(
                "Attempting to fetch {} digests from {} known workers, {worker_name}'s",
                digests.len(),
                known_workers.len()
            );
            // TODO: Can further parallelize this by worker if necessary. Maybe move the logic
            // to LocalNetwork.
            // Only have one worker for now so will leave this for a future
            // optimization.
            let request = FetchBatchesRequest { digests, known_workers };
            let blocks = loop {
                match self.inner.client.fetch_batches(worker_name.clone(), request.clone()).await {
                    Ok(resp) => break resp.batches,
                    Err(e) => {
                        error!("Failed to fetch blocks from worker {worker_name}: {e:?}");
                        // Loop forever on failure. During shutdown, this should get cancelled.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            };
            for (digest, block) in blocks {
                self.record_fetched_batch_metrics(&block, &digest);
                fetched_blocks.insert(digest, block);
            }
        }

        fetched_blocks
    }

    fn record_fetched_batch_metrics(&self, batch: &Batch, digest: &BlockHash) {
        if let Some(received_at) = batch.received_at() {
            let remote_duration = received_at.elapsed().as_secs_f64();
            debug!(
                "Batch was fetched for execution after being received from another worker {}s ago.",
                remote_duration
            );
            self.consensus_bus
                .executor_metrics()
                .block_execution_local_latency
                .with_label_values(&["other"])
                .observe(remote_duration);
        } else {
            let local_duration = batch.created_at().elapsed().as_secs_f64();
            debug!(
                "Batch was fetched for execution after being created locally {}s ago.",
                local_duration
            );
            self.consensus_bus
                .executor_metrics()
                .block_execution_local_latency
                .with_label_values(&["own"])
                .observe(local_duration);
        };

        let block_fetch_duration = batch.created_at().elapsed().as_secs_f64();
        self.consensus_bus.executor_metrics().block_execution_latency.observe(block_fetch_duration);
        debug!(
            "Block {:?} took {} seconds since it has been created to when it has been fetched for execution",
            digest,
            block_fetch_duration,
        );
    }
}

// TODO: add a unit test
