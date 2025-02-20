//! Subscriber handles consensus output.

use crate::{errors::SubscriberResult, SubscriberError};
use consensus_metrics::monitored_future;
use fastcrypto::hash::Hash;
use futures::{stream::FuturesOrdered, StreamExt};
use state_sync::{
    get_missing_consensus, last_executed_consensus_block, save_consensus, spawn_state_sync,
    stream_missing_consensus,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
    vec,
};
use tn_config::ConsensusConfig;
use tn_network_types::{local::LocalNetwork, FetchBatchesRequest, PrimaryToWorkerClient};
use tn_primary::{
    consensus::ConsensusRound, network::PrimaryNetworkHandle, ConsensusBus, NodeMode,
};
use tn_types::{
    Address, AuthorityIdentifier, Batch, BlockHash, Certificate, CommittedSubDag, Committee,
    ConsensusHeader, ConsensusOutput, Database, NetworkPublicKey, Noticer, TaskManager,
    TaskManagerClone, Timestamp, TnReceiver, TnSender, WorkerCache, WorkerId, B256,
};
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
}

struct Inner {
    authority_id: AuthorityIdentifier,
    worker_cache: WorkerCache,
    committee: Committee,
    client: LocalNetwork,
}

pub fn spawn_subscriber<DB: Database>(
    config: ConsensusConfig<DB>,
    rx_shutdown: Noticer,
    consensus_bus: ConsensusBus,
    task_manager: &TaskManager,
    network: PrimaryNetworkHandle,
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
        inner: Arc::new(Inner { authority_id, committee, worker_cache, client }),
    };
    match mode {
        // If we are active then partcipate in consensus.
        NodeMode::CvvActive => {
            task_manager.spawn_task(
                "subscriber consensus",
                monitored_future!(
                    async move {
                        info!(target: "subscriber", "Starting subscriber: CVV");
                        subscriber.run().await
                    },
                    "SubscriberTask"
                ),
            );
        }
        NodeMode::CvvInactive => {
            let clone = task_manager.get_spawner();
            // If we are not active but are a CVV then catch up and rejoin.
            task_manager.spawn_task(
                "subscriber catch up and rejoin consensus",
                monitored_future!(
                    async move {
                        info!(target: "subscriber", "Starting subscriber: Catch up and rejoin");
                        subscriber.catch_up_rejoin_consensus(clone, network).await
                    },
                    "SubscriberFollowTask"
                ),
            );
        }
        NodeMode::Observer => {
            let clone = task_manager.get_spawner();
            // If we are not active then just follow consensus.
            task_manager.spawn_task(
                "subscriber follow consensus",
                monitored_future!(
                    async move {
                        info!(target: "subscriber", "Starting subscriber: Follower");
                        subscriber.follow_consensus(clone, network).await
                    },
                    "SubscriberFollowTask"
                ),
            );
        }
    }
}

impl<DB: Database> Subscriber<DB> {
    /// Returns the max number of sub-dag to fetch payloads concurrently.
    const MAX_PENDING_PAYLOADS: usize = 1000;

    /// Turns a ConsensusHeader into a ConsensusOutput and sends it down the consensus_output
    /// channel for execution.
    async fn handle_consensus_header(
        &self,
        consensus_header: ConsensusHeader,
    ) -> SubscriberResult<()> {
        let consensus_output = self
            .fetch_batches(
                consensus_header.sub_dag.clone(),
                consensus_header.parent_hash,
                consensus_header.number,
            )
            .await;
        save_consensus(self.config.database(), consensus_output.clone())?;

        // If we want to rejoin consensus eventually then save certs.
        let _ = self
            .config
            .node_storage()
            .certificate_store
            .write(consensus_output.sub_dag.leader.clone());
        let _ = self
            .config
            .node_storage()
            .certificate_store
            .write_all(consensus_output.sub_dag.certificates.clone());

        let last_round = consensus_output.leader_round();

        // We aren't doing consensus now but still need to update these watches before
        // we send the consensus output.
        let _ = self.consensus_bus.update_consensus_rounds(ConsensusRound::new_with_gc_depth(
            last_round,
            self.config.parameters().gc_depth,
        ));
        let _ = self.consensus_bus.primary_round_updates().send(last_round);

        if let Err(e) = self.consensus_bus.consensus_output().send(consensus_output).await {
            error!(target: "subscriber", "error broadcasting consensus output for authority {}: {}", self.inner.authority_id, e);
            return Err(SubscriberError::ClosedChannel("consensus_output".to_string()));
        }
        Ok(())
    }

    /// Catch up to current consensus and then try to rejoin as an active CVV.
    async fn catch_up_rejoin_consensus(
        &self,
        tasks: TaskManagerClone,
        network: PrimaryNetworkHandle,
    ) -> SubscriberResult<()> {
        // Get a receiver than stream any missing headers so we don't miss them.
        let mut rx_consensus_headers = self.consensus_bus.consensus_header().subscribe();
        stream_missing_consensus(&self.config, &self.consensus_bus).await?;
        spawn_state_sync(self.config.clone(), self.consensus_bus.clone(), network, tasks);
        while let Some(consensus_header) = rx_consensus_headers.recv().await {
            let consensus_header_number = consensus_header.number;
            self.handle_consensus_header(consensus_header).await?;
            if consensus_header_number == self.consensus_bus.last_consensus_header().borrow().number
            {
                // We are caught up enough so try to jump back into consensus
                info!(target: "subscriber", "attempting to rejoin consensus, consensus block height {consensus_header_number}");
                // Set restart flag and trigger shutdown by returning.
                self.consensus_bus.set_restart();
                let _ = self.consensus_bus.node_mode().send(NodeMode::CvvActive);
                return Ok(());
            }
        }
        Ok(())
    }

    /// Follow along with consensus output but do not try to join consensus.
    async fn follow_consensus(
        &self,
        tasks: TaskManagerClone,
        network: PrimaryNetworkHandle,
    ) -> SubscriberResult<()> {
        // Get a receiver than stream any missing headers so we don't miss them.
        let mut rx_consensus_headers = self.consensus_bus.consensus_header().subscribe();
        stream_missing_consensus(&self.config, &self.consensus_bus).await?;
        spawn_state_sync(self.config.clone(), self.consensus_bus.clone(), network, tasks);
        while let Some(consensus_header) = rx_consensus_headers.recv().await {
            self.handle_consensus_header(consensus_header).await?;
        }
        Ok(())
    }

    /// Return the block hash and number of the last executed consensus output.
    async fn get_last_executed_consensus(&self) -> SubscriberResult<(BlockHash, u64)> {
        // Get the DB and load our last executed consensus block (note there may be unexecuted
        // blocks, catch up will execute them).
        let last_executed_block =
            last_executed_consensus_block(&self.consensus_bus, &self.config).unwrap_or_default();

        info!(target: "subscriber", ?last_executed_block, "restoring last executed consensus:");

        Ok((last_executed_block.digest(), last_executed_block.number))
    }

    /// Main loop connecting to the consensus to listen to sequence messages.
    async fn run(self) -> SubscriberResult<()> {
        // Make sure any old consensus that was not executed gets executed.
        let missing = get_missing_consensus(&self.config, &self.consensus_bus).await?;
        for consensus_header in missing.into_iter() {
            self.handle_consensus_header(consensus_header).await?;
        }
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
            tokio::select! {
                // Receive the ordered sequence of consensus messages from a consensus node.
                Some(sub_dag) = rx_sequence.recv(), if waiting.len() < Self::MAX_PENDING_PAYLOADS => {
                    debug!(target: "subscriber", subdag=?sub_dag.digest(), round=?sub_dag.leader_round(), "received committed subdag from consensus");
                    // We can schedule more then MAX_PENDING_PAYLOADS payloads but
                    // don't process more consensus messages when more
                    // then MAX_PENDING_PAYLOADS is pending
                    let parent_hash = last_parent;
                    let number = last_number + 1;
                    last_parent = ConsensusHeader::digest_from_parts(parent_hash, &sub_dag, number);

                    // Record the latest ConsensusHeader, we probably don't need this in this mode but keep it up to date anyway.
                    // Note we don't bother sending this to the consensus header channel since not needed when an active CVV.
                    if let Err(e) = self.consensus_bus.last_consensus_header().send(ConsensusHeader { parent_hash, sub_dag: sub_dag.clone(), number, extra: B256::default() }) {
                        error!(target: "subscriber", "error sending latest consensus header for authority {}: {}", self.inner.authority_id, e);
                        return Ok(());
                    }
                    last_number += 1;
                    waiting.push_back(self.fetch_batches(sub_dag, parent_hash, number));
                },

                // Receive consensus messages after all transaction data is downloaded
                // then send to the execution layer for final block production.
                //
                // NOTE: this broadcasts to all subscribers, but lagging receivers will lose messages
                Some(output) = waiting.next() => {
                    debug!(target: "subscriber", output=?output.digest(), "saving next output");
                    save_consensus(self.config.database(), output.clone())?;
                    debug!(target: "subscriber", "broadcasting output...");
                    if let Err(e) = self.consensus_bus.consensus_output().send(output).await {
                        error!(target: "subscriber", "error broadcasting consensus output for authority {}: {}", self.inner.authority_id, e);
                        return Ok(());
                    }
                    debug!(target: "subscriber", "output broadcast successfully");
                },

                _ = &self.rx_shutdown => {
                    return Ok(())
                }

            }

            self.consensus_bus
                .executor_metrics()
                .waiting_elements_subscriber
                .set(waiting.len() as i64);
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
                extra: B256::default(),
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
            extra: B256::default(),
        };

        let mut batch_digests_and_workers: HashMap<
            NetworkPublicKey,
            (HashSet<BlockHash>, HashSet<NetworkPublicKey>),
        > = HashMap::new();

        for cert in &sub_dag.certificates {
            for (digest, (worker_id, _)) in cert.header().payload().iter() {
                if let Ok(own_worker) = self.inner.worker_cache.worker(
                    self.inner
                        .committee
                        .authority(&self.inner.authority_id)
                        .expect("own workers in worker cache")
                        .protocol_key(),
                    worker_id,
                ) {
                    let own_worker_name = own_worker.name;
                    let workers = Self::workers_for_certificate(&self.inner, cert, worker_id);
                    let (batch_set, worker_set) =
                        batch_digests_and_workers.entry(own_worker_name).or_default();
                    batch_set.insert(*digest);
                    subscriber_output.batch_digests.push_back(*digest);
                    worker_set.extend(workers);
                } else {
                    error!(target: "subscriber", "failed to find a local worker for {worker_id}, malicious certificate?");
                }
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

                debug!(target: "subscriber",
                    "Adding fetched batch {digest} from certificate {} to consensus output",
                    cert.digest()
                );
                output_batches.push(batch.clone());
            }
            subscriber_output.batches.push(output_batches);
        }
        debug!(target: "subscriber", "returning output to subscriber");
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
                        error!(target: "subscriber",
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

#[cfg(test)]
mod tests {
    use super::*;
    use fastcrypto::traits::KeyPair as _;
    use indexmap::IndexMap;
    use std::{collections::BTreeSet, ops::RangeInclusive};
    use tn_network_types::MockPrimaryToWorkerClient;
    use tn_primary::consensus::{Bullshark, Consensus, LeaderSchedule};
    use tn_primary_metrics::ConsensusMetrics;
    use tn_storage::mem_db::MemDatabase;
    use tn_test_utils::CommitteeFixture;
    use tn_types::{
        network_public_key_to_libp2p, CertificateDigest, ExecHeader, HeaderBuilder, Round,
        SealedHeader, TimestampSec, DEFAULT_BAD_NODES_STAKE_THRESHOLD,
    };
    use tokio::sync::mpsc;

    fn random_batches(
        number_of_batches: usize,
    ) -> (IndexMap<BlockHash, (WorkerId, TimestampSec)>, HashMap<BlockHash, Batch>) {
        let mut payload: IndexMap<BlockHash, (WorkerId, TimestampSec)> =
            IndexMap::with_capacity(number_of_batches);
        let mut batches = HashMap::with_capacity(number_of_batches);

        for _ in 0..number_of_batches {
            let batch = tn_test_utils::batch();
            let batch_digest = batch.digest();

            payload.insert(batch_digest, (0, 0));
            batches.insert(batch_digest, batch);
        }

        (payload, batches)
    }

    /// Creates one signed certificate from a set of signers - the signers must include the origin
    fn signed_cert<DB>(
        origin: AuthorityIdentifier,
        round: Round,
        parents: BTreeSet<CertificateDigest>,
        committee: &CommitteeFixture<DB>,
    ) -> (CertificateDigest, Certificate, HashMap<BlockHash, Batch>)
    where
        DB: Database,
    {
        let (payload, batches) = random_batches(3);
        let header = HeaderBuilder::default()
            .author(origin)
            .payload(payload)
            .round(round)
            .epoch(0)
            .parents(parents)
            .build()
            .expect("valid header built for test certificate");

        let cert = committee.certificate(&header);
        (cert.digest(), cert, batches)
    }

    fn create_test_data<DB>(
        range: RangeInclusive<Round>,
        fixture: &CommitteeFixture<DB>,
    ) -> (VecDeque<Certificate>, BTreeSet<CertificateDigest>, HashMap<BlockHash, Batch>)
    where
        DB: Database,
    {
        let ids: Vec<_> = fixture.authorities().map(|a| a.id()).collect();
        let mut certificates = VecDeque::new();
        let mut next_parents = BTreeSet::new();
        let mut batches = HashMap::new();
        // use genesis for initial parents
        let mut parents: BTreeSet<_> = fixture.genesis().collect();

        // create signed certificates for every round
        for round in range {
            next_parents.clear();
            for id in &ids {
                let (digest, certificate, payload) =
                    signed_cert(*id, round, parents.clone(), fixture);
                certificates.push_back(certificate);
                next_parents.insert(digest);
                batches.extend(payload);
            }
            parents.clone_from(&next_parents);
        }

        (certificates, next_parents, batches)
    }

    #[tokio::test]
    async fn test_output_to_header() -> eyre::Result<()> {
        let num_sub_dags_per_schedule = 3;
        let fixture = CommitteeFixture::builder(MemDatabase::default).build();
        let committee = fixture.committee();
        let primary = fixture.authorities().next().unwrap();
        let config = primary.consensus_config().clone();
        let consensus_store = config.node_storage().consensus_store.clone();
        let task_manager = TaskManager::new("subscriber tests");
        let rx_shutdown = config.shutdown().subscribe();
        let consensus_bus = ConsensusBus::new();

        // subscribe to channels early
        let rx_consensus_headers = consensus_bus.last_consensus_header().subscribe();
        let mut consensus_output = consensus_bus.consensus_output().subscribe();

        let (tx, _rx) = mpsc::channel(5);
        let network = PrimaryNetworkHandle::new_for_test(tx);
        // spawn the executor
        spawn_subscriber(
            config.clone(),
            rx_shutdown,
            consensus_bus.clone(),
            &task_manager,
            network,
        );

        // yield for subscriber to spawn
        tokio::task::yield_now().await;

        // make certificates for rounds 1 to 7 (inclusive)
        let (certificates, _next_parents, batches) = create_test_data(1..=7, &fixture);

        // Set up mock worker.
        let worker = primary.worker();
        let _worker_address = &worker.info().worker_address;
        let worker_peer_id = network_public_key_to_libp2p(worker.keypair().public());
        let mock_client = Arc::new(MockPrimaryToWorkerClient { batches });
        config.local_network().set_primary_to_worker_local_handler(worker_peer_id, mock_client);

        let metrics = Arc::new(ConsensusMetrics::default());
        let leader_schedule = LeaderSchedule::from_store(
            committee.clone(),
            consensus_store.clone(),
            DEFAULT_BAD_NODES_STAKE_THRESHOLD,
        );
        let bullshark = Bullshark::new(
            committee.clone(),
            consensus_store.clone(),
            metrics.clone(),
            num_sub_dags_per_schedule,
            leader_schedule.clone(),
            DEFAULT_BAD_NODES_STAKE_THRESHOLD,
        );

        let dummy_parent = SealedHeader::new(ExecHeader::default(), B256::default());
        consensus_bus.recent_blocks().send_modify(|blocks| blocks.push_latest(dummy_parent));
        let task_manager = TaskManager::default();
        Consensus::spawn(config.clone(), &consensus_bus, bullshark, &task_manager);

        // forward certificates to trigger subdag commit
        for certificate in certificates.iter() {
            consensus_bus.new_certificates().send(certificate.clone()).await.unwrap();
        }

        let expected_num = 3;
        let mut consensus_headers_seen: Vec<_> = Vec::with_capacity(expected_num);
        while let Some(output) = consensus_output.recv().await {
            let num = output.number;
            let consensus_header = output.consensus_header();
            consensus_headers_seen.push(consensus_header);
            if num == expected_num as u64 {
                break;
            }

            // yield for other tasks
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        let last_header = rx_consensus_headers.borrow().clone();
        assert!(last_header.number == expected_num as u64);

        // NOTE: output.consensus_header() creates the consensus header and should be the same
        // result
        assert_eq!(
            last_header.digest(),
            consensus_headers_seen.last().expect("last consensus header").digest()
        );

        Ok(())
    }
}
