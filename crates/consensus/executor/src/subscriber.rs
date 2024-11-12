// Copyright (c) Telcoin, LLC
// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
use crate::{errors::SubscriberResult, metrics::ExecutorMetrics};
use consensus_metrics::spawn_logged_monitored_task;
use consensus_network::{client::NetworkClient, PrimaryToWorkerClient};
use consensus_network_types::FetchBlocksRequest;
use fastcrypto::hash::Hash;
use futures::{stream::FuturesOrdered, StreamExt};
use reth_primitives::Address;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
    vec,
};
use tn_primary::ConsensusBus;
use tn_types::{
    AuthorityIdentifier, BlockHash, Certificate, CommittedSubDag, Committee, ConsensusOutput,
    NetworkPublicKey, Noticer, Timestamp, TnReceiver, TnSender, WorkerBlock, WorkerCache, WorkerId,
};
use tokio::{sync::broadcast, task::JoinHandle};
use tracing::{debug, error, info, warn};

/// The `Subscriber` receives certificates sequenced by the consensus and waits until the
/// downloaded all the transactions references by the certificates; it then
/// forward the certificates to the Executor.
pub struct Subscriber {
    /// Receiver for shutdown
    rx_shutdown: Noticer,
    /// Used to get the sequence receiver
    consensus_bus: ConsensusBus,
    /// Inner state.
    inner: Arc<Inner>,
}

struct Inner {
    authority_id: AuthorityIdentifier,
    worker_cache: WorkerCache,
    committee: Committee,
    client: NetworkClient,
    metrics: Arc<ExecutorMetrics>,
}

#[allow(clippy::too_many_arguments)]
pub fn spawn_subscriber(
    authority_id: AuthorityIdentifier,
    worker_cache: WorkerCache,
    committee: Committee,
    client: NetworkClient,
    rx_shutdown: Noticer,
    consensus_bus: ConsensusBus,
    restored_consensus_output: Vec<CommittedSubDag>,
    consensus_output_notification_sender: broadcast::Sender<ConsensusOutput>,
) -> JoinHandle<()> {
    let metrics = Arc::new(ExecutorMetrics::default());

    spawn_logged_monitored_task!(
        async move {
            info!(target: "telcoin::subscriber", "Starting subscriber");
            let subscriber = Subscriber {
                rx_shutdown,
                consensus_bus,
                inner: Arc::new(Inner { authority_id, committee, worker_cache, client, metrics }),
            };
            subscriber
                .run(restored_consensus_output, consensus_output_notification_sender)
                .await
                .expect("Failed to run subscriber")
        },
        "SubscriberTask"
    )
}

impl Subscriber {
    /// Returns the max number of sub-dag to fetch payloads concurrently.
    const MAX_PENDING_PAYLOADS: usize = 1000;

    /// Main loop connecting to the consensus to listen to sequence messages.
    async fn run(
        self,
        restored_consensus_output: Vec<CommittedSubDag>,
        consensus_output_notification_sender: broadcast::Sender<ConsensusOutput>,
    ) -> SubscriberResult<()> {
        // It's important to have the futures in ordered fashion as we want
        // to guarantee that will deliver to the executor the certificates
        // in the same order we received from rx_sequence. So it doesn't
        // matter if we somehow managed to fetch the blocks from a later
        // certificate. Unless the earlier certificate's payload has been
        // fetched, no later certificate will be delivered.
        let mut waiting = FuturesOrdered::new();

        // First handle any consensus output messages that were restored due to a restart.
        // This needs to happen before we start listening on rx_sequence and receive messages
        // sequenced after these.
        for message in restored_consensus_output {
            let future = Self::fetch_blocks(self.inner.clone(), message);
            waiting.push_back(future);

            self.inner.metrics.subscriber_recovered_certificates_count.inc();
        }

        let mut rx_sequence = self.consensus_bus.sequence().subscribe();
        // Listen to sequenced consensus message and process them.
        loop {
            tokio::select! {
                // Receive the ordered sequence of consensus messages from a consensus node.
                Some(sub_dag) = rx_sequence.recv(), if waiting.len() < Self::MAX_PENDING_PAYLOADS => {
                    // We can schedule more then MAX_PENDING_PAYLOADS payloads but
                    // don't process more consensus messages when more
                    // then MAX_PENDING_PAYLOADS is pending
                    waiting.push_back(Self::fetch_blocks(self.inner.clone(), sub_dag));
                },

                // Receive consensus messages after all transaction data is downloaded
                // then send to the execution layer for final block production.
                //
                // NOTE: this broadcasts to all subscribers, but lagging receivers will lose messages
                Some(message) = waiting.next() => {
                    if let Err(e) = consensus_output_notification_sender.send(message) {
                        error!(target: "telcoin::subscriber", "error broadcasting consensus output for authority {}: {}", self.inner.authority_id, e);
                        return Ok(());
                    }
                },

                _ = &self.rx_shutdown => {
                    return Ok(())
                }

            }

            self.inner.metrics.waiting_elements_subscriber.set(waiting.len() as i64);
        }
    }

    /// Returns ordered vector of futures for downloading blocks for certificates
    /// Order of futures returned follows order of blocks in the certificates.
    /// See BlockFetcher for more details.
    async fn fetch_blocks(inner: Arc<Inner>, deliver: CommittedSubDag) -> ConsensusOutput {
        let num_blocks = deliver.num_blocks();
        let num_certs = deliver.len();

        // get the execution address of the authority or use zero address
        let leader = inner.committee.authority(&deliver.leader.origin());
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
                blocks: vec![],
                beneficiary: address,
                block_digests: VecDeque::new(),
            };
        }

        let sub_dag = Arc::new(deliver);
        let mut subscriber_output = ConsensusOutput {
            sub_dag: sub_dag.clone(),
            blocks: Vec::with_capacity(num_certs),
            beneficiary: address,
            block_digests: VecDeque::new(),
        };

        let mut batch_digests_and_workers: HashMap<
            NetworkPublicKey,
            (HashSet<BlockHash>, HashSet<NetworkPublicKey>),
        > = HashMap::new();

        for cert in &sub_dag.certificates {
            for (digest, (worker_id, _)) in cert.header().payload().iter() {
                let own_worker_name = inner
                    .worker_cache
                    .worker(
                        inner.committee.authority(&inner.authority_id).unwrap().protocol_key(),
                        worker_id,
                    )
                    .unwrap_or_else(|_| panic!("worker_id {worker_id} is not in the worker cache"))
                    .name;
                let workers = Self::workers_for_certificate(&inner, cert, worker_id);
                let (batch_set, worker_set) =
                    batch_digests_and_workers.entry(own_worker_name).or_default();
                batch_set.insert(*digest);
                subscriber_output.block_digests.push_back(*digest);
                worker_set.extend(workers);
            }
        }

        let fetched_batches_timer =
            inner.metrics.block_fetch_for_committed_subdag_total_latency.start_timer();
        inner.metrics.committed_subdag_block_count.observe(num_blocks as f64);
        let fetched_batches =
            Self::fetch_blocks_from_workers(&inner, batch_digests_and_workers).await;
        drop(fetched_batches_timer);

        // Map all fetched batches to their respective certificates and submit as
        // consensus output
        for cert in &sub_dag.certificates {
            let mut output_batches = Vec::with_capacity(cert.header().payload().len());

            inner.metrics.subscriber_current_round.set(cert.round() as i64);

            inner
                .metrics
                .subscriber_certificate_latency
                .observe(cert.created_at().elapsed().as_secs_f64());

            for (digest, (_, _)) in cert.header().payload().iter() {
                inner.metrics.subscriber_processed_blocks.inc();
                let batch = fetched_batches
                    .get(digest)
                    .expect("[Protocol violation] Batch not found in fetched batches from workers of certificate signers");

                debug!(
                    "Adding fetched batch {digest} from certificate {} to consensus output",
                    cert.digest()
                );
                output_batches.push(batch.clone());
            }
            subscriber_output.blocks.push(output_batches);
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

    async fn fetch_blocks_from_workers(
        inner: &Inner,
        block_digests_and_workers: HashMap<
            NetworkPublicKey,
            (HashSet<BlockHash>, HashSet<NetworkPublicKey>),
        >,
    ) -> HashMap<BlockHash, WorkerBlock> {
        let mut fetched_blocks = HashMap::new();

        for (worker_name, (digests, known_workers)) in block_digests_and_workers {
            debug!(
                "Attempting to fetch {} digests from {} known workers, {worker_name}'s",
                digests.len(),
                known_workers.len()
            );
            // TODO: Can further parallelize this by worker if necessary. Maybe move the logic
            // to NetworkClient.
            // Only have one worker for now so will leave this for a future
            // optimization.
            let request = FetchBlocksRequest { digests, known_workers };
            let blocks = loop {
                match inner.client.fetch_blocks(worker_name.clone(), request.clone()).await {
                    Ok(resp) => break resp.blocks,
                    Err(e) => {
                        error!("Failed to fetch blocks from worker {worker_name}: {e:?}");
                        // Loop forever on failure. During shutdown, this should get cancelled.
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            };
            for (digest, block) in blocks {
                Self::record_fetched_block_metrics(inner, &block, &digest);
                fetched_blocks.insert(digest, block);
            }
        }

        fetched_blocks
    }

    fn record_fetched_block_metrics(inner: &Inner, block: &WorkerBlock, digest: &BlockHash) {
        if let Some(received_at) = block.received_at() {
            let remote_duration = received_at.elapsed().as_secs_f64();
            debug!(
                "Batch was fetched for execution after being received from another worker {}s ago.",
                remote_duration
            );
            inner
                .metrics
                .block_execution_local_latency
                .with_label_values(&["other"])
                .observe(remote_duration);
        } else {
            let local_duration = block.created_at().elapsed().as_secs_f64();
            debug!(
                "Batch was fetched for execution after being created locally {}s ago.",
                local_duration
            );
            inner
                .metrics
                .block_execution_local_latency
                .with_label_values(&["own"])
                .observe(local_duration);
        };

        let block_fetch_duration = block.created_at().elapsed().as_secs_f64();
        inner.metrics.block_execution_latency.observe(block_fetch_duration);
        debug!(
            "Block {:?} took {} seconds since it has been created to when it has been fetched for execution",
            digest,
            block_fetch_duration,
        );
    }
}

// TODO: add a unit test
