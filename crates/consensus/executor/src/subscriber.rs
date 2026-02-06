//! Subscriber handles consensus output.

use crate::{errors::SubscriberResult, SubscriberError};
use futures::{stream::FuturesOrdered, StreamExt};
use state_sync::{last_executed_consensus_block, save_consensus, spawn_state_sync};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tn_config::ConsensusConfig;
use tn_network_types::{local::LocalNetwork, PrimaryToWorkerClient};
use tn_primary::{
    network::{ConsensusResult, PrimaryNetworkHandle},
    ConsensusBus, NodeMode,
};
use tn_storage::CertificateStore;
use tn_types::{
    encode, to_intent_message, Address, AuthorityIdentifier, Batch, BlockHash, BlsSigner as _,
    CertifiedBatch, CommittedSubDag, Committee, ConsensusHeader, ConsensusOutput, Database,
    Hash as _, Noticer, TaskManager, TaskSpawner, Timestamp, TnReceiver, TnSender, B256,
};
use tracing::{debug, error, info, instrument};

/// The `Subscriber` receives certificates sequenced by the consensus and waits until the
/// downloaded all the transactions references by the certificates; it then
/// forward the certificates to the Executor.
#[derive(Clone, Debug)]
pub struct Subscriber<DB> {
    /// Used to get the sequence receiver
    consensus_bus: ConsensusBus,
    /// Consensus configuration (contains the consensus DB)
    config: ConsensusConfig<DB>,
    /// The handle to the network.
    network_handle: PrimaryNetworkHandle,
    /// Inner state.
    inner: Arc<Inner>,
}

/// Inner subscriber type.
#[derive(Debug)]
struct Inner {
    /// The identifier for the authority.
    ///
    /// Used for logging, None if we are not a validator.
    authority_id: Option<AuthorityIdentifier>,
    /// The committee for the epoch.
    committee: Committee,
    /// The client to request worker batches and build consensus output.
    client: LocalNetwork,
}

/// Spawn the subscriber in the correct mode based on the validator status for the current epoch.
pub fn spawn_subscriber<DB: Database>(
    config: ConsensusConfig<DB>,
    rx_shutdown: Noticer,
    consensus_bus: ConsensusBus,
    task_manager: &TaskManager,
    network_handle: PrimaryNetworkHandle,
) {
    let authority_id = config.authority_id();
    let committee = config.committee().clone();
    let client = config.local_network().clone();
    let mode = *consensus_bus.node_mode().borrow();
    let subscriber = Subscriber {
        consensus_bus,
        config,
        network_handle,
        inner: Arc::new(Inner { authority_id, committee, client }),
    };
    match mode {
        // If we are active then partcipate in consensus.
        NodeMode::CvvActive => {
            task_manager.spawn_critical_task("subscriber consensus", async move {
                info!(target: "subscriber", "Starting subscriber: CVV");
                if let Err(e) = subscriber.run(rx_shutdown).await {
                    error!(target: "subscriber", "Error subscriber consensus: {e}");
                }
            });
        }
        NodeMode::CvvInactive => {
            let clone = task_manager.get_spawner();
            // If we are not active but are a CVV then catch up and rejoin.
            task_manager.spawn_critical_task(
                "subscriber catch up and rejoin consensus",
                async move {
                    info!(target: "subscriber", "Starting subscriber: Catch up and rejoin");
                    if let Err(e) = subscriber.catch_up_rejoin_consensus(clone).await {
                        error!(target: "subscriber", "Error catching up consensus: {e}");
                    }
                },
            );
        }
        NodeMode::Observer => {
            let clone = task_manager.get_spawner();
            // If we are not active then just follow consensus.
            task_manager.spawn_critical_task("subscriber follow consensus", async move {
                info!(target: "subscriber", "Starting subscriber: Follower");
                if let Err(e) = subscriber.follow_consensus(clone).await {
                    error!(target: "subscriber", "Error following consensus: {e}");
                }
            });
        }
    }
}

impl<DB: Database> Subscriber<DB> {
    /// Returns the max number of sub-dag to fetch payloads concurrently.
    const MAX_PENDING_PAYLOADS: usize = 1000;

    /// Turns a ConsensusHeader into a ConsensusOutput and sends it down the consensus_output
    /// channel for execution.
    #[instrument(level = "debug", skip_all, fields(number = consensus_header.number))]
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
            .await?;

        // If we want to rejoin consensus eventually then save certs.
        let _ = self.config.node_storage().write(consensus_output.sub_dag.leader.clone());
        let _ = self.config.node_storage().write_all(consensus_output.sub_dag.certificates.clone());

        // This save will essentially mark this consensus output as written in stone (added to the
        // consensus chain). This does NOT imply execution although it will be sent off for
        // execution.
        save_consensus(
            self.config.node_storage(),
            consensus_output.clone(),
            &self.inner.authority_id,
        )
        .await?;

        let last_round = consensus_output.leader_round();

        // We aren't doing consensus now but still need to update these watches before
        // we send the consensus output.
        self.consensus_bus.committed_round_updates().send_replace(last_round);
        let _ = self.consensus_bus.primary_round_updates().send(last_round);

        if let Err(e) = self.consensus_bus.consensus_output().send(consensus_output).await {
            error!(target: "subscriber", "error broadcasting consensus output for authority {:?}: {}", self.inner.authority_id, e);
            return Err(SubscriberError::ClosedChannel("consensus_output".to_string()));
        }
        Ok(())
    }

    /// Catch up to current consensus and then try to rejoin as an active CVV.
    async fn catch_up_rejoin_consensus(&self, tasks: TaskSpawner) -> SubscriberResult<()> {
        // Get a receiver and then stream any missing headers so we don't miss them.
        let mut rx_consensus_headers = self.consensus_bus.consensus_header().subscribe();
        spawn_state_sync(
            self.config.clone(),
            self.consensus_bus.clone(),
            self.network_handle.clone(),
            tasks,
        );
        while let Some(consensus_header) = rx_consensus_headers.recv().await {
            let consensus_header_number = consensus_header.number;
            self.handle_consensus_header(consensus_header).await?;
            if let Some(last_consensus_header) =
                self.consensus_bus.last_consensus_header().borrow().as_ref()
            {
                if consensus_header_number == last_consensus_header.number {
                    // We are caught up enough so try to jump back into consensus
                    info!(target: "subscriber", "attempting to rejoin consensus, consensus block height {consensus_header_number}");
                    let _ = self.consensus_bus.node_mode().send(NodeMode::CvvActive);
                    self.config.shutdown().notify();
                    return Ok(());
                }
            }
        }
        Ok(())
    }

    /// Follow along with consensus output but do not try to join consensus.
    async fn follow_consensus(&self, tasks: TaskSpawner) -> SubscriberResult<()> {
        // Get a receiver then stream any missing headers so we don't miss them.
        let mut rx_consensus_headers = self.consensus_bus.consensus_header().subscribe();
        spawn_state_sync(
            self.config.clone(),
            self.consensus_bus.clone(),
            self.network_handle.clone(),
            tasks,
        );
        while let Some(consensus_header) = rx_consensus_headers.recv().await {
            self.handle_consensus_header(consensus_header).await?;
        }
        Ok(())
    }

    /// Return the block hash and number of the last executed consensus output.
    ///
    /// This method is called on startup to retrieve the needed information to build the next
    /// `ConsensusHeader` off of this parent.
    async fn get_last_executed_consensus(&self) -> SubscriberResult<(BlockHash, u64)> {
        // Get the DB and load our last executed consensus block (note there may be unexecuted
        // blocks, catch up will execute them).
        let last_executed_block =
            last_executed_consensus_block(&self.consensus_bus, self.config.node_storage())
                .unwrap_or_default();

        info!(target: "subscriber", ?last_executed_block, "restoring last executed consensus for constucting the next ConsensusHeader:");

        Ok((last_executed_block.digest(), last_executed_block.number))
    }

    /// Main loop connecting to the consensus to listen to sequence messages.
    #[instrument(level = "info", skip_all, fields(authority = ?self.inner.authority_id))]
    async fn run(self, rx_shutdown: Noticer) -> SubscriberResult<()> {
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
                    if let Err(e) = self.consensus_bus.last_consensus_header().send(Some(ConsensusHeader { parent_hash, sub_dag: sub_dag.clone(), number, extra: B256::default() })) {
                        error!(target: "subscriber", "error sending latest consensus header for authority {:?}: {}", self.inner.authority_id, e);
                        return Err(SubscriberError::ClosedChannel("failed to send last consensus header on bus".to_string()));
                    }
                    let epoch = sub_dag.leader_epoch();
                    let round = sub_dag.leader_round();
                    let consensus_result_hash = ConsensusResult::digest_data(epoch, round, number, last_parent);
                    let sig =
                        self.config.key_config().request_signature_direct(&encode(&to_intent_message(consensus_result_hash)));
                    if let Err(e) = self.network_handle.publish_consensus(epoch, round, number, last_parent, self.config.key_config().public_key(), sig).await {
                        error!(target: "subscriber", "error publishing latest consensus to network {:?}: {}", self.inner.authority_id, e);
                    }
                    last_number += 1;
                    waiting.push_back(self.fetch_batches(sub_dag, parent_hash, number));
                },

                // Receive consensus messages after all transaction data is downloaded
                // then send to the execution layer for final block production.
                //
                // NOTE: this broadcasts to all subscribers, but lagging receivers will lose messages
                Some(output) = waiting.next() => {
                    match output {
                        Ok(output) => {
                            debug!(target: "subscriber", output=?output.digest(), "saving next output");
                            save_consensus(self.config.node_storage(), output.clone(), &self.inner.authority_id).await?;
                            debug!(target: "subscriber", "broadcasting output...");
                            if let Err(e) = self.consensus_bus.consensus_output().send(output).await {
                                error!(target: "subscriber", "error broadcasting consensus output for authority {:?}: {}", self.inner.authority_id, e);
                                return Err(SubscriberError::ClosedChannel("failed to broadcast consensus output".to_string()));
                            }
                            debug!(target: "subscriber", "output broadcast successfully");
                        }
                        Err(e) => {
                            error!(target: "subscriber", "error fetching batches: {e}");
                            // Failure to fetch batches is a fatal condition, return an error which will trigger node shutdown.
                            return Err(e);
                        }
                    }
                },

                _ = &rx_shutdown => {
                    return Ok(())
                }

            }
        }
    }

    /// Helper function to obtain authority's execution address based on their
    /// `AuthorityIdentifier`. This address is used as the beneficiary during batch execution.
    /// A fatal error is returned if the authority is missing from the committee.
    fn authority_execution_address(
        &self,
        authority_id: &AuthorityIdentifier,
    ) -> SubscriberResult<Address> {
        self.inner
            .committee
            .authority(authority_id)
            .map(|a| a.execution_address())
            .ok_or(SubscriberError::UnexpectedAuthority(authority_id.clone()))
            .inspect_err(|_| {
                error!(target: "subscriber", ?authority_id, "Authority missing from committee");
            })
    }

    /// Turn a CommittedSubDag with consensus header info into ConsensusOutput.
    /// It will retrieve any missing Batches so the ConsensusOutput will be ready
    /// to execute.
    /// Note, an error here is BAD and will most likely cause node shutdown (clean).  Do
    /// not provide a bogus sub dag...
    #[instrument(level = "debug", skip_all, fields(number, num_certs = deliver.len()))]
    async fn fetch_batches(
        &self,
        deliver: CommittedSubDag,
        parent_hash: B256,
        number: u64,
    ) -> SubscriberResult<ConsensusOutput> {
        let num_blocks = deliver.num_primary_blocks();
        let num_certs = deliver.len();

        if num_blocks == 0 {
            debug!(target: "subscriber", "No blocks to fetch, payload is empty");
            return Ok(ConsensusOutput {
                sub_dag: Arc::new(deliver),
                parent_hash,
                number,
                ..Default::default()
            });
        }

        let sub_dag = Arc::new(deliver);
        let mut consensus_output = ConsensusOutput {
            sub_dag: sub_dag.clone(),
            batches: Vec::with_capacity(num_certs),
            parent_hash,
            number,
            ..Default::default()
        };

        let mut batch_set: HashSet<BlockHash> = HashSet::new();

        for cert in &sub_dag.certificates {
            for (digest, _) in cert.header().payload().iter() {
                batch_set.insert(*digest);
                consensus_output.batch_digests.push_back(*digest);
            }
        }

        let mut fetched_batches = self.fetch_batches_from_peers(batch_set).await?;

        // map all fetched batches to their respective certificates for applying block rewards
        for cert in &sub_dag.certificates {
            // create collection of batches to execute for this certificate
            let mut cert_batches = Vec::with_capacity(cert.header().payload().len());

            // retrieve fetched batch by digest
            for digest in cert.header().payload().keys() {
                let batch = fetched_batches.remove(digest).ok_or(SubscriberError::MissingFetchedBatch(*digest)).inspect_err(|_| {
                    error!(target: "subscriber", "[Protocol violation] Batch not found in fetched batches from workers of certificate signers");
                })?;

                debug!(
                    target: "subscriber",
                    "Adding fetched batch {digest} from certificate {} to consensus output",
                    cert.digest()
                );
                cert_batches.push(batch);
            }

            // main collection for execution
            consensus_output.batches.push(CertifiedBatch {
                address: self.authority_execution_address(cert.origin())?,
                batches: cert_batches,
            });
        }
        // Count total transactions across all batches
        let total_txs: usize = consensus_output
            .batches
            .iter()
            .flat_map(|cb| cb.batches.iter())
            .map(|b| b.transactions.len())
            .sum();

        // Metric: consensus_output_ready - tracks consensus output ready for execution
        info!(
            target: "consensus::metrics",
            number = number,
            leader_round = sub_dag.leader_round(),
            num_certs = num_certs,
            num_batches = consensus_output.batch_digests.len(),
            total_txs = total_txs,
            "consensus output ready"
        );

        debug!(target: "subscriber", "returning output to subscriber");
        Ok(consensus_output)
    }

    async fn fetch_batches_from_peers(
        &self,
        batch_digests: HashSet<BlockHash>,
    ) -> SubscriberResult<HashMap<BlockHash, Batch>> {
        let mut fetched_blocks = HashMap::new();

        debug!(target: "subscriber", "Attempting to fetch {} digests peers", batch_digests.len(),);
        let blocks = match self.inner.client.fetch_batches(batch_digests.clone()).await {
            Ok(resp) => resp,
            Err(e) => {
                error!(target: "subscriber", "Failed to fetch batches from peers: {e:?}");
                return Err(SubscriberError::ClientRequestsFailed);
            }
        };
        for (digest, block) in blocks.batches.into_iter() {
            if let Some(received_at) = block.received_at() {
                let remote_duration = received_at.elapsed().as_secs_f64();
                debug!(
                    target: "subscriber",
                    "Block {:?} took {} seconds since it was received to when it was fetched for execution",
                    digest,
                    remote_duration,
                );
            }
            fetched_blocks.insert(digest, block);
        }

        Ok(fetched_blocks)
    }
}
